#!/usr/bin/env python3
"""
Table Builder for Iceberg tables from exhaustive shape configs.

Builds Iceberg tables with exact control over file layout using
PyArrow for Parquet writing and Iceberg JVM RowDelta API for commits.
Takes raw config dicts directly from the TableConfigGenerator.
"""

import json
import logging
import os
import tempfile
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field

import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import SparkSession

from .dimensions.interfaces import SchemaProvider, WriteOptionsProvider
from .dimensions.compression import CompressionChoice

logger = logging.getLogger(__name__)

ICEBERG_SPARK_RUNTIME = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0"

# Bootstrap codec for the bytes-per-row measurement probe, used only before
# build_table sets the actual data codec from write_options (e.g. when
# bytes_per_row is read standalone). It is NOT a data-file write fallback: data
# files take their codec exclusively from write_options, which is required.
# Derived from CompressionChoice's own default rather than a scattered literal.
DEFAULT_COMPRESSION = CompressionChoice.default_codec.value.lower()

# Delete files always use this codec — their schema (file_path, pos) is fixed,
# so per-column write_options do not apply to them.
DELETE_COMPRESSION = "zstd"

# Sentinel distinguishing "no typed_value key" from "typed_value is None" (a
# legitimately null row under the nulls value_strategy).
_NO_TYPED_VALUE = object()


@dataclass
class BuildStats:
    """Statistics from table build."""
    snapshot_count: int
    data_file_count: int
    delete_file_count: int
    record_count: int
    total_size_bytes: int
    validations: Dict[str, bool] = field(default_factory=dict)
    validation_messages: List[str] = field(default_factory=list)


class UnifiedTableBuilder:
    """
    Builds Iceberg tables from config dicts with exact control over file layout.

    Uses PyArrow for writing Parquet + Iceberg JVM RowDelta API for commits.
    The config dict is used DIRECTLY — all ~15 fields read as needed.
    """

    def __init__(self, warehouse_path: str, target_size_mb: float = 0.1, seed: int = 42):
        self.warehouse_path = Path(warehouse_path).absolute()
        self.warehouse_path.mkdir(parents=True, exist_ok=True)
        self.target_size_mb = target_size_mb
        self.seed = seed
        self._spark: Optional[SparkSession] = None
        self._file_counter = 0

        # Sentinel sizing is derived from a measured bytes-per-row. The values
        # are computed lazily on first access — the measurement writes a sample
        # Parquet file, so keeping it out of __init__ lets the many pure unit
        # tests (config validation, file naming, context manager) construct a
        # builder with no disk I/O and no mocking.
        #
        # bytes-per-row depends on the data codec, so it is cached per codec:
        # reusing a builder across tables with different codecs re-measures and
        # stores a new entry rather than reusing a stale figure. _data_codec is
        # set from write_options at the top of build_table.
        self._bytes_per_row_by_codec: Dict[str, int] = {}
        self._target_row_count: Optional[int] = None
        self._data_codec: str = DEFAULT_COMPRESSION

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    @property
    def bytes_per_row(self) -> int:
        """Measured bytes-per-row for the current data codec (cached per codec)."""
        codec = self._data_codec
        if codec not in self._bytes_per_row_by_codec:
            self._bytes_per_row_by_codec[codec] = self._measure_bytes_per_row(codec)
        return self._bytes_per_row_by_codec[codec]

    @property
    def _base_target_row_count(self) -> int:
        """Base row count for sentinel files, derived from bytes_per_row.

        Not cached — bytes_per_row (the only disk I/O) is already cached per
        codec, and the division is trivial to redo.
        """
        target_bytes = self.target_size_mb * 1024 * 1024
        return max(1, int(target_bytes / self.bytes_per_row))

    @property
    def target_row_count(self) -> int:
        """Current sentinel target row count (defaults to the base, mutable
        across sentinel retries)."""
        if self._target_row_count is None:
            self._target_row_count = self._base_target_row_count
        return self._target_row_count

    @target_row_count.setter
    def target_row_count(self, value: int) -> None:
        self._target_row_count = value

    def _measure_bytes_per_row(self, codec: str = DEFAULT_COMPRESSION) -> int:
        """Measure bytes per row by writing a sample parquet file.

        Uses the given codec so sentinel sizing reflects the codec the real
        data files are written with (defaults to DEFAULT_COMPRESSION).
        """
        sample_size = 1000
        schema = pa.schema([pa.field("value", pa.int32(), nullable=False)
                           .with_metadata({"PARQUET:field_id": "1"})])
        table = pa.Table.from_arrays(
            [pa.array(list(range(sample_size)), type=pa.int32())], schema=schema)
        with tempfile.NamedTemporaryFile(suffix='.parquet') as tmp:
            pq.write_table(table, tmp.name, compression=codec)
            return os.path.getsize(tmp.name) // sample_size

    def _next_file_name(self, prefix: str) -> str:
        """Generate a deterministic file name from seed + counter."""
        self._file_counter += 1
        tag = f"{self.seed:08x}{self._file_counter:08x}"
        return f"{prefix}-{tag}.parquet"

    def _get_spark(self) -> SparkSession:
        """Lazy-init Spark session with Iceberg support."""
        if self._spark is None:
            self._spark = (
                SparkSession.builder
                .appName("UnifiedTableBuilder")
                .master("local[1]")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.extensions",
                        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.local.type", "hadoop")
                .config("spark.sql.catalog.local.warehouse", str(self.warehouse_path))
                .config("spark.jars.packages", ICEBERG_SPARK_RUNTIME)
                .config("spark.jars.ivy", "/tmp/.ivy2")
                .config("spark.driver.host", "localhost")
                .config("spark.sql.parquet.compression.codec", "snappy")
                .config("spark.driver.memory", "4g")
                .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
                .config("spark.sql.iceberg.delete.mode", "merge-on-read")
                .config("spark.sql.adaptive.enabled", "false")
                .getOrCreate()
            )
            self._spark.sparkContext.setLogLevel("ERROR")
        return self._spark

    # =========================================================================
    # PUBLIC API
    # =========================================================================

    def build_table(self, config: Dict[str, Any], namespace: str, table_name: str,
                    schema_provider: SchemaProvider,
                    write_options: WriteOptionsProvider,
                    validate: bool = True) -> BuildStats:
        """
        Build an Iceberg table from a raw config dict.

        The config dict comes directly from the exhaustive generator — all fields
        (data_files, delete_files, snapshots, sentinel_in_file, value_strategy, etc.)
        are read directly. No wrappers.

        Args:
            config: Raw config dict from the exhaustive generator, with typed
                values already filled per its value_strategy (see TypedValueFiller).
            namespace: Iceberg namespace.
            table_name: Table name.
            schema_provider: SchemaProvider (a DataTypeChoice) supplying the column
                DDL, Arrow schema, and per-row value generation.
            write_options: WriteOptionsProvider supplying per-column compression
                and encoding.
            validate: When True (default), read the built table back and compare
                it against the config (row/file/delete/snapshot counts, sentinel
                size, field ids). When False, skip all verification — no Spark
                metadata queries, no delete-parquet reads, no field-id harvest —
                so bulk generation of tables you already trust pays no cost.
                stats.validations/validation_messages are left empty when False.
        """
        required_keys = ('data_files', 'delete_files', 'snapshots', 'row_count')
        missing = [k for k in required_keys if k not in config]
        if missing:
            raise ValueError(
                f"Config missing required keys: {missing}. Got keys: {list(config.keys())}")

        # Sentinel sizing measures bytes-per-row with the codec the data files
        # actually use. The probe writes a single 'value' int column, so its
        # codec is that column's — required, no default (same rule as the write
        # path). bytes-per-row is cached per codec, so calling this builder for
        # different config_ids with different codecs re-measures per codec; a
        # codec change here invalidates the derived base-row-count cache.
        data_codec = write_options.get_compression("value")
        if not data_codec:
            raise ValueError(
                "write_options supplies no compression codec for column 'value'; "
                "data-file compression must be explicit.")
        data_codec = data_codec.lower()
        self._data_codec = data_codec

        spark = self._get_spark()

        full_namespace = f"local.{namespace}"
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {full_namespace}")

        full_table_name = f"{full_namespace}.{table_name}"
        min_sentinel_bytes = int(self.target_size_mb * 1024 * 1024)
        max_retries = 10

        # Reset target_row_count per table so sentinel retries don't leak
        self.target_row_count = self._base_target_row_count

        for attempt in range(max_retries + 1):
            spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")

            # Create table DDL from the schema provider.
            col_def = schema_provider.get_spark_ddl()
            spark.sql(f"""
                CREATE TABLE {full_table_name} ({col_def})
                USING iceberg
                TBLPROPERTIES (
                    'write.delete.mode' = 'merge-on-read',
                    'format-version' = '2'
                )
            """)

            # Build all snapshots
            self._build_snapshots(spark, full_table_name, config,
                                  schema_provider=schema_provider,
                                  write_options=write_options)

            # Check sentinel file size
            sentinel_file_id = config.get('sentinel_in_file')
            if not sentinel_file_id:
                break

            sentinel_size = self._get_sentinel_file_size(spark, full_table_name)
            if sentinel_size is None or sentinel_size >= min_sentinel_bytes:
                break

            if attempt == max_retries:
                break

            # Adjust target_row_count and retry. The estimate assumes size grows
            # linearly with rows, but for compressible types (e.g. string) the
            # marginal bytes-per-row shrinks as similar rows are added, so a
            # linear step lands short and the loop only creeps toward the target.
            # Overshoot by OVERSHOOT_FACTOR so a sub-linear step still clears the
            # minimum; exceeding the *minimum* sentinel size is harmless.
            OVERSHOOT_FACTOR = 1.5
            compressed_bpr = max(1, sentinel_size // self.target_row_count)
            deficit = min_sentinel_bytes - sentinel_size
            extra_rows = max(1, int(deficit / compressed_bpr * OVERSHOOT_FACTOR) + 1)
            self.target_row_count += extra_rows

        stats = self._query_table_metadata(spark, full_table_name)
        if validate:
            validations, messages = self._validate_against_config(
                spark, full_table_name, config, stats,
                schema_provider=schema_provider)
            stats.validations = validations
            stats.validation_messages = messages
        return stats

    # =========================================================================
    # SNAPSHOT BUILDING
    # =========================================================================

    def _build_snapshots(self, spark: SparkSession, full_table_name: str, config: Dict[str, Any],
                         schema_provider: SchemaProvider,
                         write_options: WriteOptionsProvider):
        """Iterate config snapshots, commit each via RowDelta for exact file layout control."""
        sentinel_file_id = config.get('sentinel_in_file')

        # Tracks file_index → physical path across all snapshots,
        # so position-delete files can reference data from earlier snapshots.
        file_index_to_path: Dict[int, str] = {}

        for snapshot_idx, snapshot in enumerate(config['snapshots']):
            data_file_ids = snapshot['data_files']
            delete_file_ids = snapshot['delete_files']

            if not data_file_ids and not delete_file_ids:
                continue

            self._build_snapshot_row_delta(
                spark, full_table_name, config, snapshot_idx,
                data_file_ids, delete_file_ids, sentinel_file_id,
                file_index_to_path,
                schema_provider=schema_provider,
                write_options=write_options)

    # =========================================================================
    # ROW DELTA PATH: PyArrow + JVM API
    # =========================================================================

    def _build_snapshot_row_delta(self, spark, full_table_name, config, snapshot_idx,
                                  data_file_ids, delete_file_ids, sentinel_file_id,
                                  file_index_to_path,
                                  schema_provider: SchemaProvider,
                                  write_options: WriteOptionsProvider):
        """Build a snapshot with multiple files using RowDelta."""
        table_path = self._get_table_path(full_table_name)
        data_dir = table_path / "data"
        data_dir.mkdir(parents=True, exist_ok=True)

        # Step 1: Write data parquet files
        new_data_file_infos = []

        if data_file_ids:
            new_data_file_infos = self._write_data_files(
                config, data_file_ids, sentinel_file_id, data_dir, table_path,
                file_index_to_path,
                schema_provider=schema_provider,
                write_options=write_options)

        # Step 2: Write position delete parquet files
        delete_file_infos = []
        if delete_file_ids:
            delete_file_infos = self._write_delete_files(
                config, delete_file_ids, file_index_to_path, data_dir)

        # Step 3: Commit via RowDelta
        self._commit_row_delta(spark, full_table_name, new_data_file_infos, delete_file_infos)

    # =========================================================================
    # FILE WRITING
    # =========================================================================

    def _write_data_files(self, config, data_file_ids, sentinel_file_id, data_dir,
                          table_path, file_index_to_path,
                          schema_provider: SchemaProvider,
                          write_options: WriteOptionsProvider):
        """Write data parquet files with Iceberg field IDs via PyArrow.

        Uses the schema provider's Arrow schema and per-row value generation
        (honoring the config's value_strategy via typed_value), and applies the
        per-column compression/encoding from write_options.
        """
        # Field-id seam: choose where the field ids come from, then hand the map
        # to the provider (which only stamps it). Today we use the computed ids;
        # to instead harvest Iceberg's own assignment, swap this one line for a
        # read-back of the created table's metadata.
        field_ids = schema_provider.get_field_ids()
        data_schema = schema_provider.get_arrow_schema(field_ids)

        write_kwargs = self._build_write_kwargs(data_schema, write_options)

        file_infos = []

        for file_id in data_file_ids:
            data_file = next(
                (f for f in config['data_files'] if f['file_id'] == file_id), None)
            if data_file is None:
                raise ValueError(
                    f"Config missing data_file with file_id='{file_id}'. "
                    f"Available: {[f['file_id'] for f in config['data_files']]}")

            arrays = self._generate_arrays_from_provider(
                data_file, data_schema, schema_provider, sentinel_file_id, file_id)

            table = pa.Table.from_arrays(arrays, schema=data_schema)

            file_name = self._next_file_name("data")
            file_path = data_dir / file_name
            pq.write_table(table, str(file_path), **write_kwargs)

            file_size = file_path.stat().st_size
            file_infos.append((str(file_path), file_size, table.num_rows))

            # Register in the cross-snapshot map for position-delete resolution
            file_index_to_path[data_file['file_index']] = str(file_path)

        return file_infos

    def _generate_arrays_from_provider(self, data_file, data_schema, schema_provider,
                                        sentinel_file_id, file_id):
        """Generate PyArrow arrays for each column, honoring the config's values.

        Real rows take their value from the config (typed_value if the filler
        ran, else generate_value(row_id)). Sentinel padding rows have no config
        entry, so they are generated as typed filler via generate_value.
        """
        rows = data_file['rows']
        is_sentinel = sentinel_file_id and file_id == sentinel_file_id

        padding_ids = []
        if is_sentinel:
            padding_needed = self.target_row_count - len(rows)
            if padding_needed > 0:
                padding_ids = [-(i + 1000000) for i in range(padding_needed)]

        num_columns = len(data_schema)
        arrays = []
        for field_idx in range(num_columns):
            col_name = data_schema.field(field_idx).name
            col_type = data_schema.field(field_idx).type
            values = [self._resolve_row_value(row, col_name, num_columns, schema_provider)
                      for row in rows]
            values += [schema_provider.generate_value(col_name, pid) for pid in padding_ids]
            arrays.append(pa.array(values, type=col_type))
        return arrays

    @staticmethod
    def _resolve_row_value(row, col_name, num_columns, schema_provider):
        """Resolve a single row's value for a column on the schema_provider path.

        Order: row['typed_value'] (set by TypedValueFiller — already applies the
        value_strategy) if present, else generate_value(col, row_id). A
        multi-column typed_value is a {col: value} dict; single-column is the
        value itself. row['value'] (the abstract shape int) is not used here.
        """
        tv = row.get('typed_value', _NO_TYPED_VALUE)
        if tv is not _NO_TYPED_VALUE:
            if num_columns > 1:
                return tv.get(col_name) if isinstance(tv, dict) else tv
            return tv
        return schema_provider.generate_value(col_name, row['row_id'])

    @staticmethod
    def _leaf_column_paths(field: pa.Field, prefix: str = "") -> List[str]:
        """Return the physical Parquet leaf-column path(s) for an Arrow field.

        pq.write_table keys per-column compression/encoding by physical leaf
        path, which for nested types differs from the top-level name. This walks
        the Arrow type to PyArrow's leaf paths:
          - primitive    -> "name"
          - list<T>      -> "name.list.element" (recurse into T)
          - map<K,V>     -> "name.key_value.key" / ".value" (recurse into K, V)
          - struct<...>  -> "name.field" per field (recurse into each)
        """
        name = f"{prefix}{field.name}" if prefix else field.name
        t = field.type

        if pa.types.is_list(t) or pa.types.is_large_list(t):
            element = t.value_field.with_name("element")
            return UnifiedTableBuilder._leaf_column_paths(element, prefix=f"{name}.list.")
        if pa.types.is_map(t):
            kv = f"{name}.key_value."
            key = t.key_field.with_name("key")
            value = t.item_field.with_name("value")
            return (UnifiedTableBuilder._leaf_column_paths(key, prefix=kv)
                    + UnifiedTableBuilder._leaf_column_paths(value, prefix=kv))
        if pa.types.is_struct(t):
            paths: List[str] = []
            for i in range(t.num_fields):
                paths.extend(
                    UnifiedTableBuilder._leaf_column_paths(t.field(i), prefix=f"{name}."))
            return paths
        return [name]

    def _build_write_kwargs(self, data_schema: pa.Schema,
                            write_options: WriteOptionsProvider) -> Dict[str, Any]:
        """Build kwargs for pq.write_table from write_options.

        Data-file compression comes exclusively from write_options — every
        column must supply a codec (there is no default fallback). Requesting an
        explicit (non-dictionary) encoding sets use_dictionary=False so PyArrow
        doesn't override it with its default dictionary encoding.
        """
        col_names = [data_schema.field(i).name for i in range(len(data_schema))]

        compression_map = {}
        for i, col_name in enumerate(col_names):
            codec = write_options.get_compression(col_name)
            if not codec:
                raise ValueError(
                    f"write_options supplies no compression codec for column "
                    f"'{col_name}'; data-file compression must be explicit.")
            for leaf in self._leaf_column_paths(data_schema.field(i)):
                compression_map[leaf] = codec.lower()

        # Any incompatibility should have been caught by the CLI gate; if one
        # slips through, fail loudly here rather than silently swapping the
        # requested encoding for a fallback.
        incompatibilities = write_options.validate_compatibility()
        if incompatibilities:
            raise ValueError(
                "Incompatible write options: " + "; ".join(incompatibilities))

        encoding_map = {}
        has_explicit_encoding = False
        for i, col_name in enumerate(col_names):
            enc = write_options.get_encoding(col_name)
            if enc is None:
                continue
            if enc.upper() == 'DICTIONARY':
                continue  # expressed via use_dictionary, not column_encoding
            for leaf in self._leaf_column_paths(data_schema.field(i)):
                encoding_map[leaf] = enc
            has_explicit_encoding = True

        kwargs: Dict[str, Any] = {'compression': compression_map}
        if has_explicit_encoding:
            # Explicit encoding was requested, so a failure to apply it must fail
            # the build loudly rather than silently fall back to writer defaults
            # (a silently mis-encoded table is worse than no table).
            import pyarrow
            try:
                pa_version = tuple(int(x) for x in pyarrow.__version__.split('.')[:2])
            except (ValueError, AttributeError) as e:
                raise RuntimeError(
                    f"Could not parse PyArrow version {pyarrow.__version__!r} to "
                    f"confirm column_encoding support (requires >=14.0); refusing "
                    f"to write with requested encoding {encoding_map}.") from e
            if pa_version < (14, 0):
                raise RuntimeError(
                    f"PyArrow {pyarrow.__version__} does not support column_encoding "
                    f"(requires >=14.0); cannot honor requested encoding {encoding_map}.")
            kwargs['use_dictionary'] = False
            kwargs['column_encoding'] = encoding_map
        return kwargs

    def _write_delete_files(self, config, delete_file_ids, file_index_to_path, data_dir):
        """Write position-delete parquet files."""
        delete_schema = pa.schema([
            pa.field('file_path', pa.string(), nullable=False)
                .with_metadata({'PARQUET:field_id': '2147483546'}),
            pa.field('pos', pa.int64(), nullable=False)
                .with_metadata({'PARQUET:field_id': '2147483545'})
        ])

        file_infos = []

        for del_file_id in delete_file_ids:
            delete_file = next(
                (f for f in config['delete_files'] if f['file_id'] == del_file_id), None)
            if delete_file is None:
                raise ValueError(
                    f"Config missing delete_file with file_id='{del_file_id}'. "
                    f"Available: {[f['file_id'] for f in config['delete_files']]}")
            delete_rows = []

            for delete in delete_file['deletes']:
                data_file_path = file_index_to_path.get(delete['data_file_index'])
                if not data_file_path:
                    raise ValueError(
                        f"Position delete references data_file_index={delete['data_file_index']} "
                        f"but no physical path is known. Known indices: "
                        f"{sorted(file_index_to_path.keys())}")
                delete_rows.append((data_file_path, delete['position']))

            if not delete_rows:
                continue

            file_paths_col = [r[0] for r in delete_rows]
            positions_col = [r[1] for r in delete_rows]

            delete_table = pa.Table.from_arrays(
                [pa.array(file_paths_col), pa.array(positions_col)],
                schema=delete_schema)

            file_name = self._next_file_name("delete")
            file_path = data_dir / file_name
            pq.write_table(delete_table, str(file_path), compression=DELETE_COMPRESSION)

            file_size = file_path.stat().st_size
            file_infos.append((str(file_path), file_size, len(delete_rows)))

        return file_infos

    # =========================================================================
    # ROW DELTA JVM COMMIT
    # =========================================================================

    def _commit_row_delta(self, spark, full_table_name, data_file_infos, delete_file_infos):
        """Commit data + delete files atomically via Iceberg JVM RowDelta API."""
        jvm = spark._jvm

        jvm_table = jvm.org.apache.iceberg.spark.Spark3Util \
            .loadIcebergTable(spark._jsparkSession, full_table_name)

        spec = jvm_table.spec()
        FileFormat = jvm.org.apache.iceberg.FileFormat

        row_delta = jvm_table.newRowDelta()

        for path, size, count in data_file_infos:
            data_file = jvm.org.apache.iceberg.DataFiles.builder(spec) \
                .withPath(path) \
                .withFileSizeInBytes(size) \
                .withRecordCount(count) \
                .withFormat(FileFormat.PARQUET) \
                .build()
            row_delta.addRows(data_file)

        for path, size, count in delete_file_infos:
            delete_file = jvm.org.apache.iceberg.FileMetadata \
                .deleteFileBuilder(spec) \
                .ofPositionDeletes() \
                .withPath(path) \
                .withFileSizeInBytes(size) \
                .withRecordCount(count) \
                .withFormat(FileFormat.PARQUET) \
                .build()
            row_delta.addDeletes(delete_file)

        row_delta.commit()

    # =========================================================================
    # HELPERS
    # =========================================================================

    def _get_table_path(self, full_table_name: str) -> Path:
        """Convert full table name to filesystem path.

        Queries the Spark catalog to get the actual table location,
        falling back to computing from warehouse_path if unavailable.
        """
        if self._spark is not None:
            try:
                props = self._spark.sql(
                    f"DESCRIBE EXTENDED {full_table_name}"
                ).collect()
                for row in props:
                    if row['col_name'] == 'Location':
                        loc = row['data_type']
                        if loc.startswith('file:'):
                            loc = loc[len('file:'):]
                        return Path(loc)
            except Exception:
                pass

        # Fallback: compute from warehouse path
        parts = full_table_name.replace('local.', '').split('.')
        path = self.warehouse_path
        for part in parts:
            path = path / part
        return path

    def _harvest_field_ids(self, table_path: Path) -> Dict[Tuple[str, ...], int]:
        """Read the field-id assignment Iceberg wrote into the table metadata.

        Returns a {field_path -> id} map in the SAME shape as
        data_types.compute_field_ids, so the two can be compared with ==. Field
        paths follow the provider convention: a top-level column is a 1-tuple,
        e.g. ('value',); nested nodes extend it — list -> ('xs','element'),
        map -> ('m','key')/('m','value'), struct -> ('r','a').

        Iceberg's metadata encodes a field's type as either a primitive string
        or a nested object with its own ids: struct fields carry 'id' + 'name';
        list carries 'element-id' (+ 'element' type); map carries 'key-id' /
        'value-id' (+ 'key'/'value' types).
        """
        metadata_dir = table_path / "metadata"
        metadata_files = sorted(
            metadata_dir.glob("v*.metadata.json"),
            key=lambda p: int(p.name.split('.')[0][1:]), reverse=True)
        if not metadata_files:
            raise FileNotFoundError(f"No metadata files in {metadata_dir}")
        with open(metadata_files[0]) as f:
            metadata = json.load(f)

        current_schema_id = metadata.get("current-schema-id", 0)
        schema = next(
            (s for s in metadata.get("schemas", [])
             if s.get("schema-id") == current_schema_id), None)
        if schema is None:
            raise ValueError(f"Schema {current_schema_id} not found in metadata")

        field_ids: Dict[Tuple[str, ...], int] = {}

        def walk_type(path: Tuple[str, ...], type_node: Any) -> None:
            """Recurse into a type node, stamping nested ids under `path`."""
            if not isinstance(type_node, dict):
                return  # primitive type string — no nested ids
            kind = type_node.get("type")
            if kind == "struct":
                for fld in type_node.get("fields", []):
                    child_path = path + (fld["name"],)
                    field_ids[child_path] = fld["id"]
                    walk_type(child_path, fld["type"])
            elif kind == "list":
                child_path = path + ("element",)
                field_ids[child_path] = type_node["element-id"]
                walk_type(child_path, type_node["element"])
            elif kind == "map":
                key_path = path + ("key",)
                val_path = path + ("value",)
                field_ids[key_path] = type_node["key-id"]
                field_ids[val_path] = type_node["value-id"]
                walk_type(key_path, type_node["key"])
                walk_type(val_path, type_node["value"])
            else:
                # A dict type_node must be struct/list/map — the only complex
                # Iceberg kinds. Any other value means a bug or an unhandled new
                # kind / changed metadata format. Fail loudly rather than
                # silently returning a field-id map with missing subtree ids
                # (which would make the V8 comparison wrong, not just fail).
                raise ValueError(
                    f"Unexpected complex type kind {kind!r} at path {path} in "
                    f"table metadata; _harvest_field_ids cannot map its field ids.")

        for fld in schema.get("fields", []):
            path = (fld["name"],)
            field_ids[path] = fld["id"]
            walk_type(path, fld["type"])

        return field_ids

    def _get_sentinel_file_size(self, spark, full_table_name) -> Optional[int]:
        """Query sentinel file size from Iceberg metadata."""
        try:
            spark.sql(f"REFRESH TABLE {full_table_name}")
            files = spark.sql(f"""
                SELECT file_size_in_bytes, record_count
                FROM {full_table_name}.files
                WHERE content = 0
            """).collect()
            for row in files:
                if row.record_count == self.target_row_count:
                    return row.file_size_in_bytes
            return None
        except Exception as e:
            logger.warning("Failed to query sentinel file size: %s", e)
            return None

    def _iceberg_type_to_arrow(self, iceberg_type: str) -> pa.DataType:
        """Map an Iceberg type string to a PyArrow type.

        Raises on an unrecognized type rather than silently defaulting to int32
        — an unsupported type should fail the build loudly, not corrupt the
        column (per PR#60 review).
        """
        mapping = {
            "int": pa.int32(), "integer": pa.int32(),
            "long": pa.int64(), "float": pa.float32(),
            "double": pa.float64(), "string": pa.string(),
            "boolean": pa.bool_(), "binary": pa.binary(),
            "date": pa.date32(), "timestamp": pa.timestamp('us'),
            "timestamptz": pa.timestamp('us', tz='UTC'),
        }
        if iceberg_type not in mapping:
            raise ValueError(f"Unsupported Iceberg type: {iceberg_type!r}")
        return mapping[iceberg_type]

    # =========================================================================
    # METADATA QUERY
    # =========================================================================

    def _query_table_metadata(self, spark, full_table_name) -> BuildStats:
        """Query table metadata from Iceberg catalog. Pure read — no validation logic."""
        spark.sql(f"REFRESH TABLE {full_table_name}")

        row_count = spark.sql(f"SELECT COUNT(*) FROM {full_table_name}").collect()[0][0]

        # File counts from latest snapshot
        try:
            summary_row = spark.sql(f"""
                SELECT summary FROM {full_table_name}.snapshots
                ORDER BY committed_at DESC LIMIT 1
            """).collect()[0]
            summary = summary_row['summary']
            actual_data_files = int(summary.get('total-data-files', 0))
            actual_delete_files = int(summary.get('total-delete-files', 0))
        except Exception as e:
            logger.warning("Failed to query file counts for %s: %s", full_table_name, e)
            actual_data_files = 0
            actual_delete_files = 0

        # Snapshot count
        try:
            snapshot_count = spark.sql(
                f"SELECT COUNT(*) FROM {full_table_name}.snapshots").collect()[0][0]
        except Exception as e:
            logger.warning("Failed to query snapshot count for %s: %s", full_table_name, e)
            snapshot_count = 0

        # Total size
        try:
            size_row = spark.sql(f"""
                SELECT SUM(file_size_in_bytes) as total
                FROM {full_table_name}.files
            """).collect()[0]
            total_size = size_row['total'] or 0
        except Exception as e:
            logger.warning("Failed to query total size for %s: %s", full_table_name, e)
            total_size = 0

        return BuildStats(
            snapshot_count=snapshot_count,
            data_file_count=actual_data_files,
            delete_file_count=actual_delete_files,
            record_count=row_count,
            total_size_bytes=total_size,
        )

    # =========================================================================
    # VALIDATION
    # =========================================================================

    def _validate_against_config(self, spark, full_table_name, config,
                                 stats: BuildStats,
                                 schema_provider: SchemaProvider = None
                                 ) -> Tuple[Dict[str, bool], List[str]]:
        """Compare observed table metadata against expected config. Returns (validations, messages).

        schema_provider, when supplied, enables the field-id check (V8): the ids
        we STAMPED into the parquet files (computed) must equal the ids Iceberg
        assigned in the created table's metadata (harvested).
        """
        validations = {}
        messages = []
        sentinel_file_id = config.get('sentinel_in_file')

        # Expected row count: sum of data rows (sentinel padded) minus deleted rows
        total_rows = 0
        for df in config['data_files']:
            if sentinel_file_id and df['file_id'] == sentinel_file_id:
                total_rows += self.target_row_count
            else:
                total_rows += len(df['rows'])
        all_deleted = set(d['row_id'] for df in config['delete_files'] for d in df['deletes'])
        expected_rows = total_rows - len(all_deleted)

        # V1: Row count
        validations['row_count'] = (stats.record_count == expected_rows)
        if not validations['row_count']:
            messages.append(f"Row count: expected {expected_rows}, got {stats.record_count}")

        # V2: Data file count
        expected_data = len(config['data_files'])
        validations['data_file_count'] = (stats.data_file_count == expected_data)
        if not validations['data_file_count']:
            messages.append(f"Data files: expected {expected_data}, got {stats.data_file_count}")

        # V3: Delete file count
        expected_del = len(config['delete_files'])
        validations['delete_file_count'] = (stats.delete_file_count == expected_del)
        if not validations['delete_file_count']:
            messages.append(
                f"Delete files: expected {expected_del}, got {stats.delete_file_count}")

        # V4: Snapshot count
        config_snaps = sum(1 for s in config['snapshots']
                          if s['data_files'] or s['delete_files'])
        validations['snapshot_count'] = (stats.snapshot_count >= config_snaps)
        if not validations['snapshot_count']:
            messages.append(
                f"Snapshots: expected >= {config_snaps}, got {stats.snapshot_count}")

        # V5: File row distribution
        try:
            file_records = spark.sql(f"""
                SELECT record_count FROM {full_table_name}.files WHERE content = 0
            """).collect()
            expected_counts = sorted(
                self.target_row_count if (sentinel_file_id and df['file_id'] == sentinel_file_id)
                else len(df['rows'])
                for df in config['data_files']
            )
            actual_counts = sorted(r['record_count'] for r in file_records)
            validations['file_row_distribution'] = (expected_counts == actual_counts)
            if not validations['file_row_distribution']:
                messages.append(
                    f"File distribution: expected {expected_counts}, got {actual_counts}")
        except Exception as e:
            validations['file_row_distribution'] = False
            messages.append(f"File distribution check failed: {e}")

        # V6: Position delete contents
        if config['delete_files']:
            try:
                table_path = self._get_table_path(full_table_name)
                data_dir = table_path / "data"
                del_files = list(data_dir.glob("delete-*.parquet"))
                total_del_entries = sum(len(pq.read_table(f)) for f in del_files)
                expected_del_entries = sum(
                    len(df['deletes']) for df in config['delete_files'])
                validations['position_delete_contents'] = (
                    total_del_entries == expected_del_entries)
                if not validations['position_delete_contents']:
                    messages.append(
                        f"Position deletes: expected {expected_del_entries}, "
                        f"got {total_del_entries}")
            except Exception as e:
                validations['position_delete_contents'] = False
                messages.append(f"Position delete check failed: {e}")
        else:
            validations['position_delete_contents'] = True

        # V7: Sentinel file size
        if sentinel_file_id:
            sentinel_size = self._get_sentinel_file_size(spark, full_table_name)
            min_bytes = int(self.target_size_mb * 1024 * 1024)
            validations['sentinel_file_size'] = (
                sentinel_size is not None and sentinel_size >= min_bytes)
            if not validations['sentinel_file_size']:
                messages.append(
                    f"Sentinel size: {sentinel_size} bytes < {min_bytes} required")
        else:
            validations['sentinel_file_size'] = True

        # V8: Field ids — the ids we STAMPED into the data parquet (computed by
        # the provider) must equal the ids Iceberg assigned in the table
        # metadata. A mismatch means the files point at the wrong columns, which
        # would silently read wrong data; surface it via the standard channel.
        if schema_provider is not None:
            try:
                computed = schema_provider.get_field_ids()
                harvested = self._harvest_field_ids(
                    self._get_table_path(full_table_name))
                validations['field_ids'] = (computed == harvested)
                if not validations['field_ids']:
                    messages.append(
                        f"Field ids: computed {computed}, table has {harvested}")
            except Exception as e:
                validations['field_ids'] = False
                messages.append(f"Field id check failed: {e}")

        return validations, messages

    def close(self):
        """Stop Spark session."""
        if self._spark is not None:
            self._spark.stop()
            self._spark = None
