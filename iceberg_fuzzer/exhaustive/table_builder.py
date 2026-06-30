#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
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

logger = logging.getLogger(__name__)

ICEBERG_SPARK_RUNTIME = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1"


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

        # Measure bytes per row for sentinel sizing
        self.bytes_per_row = self._measure_bytes_per_row()
        target_bytes = target_size_mb * 1024 * 1024
        self._base_target_row_count = max(1, int(target_bytes / self.bytes_per_row))
        self.target_row_count = self._base_target_row_count

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def _measure_bytes_per_row(self) -> int:
        """Measure bytes per row by writing a sample parquet file."""
        sample_size = 1000
        schema = pa.schema([pa.field("value", pa.int32(), nullable=False)
                           .with_metadata({"PARQUET:field_id": "1"})])
        table = pa.Table.from_arrays(
            [pa.array(list(range(sample_size)), type=pa.int32())], schema=schema)
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
            tmp_path = tmp.name
        try:
            pq.write_table(table, tmp_path, compression='zstd')
            return os.path.getsize(tmp_path) // sample_size
        finally:
            os.unlink(tmp_path)

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

    def build_table(self, config: Dict[str, Any], namespace: str, table_name: str) -> BuildStats:
        """
        Build an Iceberg table from a raw config dict.

        The config dict comes directly from the exhaustive generator — all fields
        (data_files, delete_files, snapshots, sentinel_in_file, value_strategy, etc.)
        are read directly. No wrappers.
        """
        required_keys = ('data_files', 'delete_files', 'snapshots', 'row_count')
        missing = [k for k in required_keys if k not in config]
        if missing:
            raise ValueError(
                f"Config missing required keys: {missing}. Got keys: {list(config.keys())}")

        spark = self._get_spark()

        full_namespace = f"local.{namespace}"
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {full_namespace}")

        full_table_name = f"{full_namespace}.{table_name}"
        min_sentinel_bytes = int(self.target_size_mb * 1024 * 1024)
        max_retries = 5

        # Reset target_row_count per table so sentinel retries don't leak
        self.target_row_count = self._base_target_row_count

        for attempt in range(max_retries + 1):
            spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")

            # Create table — nullable if config uses nulls strategy
            nullable = config.get('value_strategy') == 'nulls'
            col_def = 'value INT' if nullable else 'value INT NOT NULL'
            spark.sql(f"""
                CREATE TABLE {full_table_name} ({col_def})
                USING iceberg
                TBLPROPERTIES (
                    'write.delete.mode' = 'merge-on-read',
                    'format-version' = '2'
                )
            """)

            # Build all snapshots
            self._build_snapshots(spark, full_table_name, config)

            # Check sentinel file size
            sentinel_file_id = config.get('sentinel_in_file')
            if not sentinel_file_id:
                break

            sentinel_size = self._get_sentinel_file_size(spark, full_table_name)
            if sentinel_size is None or sentinel_size >= min_sentinel_bytes:
                break

            if attempt == max_retries:
                break

            # Adjust target_row_count and retry
            compressed_bpr = max(1, sentinel_size // self.target_row_count)
            deficit = min_sentinel_bytes - sentinel_size
            extra_rows = max(1, deficit // compressed_bpr + 1)
            self.target_row_count += extra_rows

        stats = self._query_table_metadata(spark, full_table_name)
        validations, messages = self._validate_against_config(
            spark, full_table_name, config, stats)
        stats.validations = validations
        stats.validation_messages = messages
        return stats

    # =========================================================================
    # SNAPSHOT BUILDING
    # =========================================================================

    def _build_snapshots(self, spark: SparkSession, full_table_name: str, config: Dict[str, Any]):
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
                file_index_to_path)

    # =========================================================================
    # ROW DELTA PATH: PyArrow + JVM API
    # =========================================================================

    def _build_snapshot_row_delta(self, spark, full_table_name, config, snapshot_idx,
                                  data_file_ids, delete_file_ids, sentinel_file_id,
                                  file_index_to_path):
        """Build a snapshot with multiple files using RowDelta."""
        table_path = self._get_table_path(full_table_name)
        data_dir = table_path / "data"
        data_dir.mkdir(parents=True, exist_ok=True)

        # Step 1: Write data parquet files
        new_data_file_infos = []

        if data_file_ids:
            new_data_file_infos = self._write_data_files(
                config, data_file_ids, sentinel_file_id, data_dir, table_path,
                file_index_to_path)

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
                          table_path, file_index_to_path):
        """Write data parquet files with Iceberg field IDs via PyArrow."""
        field_ids = self._read_iceberg_field_ids(table_path)

        # Build schema with field IDs
        pa_fields = []
        for col_name, col_type, field_id, required in field_ids:
            pa_type = self._iceberg_type_to_arrow(col_type)
            pa_fields.append(
                pa.field(col_name, pa_type, nullable=not required)
                .with_metadata({'PARQUET:field_id': str(field_id)}))
        data_schema = pa.schema(pa_fields)

        file_infos = []

        for file_id in data_file_ids:
            data_file = next(
                (f for f in config['data_files'] if f['file_id'] == file_id), None)
            if data_file is None:
                raise ValueError(
                    f"Config missing data_file with file_id='{file_id}'. "
                    f"Available: {[f['file_id'] for f in config['data_files']]}")
            values = [row['value'] for row in data_file['rows']]

            if sentinel_file_id and file_id == sentinel_file_id:
                values = self._pad_sentinel_values(values)

            arrays = [pa.array(values, type=pa_fields[0].type)]
            table = pa.Table.from_arrays(arrays, schema=data_schema)

            file_name = self._next_file_name("data")
            file_path = data_dir / file_name
            pq.write_table(table, str(file_path), compression='zstd')

            file_size = file_path.stat().st_size
            file_infos.append((str(file_path), file_size, len(values)))

            # Register in the cross-snapshot map for position-delete resolution
            file_index_to_path[data_file['file_index']] = str(file_path)

        return file_infos

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
            pq.write_table(delete_table, str(file_path), compression='zstd')

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

    def _pad_sentinel_values(self, values: List[Any]) -> List[Any]:
        """Pad values list for sentinel file to reach target size."""
        padding_needed = self.target_row_count - len(values)
        if padding_needed > 0:
            values = values + [-(i + 1000000) for i in range(padding_needed)]
        return values

    def _read_iceberg_field_ids(self, table_path: Path) -> List[Tuple[str, str, int, bool]]:
        """Read field IDs and nullability from Iceberg metadata.

        Returns list of (name, type, field_id, required).
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
        for s in metadata.get("schemas", []):
            if s.get("schema-id") == current_schema_id:
                return [(fld["name"], fld["type"], fld["id"], fld.get("required", True))
                        for fld in s["fields"]]
        raise ValueError(f"Schema {current_schema_id} not found")

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
        """Map Iceberg type string to PyArrow type."""
        mapping = {
            "int": pa.int32(), "integer": pa.int32(),
            "long": pa.int64(), "float": pa.float32(),
            "double": pa.float64(), "string": pa.string(),
            "boolean": pa.bool_(), "binary": pa.binary(),
            "date": pa.date32(), "timestamp": pa.timestamp('us'),
            "timestamptz": pa.timestamp('us', tz='UTC'),
        }
        return mapping.get(iceberg_type, pa.int32())

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
                                 stats: BuildStats) -> Tuple[Dict[str, bool], List[str]]:
        """Compare observed table metadata against expected config. Returns (validations, messages)."""
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

        return validations, messages

    def close(self):
        """Stop Spark session."""
        if self._spark is not None:
            self._spark.stop()
            self._spark = None
