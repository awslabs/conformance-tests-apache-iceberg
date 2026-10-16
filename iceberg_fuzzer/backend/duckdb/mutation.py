"""DuckDB backend: mutation through DuckDB's Parquet writer.

DuckDB cannot commit to these tables, so the rows, computed by Spark, are written by DuckDB
into one Parquet file per partition, and PyIceberg commits the files, with the partition each
was written for and statistics read from its footer. The files carry no field IDs, so readers
and compactors map their columns by the table's name mapping, which the commit sets if the
table has none. DuckDB writes every column as optional, even NOT NULL ones, and the files are
committed as they are, the way DuckDB's own Iceberg writer leaves them.

Statements are `COPY (SELECT ...) TO ${table} (OPTIONS)`, where the SELECT is Spark SQL and
OPTIONS are DuckDB COPY options such as `COMPRESSION zstd, PARQUET_VERSION v2`, or a plain
`INSERT INTO ${table} SELECT ...`.
"""

import os
import re
import uuid
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.io.pyarrow import (
    _determine_partitions,
    compute_statistics_plan,
    data_file_statistics_from_parquet_metadata,
    parquet_path_to_id_mapping,
)
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.table import Table, TableProperties
from pyiceberg.typedef import Record

from iceberg_fuzzer.backend.pyiceberg.mutation import PyIcebergMutation

COPY_TO = re.compile(r"^\s*COPY\s*\((SELECT\b.*)\)\s*TO\s+\S+\s*\((.*)\)\s*$", re.IGNORECASE | re.DOTALL)


class DuckDBMutation(PyIcebergMutation):
    """Appends rows that Spark computes, written by DuckDB and registered by PyIceberg."""

    def _parse(self, statement: str) -> tuple[str, str]:
        match = COPY_TO.match(statement)
        if match:
            return match.group(1), match.group(2).strip()
        return super()._parse(statement)

    def _write(self, table: Table, rows: pa.Table, options: str) -> None:
        metadata, schema = table.metadata, table.schema()
        if table.spec().fields:
            slices = [(p.partition_key.partition, p.arrow_table_partition) for p in _determine_partitions(table.spec(), schema, rows)]
        else:
            slices = [(Record(), rows)]
        data_dir = table.location().removeprefix("file:").rstrip("/") + "/data"
        Path(data_dir).mkdir(parents=True, exist_ok=True)
        data_files = []
        with duckdb.connect() as connection:
            for partition, rows_slice in slices:
                path = f"{data_dir}/duckdb-{uuid.uuid4()}.parquet"
                connection.register("rows", rows_slice)
                extra = f", {options}" if options else ""
                connection.execute(f"COPY rows TO '{path}' (FORMAT parquet{extra})")
                connection.unregister("rows")
                statistics = data_file_statistics_from_parquet_metadata(
                    parquet_metadata=pq.read_metadata(path),
                    stats_columns=compute_statistics_plan(schema, metadata.properties),
                    parquet_column_mapping=parquet_path_to_id_mapping(schema),
                )
                data_files.append(DataFile.from_args(
                    content=DataFileContent.DATA, file_path=path, file_format=FileFormat.PARQUET, partition=partition,
                    file_size_in_bytes=os.path.getsize(path), sort_order_id=None, spec_id=metadata.default_spec_id,
                    equality_ids=None, key_metadata=None, **statistics.to_serialized_dict(),
                ))
        with table.transaction() as transaction:
            if transaction.table_metadata.name_mapping() is None:
                transaction.set_properties(**{TableProperties.DEFAULT_NAME_MAPPING: schema.name_mapping.model_dump_json()})
            with transaction.update_snapshot().fast_append() as append:
                for data_file in data_files:
                    append.append_data_file(data_file)
