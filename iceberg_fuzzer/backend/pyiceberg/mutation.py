"""PyIceberg backend: mutation through PyIceberg's own Parquet writer and commit.

PyIceberg cannot evaluate SQL, so `run_sql` takes `INSERT INTO ${table} SELECT ...` statements,
computes the selected rows with Spark, and appends them with PyIceberg. The files and commits
are PyIceberg's, which differ from Spark's, for example in Arrow string types and manifests.

Local warehouses use the layout Spark's Hadoop catalog reads: `vN.metadata.json` files and a
`version-hint.text` naming the current one.
"""

import re
from pathlib import Path
from string import Template

import pyarrow as pa
from pyiceberg.catalog import MetastoreCatalog
from pyiceberg.io import load_file_io
from pyiceberg.serializers import FromInputFile
from pyiceberg.table import CommitTableResponse, Table

from iceberg_fuzzer.backend import AppendResult, DeleteResult, MutationEngine, SqlResult, is_s3_tables_warehouse
from iceberg_fuzzer.backend.pyspark.builders import create_spark_session

INSERT_SELECT = re.compile(r"^\s*INSERT\s+INTO\s+\S+\s+(SELECT\b.*)$", re.IGNORECASE | re.DOTALL)


class HadoopLayoutCatalog(MetastoreCatalog):
    """Loads and commits tables laid out as Spark's Hadoop catalog writes them. Supports only
    what appending to an existing table needs."""

    def __init__(self, warehouse: str):
        super().__init__("hadoop", warehouse=warehouse)
        self._warehouse = Path(warehouse)

    def _metadata_dir(self, identifier) -> Path:
        namespace, name = self.identifier_to_tuple(identifier)
        return self._warehouse / namespace / name / "metadata"

    def _current_version(self, identifier) -> int:
        return int((self._metadata_dir(identifier) / "version-hint.text").read_text().strip())

    def load_table(self, identifier) -> Table:
        location = str(self._metadata_dir(identifier) / f"v{self._current_version(identifier)}.metadata.json")
        io = load_file_io(self.properties, location)
        metadata = FromInputFile.table_metadata(io.new_input(location))
        return Table(self.identifier_to_tuple(identifier), metadata, location, io, catalog=self)

    def commit_table(self, table, requirements, updates) -> CommitTableResponse:
        identifier = table.name()
        staged = self._update_and_stage_table(self.load_table(identifier), identifier, requirements, updates)
        version = self._current_version(identifier) + 1
        location = str(self._metadata_dir(identifier) / f"v{version}.metadata.json")
        self._write_metadata(staged.metadata, staged.io, location)
        (self._metadata_dir(identifier) / "version-hint.text").write_text(str(version))
        return CommitTableResponse(metadata=staged.metadata, metadata_location=location)

    def _unsupported(self, *args, **kwargs):
        raise NotImplementedError("HadoopLayoutCatalog only loads and commits existing tables")

    create_table = register_table = drop_table = rename_table = _unsupported
    create_namespace = drop_namespace = list_namespaces = list_tables = _unsupported
    load_namespace_properties = update_namespace_properties = _unsupported
    list_views = drop_view = view_exists = load_view = register_view = _unsupported


class PyIcebergMutation(MutationEngine):
    """Appends rows that Spark computes, written and committed by PyIceberg."""

    def __init__(self, warehouse_path: str, seed: int | None = None):
        super().__init__(seed)
        if is_s3_tables_warehouse(warehouse_path):
            raise NotImplementedError("The PyIceberg mutation backend supports local warehouses only")
        self._warehouse_path = warehouse_path
        self._catalog = HadoopLayoutCatalog(warehouse_path)
        self._spark = None

    def _get_spark(self):
        if self._spark is None:
            self._spark = create_spark_session(self._warehouse_path)
        return self._spark

    def run_sql(self, namespace: str, table_name: str, statements: list[str]) -> SqlResult:
        spark = self._get_spark()
        qualified = f"local.{namespace}.{table_name}"
        placeholders = {'table': qualified, 'namespace': namespace, 'table_name': table_name}
        for statement in statements:
            query, options = self._parse(Template(statement).safe_substitute(placeholders))
            table = self._catalog.load_table((namespace, table_name))
            batches = spark.sql(query)._collect_as_arrow()
            if not batches:
                continue
            rows = pa.Table.from_batches(batches).rename_columns([f.name for f in table.schema().fields])
            self._write(table, rows.cast(table.schema().as_arrow()), options)
        snapshot = self._catalog.load_table((namespace, table_name)).current_snapshot()
        return SqlResult(
            snapshot_id=snapshot.snapshot_id if snapshot else None,
            row_count=spark.sql(f"SELECT COUNT(*) FROM {qualified}").collect()[0][0],
        )

    def _parse(self, statement: str) -> tuple[str, str]:
        """The Spark query that computes the rows, and writer options."""
        match = INSERT_SELECT.match(statement)
        if not match:
            raise ValueError(f"{type(self).__name__} runs only INSERT INTO ... SELECT statements: {statement[:80]}")
        return match.group(1), ""

    def _write(self, table: Table, rows: pa.Table, options: str) -> None:
        table.append(rows)

    def append_rows(self, namespace: str, table_name: str, num_rows: int) -> AppendResult:
        raise NotImplementedError("The PyIceberg mutation backend writes through sql steps")

    def delete_rows(self, namespace: str, table_name: str, num_rows: int) -> DeleteResult:
        raise NotImplementedError("The PyIceberg mutation backend writes through sql steps")

    def rollback_to_snapshot(self, namespace: str, table_name: str, snapshot_id: int) -> int:
        raise NotImplementedError("The PyIceberg mutation backend writes through sql steps")

    def close(self):
        self._spark = None
