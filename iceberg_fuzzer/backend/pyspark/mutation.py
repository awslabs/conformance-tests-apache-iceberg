"""PySpark backend — mutation (append, etc.)."""

from string import Template

from pyspark.sql import Row

from iceberg_fuzzer.backend import AppendResult, DeleteResult, MutationEngine, SqlResult
from iceberg_fuzzer.backend.pyspark.builders import create_spark_session
from iceberg_fuzzer.backend.pyspark.data import generate_value
from iceberg_fuzzer.common import row_id_from_index


class PySparkMutation(MutationEngine):
    """PySpark-based mutation engine using SparkSession and DataFrameWriterV2."""

    def __init__(self, warehouse_path: str, seed: int | None = None):
        super().__init__(seed)
        self._warehouse_path = warehouse_path
        self._spark = None

    def _get_spark(self):
        if self._spark is None:
            self._spark = create_spark_session(self._warehouse_path)
        return self._spark

    def _catalog_name(self) -> str:
        is_s3 = self._warehouse_path.startswith('arn:aws:s3tables:')
        return "s3tables" if is_s3 else "local"

    def _qualified_name(self, namespace: str, table_name: str) -> str:
        return f"{self._catalog_name()}.{namespace}.{table_name}"

    def _get_current_snapshot_id(self, qualified: str) -> int | None:
        spark = self._get_spark()
        rows = spark.sql(
            f"SELECT snapshot_id FROM {qualified}.refs "
            f"WHERE name = 'main' AND type = 'BRANCH'"
        ).collect()
        return rows[0].snapshot_id if rows else None

    def append_rows(self, namespace: str, table_name: str, num_rows: int) -> AppendResult:
        spark = self._get_spark()
        qualified = self._qualified_name(namespace, table_name)

        schema = spark.table(qualified).schema
        starting_offset = spark.sql(f"SELECT COALESCE(MAX(id), 0) FROM {qualified}").collect()[0][0]

        appended_ids: set[int] = set()
        rows = []
        for i in range(num_rows):
            rid = row_id_from_index(i, starting_offset)
            appended_ids.add(rid)
            row_data = {
                field.name: generate_value(field.name, field.dataType.simpleString(), rid)
                for field in schema
            }
            rows.append(Row(**row_data))

        df = spark.createDataFrame(rows, schema)
        df.writeTo(qualified).append()
        print(f"    Appended {num_rows} rows to {qualified}")
        snapshot_id = self._get_current_snapshot_id(qualified)
        return AppendResult(snapshot_id=snapshot_id, appended_ids=appended_ids)

    def delete_rows(self, namespace: str, table_name: str, num_rows: int) -> DeleteResult:
        spark = self._get_spark()
        qualified = self._qualified_name(namespace, table_name)

        all_ids = sorted(row.id for row in spark.sql(f"SELECT id FROM {qualified}").collect())

        if num_rows > len(all_ids):
            raise ValueError(
                f"Cannot delete {num_rows} rows from {qualified}: only {len(all_ids)} rows exist"
            )

        ids_to_delete = self._rng.sample(all_ids, num_rows)
        id_list = ", ".join(str(i) for i in ids_to_delete)
        spark.sql(f"DELETE FROM {qualified} WHERE id IN ({id_list})")
        print(f"    Deleted {num_rows} rows from {qualified}")
        return DeleteResult(
            snapshot_id=self._get_current_snapshot_id(qualified),
            deleted_ids=set(ids_to_delete),
        )

    def run_sql(self, namespace: str, table_name: str, statements: list[str]) -> SqlResult:
        spark = self._get_spark()
        qualified = self._qualified_name(namespace, table_name)
        placeholders = {'table': qualified, 'namespace': namespace, 'table_name': table_name, 'catalog': self._catalog_name()}
        for statement in statements:
            spark.sql(Template(statement).safe_substitute(placeholders))
        return SqlResult(
            snapshot_id=self._get_current_snapshot_id(qualified),
            row_count=spark.sql(f"SELECT COUNT(*) FROM {qualified}").collect()[0][0],
        )

    def rollback_to_snapshot(self, namespace: str, table_name: str, snapshot_id: int) -> int:
        spark = self._get_spark()
        catalog = self._catalog_name()
        qualified = self._qualified_name(namespace, table_name)
        spark.sql(
            f"CALL {catalog}.system.rollback_to_snapshot("
            f"table => '{namespace}.{table_name}', snapshot_id => {snapshot_id})"
        )
        print(f"    Rolled back {qualified} to snapshot {snapshot_id}")
        return self._get_current_snapshot_id(qualified)

    def close(self):
        if self._spark is not None:
            self._spark.stop()
            self._spark = None
