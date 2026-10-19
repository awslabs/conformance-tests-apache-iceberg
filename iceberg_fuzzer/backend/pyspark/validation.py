"""PySpark backend — validation via SparkSession table reads."""

import datetime

from pyspark.sql.types import TimestampType

from iceberg_fuzzer.backend import (
    ValidationEngine,
    Checkpoint,
    AssertionResult,
    ColumnFilter,
    ColumnName,
    ColTypeStr,
)
from iceberg_fuzzer.backend.canonical import HASH_MODULUS, hash_rows
from iceberg_fuzzer.backend.point_query import format_sql_literal
from iceberg_fuzzer.backend.pyspark.builders import create_spark_session


def _as_utc(rows, local_time: list[int]):
    """PySpark returns TIMESTAMP values as naive datetimes in the Python process's local time,
    so they are made aware before hashing. TIMESTAMP_NTZ values stay naive."""
    for row in rows:
        values = list(row)
        for i in local_time:
            if values[i] is not None:
                values[i] = values[i].astimezone(datetime.timezone.utc)
        yield values


class PySparkValidation(ValidationEngine):
    """PySpark-based validation engine using Spark SQL for table access.

    Each Spark task hashes its rows with the shared encoding in canonical.py, and the driver
    adds the partial results. The rows go through the RDD API, because mapInArrow needs pandas.
    """

    reports_snapshots = True

    def __init__(self, warehouse_path: str):
        self._warehouse_path = warehouse_path
        self._spark = None

    def _get_spark(self):
        if self._spark is None:
            self._spark = create_spark_session(self._warehouse_path)
        return self._spark

    def _resolve_table_ref(self, namespace: str, table_name: str) -> str:
        is_s3 = self._warehouse_path.startswith('arn:aws:s3tables:')
        catalog = "s3tables" if is_s3 else "local"
        return f"{catalog}.{namespace}.{table_name}"

    def _count_and_hash(self, table_ref: str) -> tuple[int, int]:
        df = self._get_spark().table(table_ref)
        local_time = [i for i, f in enumerate(df.schema) if isinstance(f.dataType, TimestampType)]
        partials = df.rdd.mapPartitions(lambda rows: [hash_rows(_as_utc(rows, local_time))]).collect()
        return sum(rows for rows, _ in partials), sum(h for _, h in partials) % HASH_MODULUS

    def capture_checkpoint(self, warehouse: str, namespace: str, table_name: str) -> Checkpoint:
        row_count, content_hash = self._count_and_hash(self._resolve_table_ref(namespace, table_name))
        return Checkpoint(row_count=row_count, content_hash=content_hash)

    def validate(self, checkpoint: Checkpoint, warehouse: str, namespace: str, table_name: str) -> list[AssertionResult]:
        current = self.capture_checkpoint(warehouse, namespace, table_name)
        return [
            AssertionResult.row_count(checkpoint.row_count, current.row_count),
            AssertionResult.content_hash(checkpoint.content_hash, current.content_hash),
        ]

    def filtered_count(
        self,
        warehouse: str,
        namespace: str,
        table_name: str,
        column_filter: ColumnFilter,
    ) -> int:
        table_ref = self._resolve_table_ref(namespace, table_name)
        spark = self._get_spark()
        col = f"`{column_filter.column}`"
        if column_filter.is_null:
            where = f"{col} IS NULL"
        else:
            where = f"{col} = {format_sql_literal(column_filter.value)}"
        query = f"SELECT COUNT(*) FROM {table_ref} WHERE {where}"
        return spark.sql(query).collect()[0][0]

    def get_columns(self, warehouse: str, namespace: str, table_name: str) -> list[tuple[ColumnName, ColTypeStr]]:
        spark = self._get_spark()
        table_ref = self._resolve_table_ref(namespace, table_name)
        schema = spark.table(table_ref).schema
        return [(f.name, f.dataType.simpleString()) for f in schema if f.name != "id"]

    def get_row_ids(self, warehouse: str, namespace: str, table_name: str) -> set[int]:
        table_ref = self._resolve_table_ref(namespace, table_name)
        spark = self._get_spark()
        rows = spark.sql(f"SELECT id FROM {table_ref}").collect()
        return {row.id for row in rows}

    def current_snapshot(self, warehouse: str, namespace: str, table_name: str) -> tuple[int | None, str | None]:
        table_ref = self._resolve_table_ref(namespace, table_name)
        rows = self._get_spark().sql(
            f"SELECT s.snapshot_id, s.operation FROM {table_ref}.refs r "
            f"JOIN {table_ref}.snapshots s ON r.snapshot_id = s.snapshot_id "
            f"WHERE r.name = 'main' AND r.type = 'BRANCH'"
        ).collect()
        return (rows[0].snapshot_id, rows[0].operation) if rows else (None, None)

    def close(self):
        if self._spark is not None:
            self._spark.stop()
            self._spark = None
