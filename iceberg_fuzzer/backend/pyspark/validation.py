"""PySpark backend — validation via SparkSession table reads."""

from pyspark.sql import functions as F

from iceberg_fuzzer.backend import ColumnFilter, ValidationEngine, Checkpoint, AssertionResult
from iceberg_fuzzer.backend.point_query import format_sql_literal
from iceberg_fuzzer.backend.pyspark.canonical import PySparkCanonicalCast
from iceberg_fuzzer.backend.pyspark.builders import create_spark_session


class PySparkValidation(ValidationEngine):
    """PySpark-based validation engine using Spark SQL for table access.

    Computes content hashes using canonical type formatting for cross-engine agreement.
    """

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

    def _compute_hash(self, table_ref: str) -> int:
        spark = self._get_spark()
        df = spark.table(table_ref)

        canonical = PySparkCanonicalCast()
        canonical_cols = [
            canonical.cast(field.name, field.dataType)
            for field in df.schema
        ]
        concat_expr = F.concat_ws("|", *canonical_cols)
        hash_col = F.sha2(concat_expr, 256)
        # 15 hex chars = 60 bits, fits in signed long
        truncated = F.conv(F.substring(hash_col, 1, 15), 16, 10).cast("long")
        result = df.select(F.bit_xor(truncated).alias("h")).collect()[0]["h"]
        return result or 0

    def capture_checkpoint(self, warehouse: str, namespace: str, table_name: str) -> Checkpoint:
        table_ref = self._resolve_table_ref(namespace, table_name)
        spark = self._get_spark()
        row_count = spark.table(table_ref).count()
        content_hash = self._compute_hash(table_ref)
        return Checkpoint(row_count=row_count, content_hash=content_hash)

    def validate(self, checkpoint: Checkpoint, warehouse: str, namespace: str, table_name: str) -> list[AssertionResult]:
        current = self.capture_checkpoint(warehouse, namespace, table_name)
        return [
            AssertionResult.row_count(checkpoint.row_count, current.row_count),
            AssertionResult.content_hash(checkpoint.content_hash, current.content_hash),
        ]

    def filtered_count(self, warehouse, namespace, table_name, column_filter: ColumnFilter) -> int:
        table_ref = self._resolve_table_ref(namespace, table_name)
        spark = self._get_spark()
        if column_filter.is_null:
            where = f'`{column_filter.column}` IS NULL'
        else:
            where = f'`{column_filter.column}` = {format_sql_literal(column_filter.value)}'
        return spark.sql(f"SELECT COUNT(*) FROM {table_ref} WHERE {where}").collect()[0][0]

    def get_columns(self, warehouse, namespace, table_name) -> list[tuple[str, str]]:
        spark = self._get_spark()
        table_ref = self._resolve_table_ref(namespace, table_name)
        schema = spark.table(table_ref).schema
        return [(f.name, f.dataType.simpleString()) for f in schema if f.name != "id"]

    def get_row_ids(self, warehouse, namespace, table_name) -> set[int]:
        spark = self._get_spark()
        table_ref = self._resolve_table_ref(namespace, table_name)
        rows = spark.sql(f"SELECT id FROM {table_ref}").collect()
        return {row[0] for row in rows}

    def close(self):
        if self._spark is not None:
            self._spark.stop()
            self._spark = None
