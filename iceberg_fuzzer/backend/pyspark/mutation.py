"""PySpark backend — mutation (append, etc.)."""

import logging
import os

from pyspark.sql import Row

from iceberg_fuzzer.backend import MutationEngine
from iceberg_fuzzer.backend.pyspark.builders import create_spark_session
from iceberg_fuzzer.backend.pyspark.data import generate_value
from iceberg_fuzzer.common import row_id_from_index


class PySparkMutation(MutationEngine):
    """PySpark-based mutation engine using SparkSession and DataFrameWriterV2."""

    def __init__(self, warehouse_path: str):
        self._warehouse_path = warehouse_path
        self._spark = None

    def _get_spark(self):
        if self._spark is None:
            logging.getLogger("py4j").setLevel(logging.ERROR)
            os.environ.setdefault("SPARK_LOG_LEVEL", "ERROR")
            self._spark = create_spark_session(self._warehouse_path)
            self._spark.sparkContext.setLogLevel("ERROR")
        return self._spark

    def append_rows(self, namespace: str, table_name: str, num_rows: int):
        spark = self._get_spark()
        is_s3 = self._warehouse_path.startswith('arn:aws:s3tables:')
        catalog = "s3tables" if is_s3 else "local"
        qualified = f"{catalog}.{namespace}.{table_name}"

        schema = spark.table(qualified).schema
        starting_offset = spark.sql(f"SELECT COALESCE(MAX(id), 0) FROM {qualified}").collect()[0][0]

        rows = []
        for i in range(num_rows):
            rid = row_id_from_index(i, starting_offset)
            row_data = {
                field.name: generate_value(field.name, field.dataType.simpleString(), rid)
                for field in schema
            }
            rows.append(Row(**row_data))

        df = spark.createDataFrame(rows, schema)
        df.writeTo(qualified).append()
        print(f"    Appended {num_rows} rows to {qualified}")

    def close(self):
        if self._spark is not None:
            self._spark.stop()
            self._spark = None
