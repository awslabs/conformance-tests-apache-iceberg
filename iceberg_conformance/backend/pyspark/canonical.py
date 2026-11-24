"""PySpark implementation of canonical type formatting."""

from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, IntegerType, LongType, ShortType, ByteType,
    FloatType, DoubleType, DecimalType, StringType, BinaryType,
    DateType, TimestampType, TimestampNTZType,
    ArrayType, MapType, StructType,
)

from iceberg_fuzzer.backend.canonical import CanonicalCast


class PySparkCanonicalCast(CanonicalCast):
    """Produces PySpark Column expressions for canonical formatting."""

    def cast(self, col_name: str, col_type) -> Column:
        col = F.col(f"`{col_name}`")
        expr = self._expr_for_type(col, col_type)
        return F.coalesce(expr, F.lit(""))

    def _expr_for_type(self, col: Column, spark_type) -> Column:
        if isinstance(spark_type, BooleanType):
            return F.when(col, F.lit("true")).otherwise(F.lit("false"))

        if isinstance(spark_type, (IntegerType, LongType, ShortType, ByteType)):
            return col.cast("string")

        if isinstance(spark_type, (FloatType, DoubleType)):
            return col.cast("string")

        if isinstance(spark_type, DecimalType):
            return col.cast("string")

        if isinstance(spark_type, StringType):
            return col

        if isinstance(spark_type, DateType):
            return F.date_format(col, "yyyy-MM-dd")

        if isinstance(spark_type, TimestampType):
            return F.date_format(col, "yyyy-MM-dd HH:mm:ss.SSSSSS")

        if isinstance(spark_type, TimestampNTZType):
            return F.date_format(col, "yyyy-MM-dd HH:mm:ss.SSSSSS")

        if isinstance(spark_type, BinaryType):
            return F.lower(F.hex(col))

        if isinstance(spark_type, (ArrayType, MapType, StructType)):
            return F.to_json(col)

        return col.cast("string")
