"""Spark's half of the shared content hash must match the Python reference in canonical.py."""

import datetime
import decimal
import math
import shutil

import pytest

pyspark = pytest.importorskip("pyspark")
if shutil.which("java") is None:
    pytest.skip("Spark needs a JVM", allow_module_level=True)

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import types as T  # noqa: E402

from iceberg_fuzzer.backend.canonical import HASH_MODULUS, row_hash  # noqa: E402
from iceberg_fuzzer.backend.pyspark.validation import PySparkValidation  # noqa: E402

# Spark has no time or uuid type, so those are covered by the DuckDB test only.
_FIELDS = [
    ("b", T.BooleanType(), True),
    ("i", T.IntegerType(), -7),
    ("l", T.LongType(), 9007199254740993),
    ("f", T.FloatType(), math.nan),
    ("d", T.DoubleType(), -0.0),
    ("dec", T.DecimalType(10, 2), decimal.Decimal("12.50")),
    ("s", T.StringType(), ""),
    ("bin", T.BinaryType(), b"\x00\xff"),
    ("dt", T.DateType(), datetime.date(1969, 12, 31)),
    ("ts", T.TimestampType(), datetime.datetime(2026, 2, 28, 1, 2, 3, 4, tzinfo=datetime.timezone.utc)),
    ("ntz", T.TimestampNTZType(), datetime.datetime(2026, 2, 28, 1, 2, 3, 4)),
    ("lst", T.ArrayType(T.IntegerType()), [1, None, 3]),
    ("m", T.MapType(T.StringType(), T.IntegerType()), {"b": 2, "a": 1}),
    ("st", T.StructType([T.StructField("x", T.IntegerType()), T.StructField("y", T.StringType())]), {"x": 1, "y": None}),
]


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_spark_matches_the_python_reference_for_every_type(spark):
    schema = T.StructType([T.StructField(name, spark_type) for name, spark_type, _ in _FIELDS])
    row = tuple(T.Row(**v) if isinstance(v, dict) and isinstance(t, T.StructType) else v for _, t, v in _FIELDS)
    spark.createDataFrame([row, row], schema).createOrReplaceTempView("typed")
    reader = PySparkValidation("unused")
    reader._spark = spark
    one = row_hash(v for _, _, v in _FIELDS)
    assert reader._count_and_hash("typed") == (2, (2 * one) % HASH_MODULUS)
