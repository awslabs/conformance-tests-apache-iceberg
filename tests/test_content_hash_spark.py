"""Spark's half of the shared content hash must match the Python reference in canonical.py, with
its Python workers and its session in a time zone far from UTC."""

import datetime
import decimal
import math
import time

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as T

from iceberg_fuzzer.backend.canonical import HASH_MODULUS, Epoch, MapPairs, row_hash
from iceberg_fuzzer.backend.pyspark.validation import PySparkValidation, _to_canonical

# Fourteen hours east of UTC, so a value read in this zone where UTC was meant moves by a day.
FAR_ZONE, FAR_OFFSET_SECONDS = "Pacific/Kiritimati", 14 * 3600
# Microseconds and days since the epoch of the first instant of year 10000 and of year 0, which
# Python's datetime cannot hold, and of the ends of the range it can.
YEAR_10000_MICROS, YEAR_0_MICROS = 253_402_300_800_000_000, -62_167_219_200_000_000
YEAR_10000_DAYS, YEAR_0_DAYS = 2_932_897, -719_528
LAST_MICROS, FIRST_MICROS = 253_402_300_799_999_999, -62_135_596_800_000_000

# Spark has no time or uuid type, so those are covered by the DuckDB test only.
_FIELDS = [
    ("b", T.BooleanType(), True),
    ("i", T.IntegerType(), -7),
    ("l", T.LongType(), 9007199254740993),
    ("f", T.FloatType(), math.nan),
    ("d", T.DoubleType(), -0.0),
    ("dec", T.DecimalType(10, 2), decimal.Decimal("12.50")),
    # PySpark hands a decimal with an exponent below -6 in engineering notation.
    ("tiny", T.DecimalType(38, 10), decimal.Decimal("-1E-10")),
    ("wide", T.DecimalType(38, 10), decimal.Decimal("-9999999999999999999999999999.9999999999")),
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
    # A session an earlier module left running would keep its Python workers' zone.
    if (running := SparkSession.getActiveSession()) is not None:
        running.stop()
    session = (
        SparkSession.builder.master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.executorEnv.TZ", FAR_ZONE)
        .config("spark.sql.session.timeZone", FAR_ZONE)
        .getOrCreate()
    )
    assert session.sparkContext.parallelize([0], 1).map(lambda _: time.timezone).collect() == [-FAR_OFFSET_SECONDS]
    yield session
    session.stop()


def _reader(spark) -> PySparkValidation:
    reader = PySparkValidation("unused")
    reader._spark = spark
    return reader


def test_spark_matches_the_python_reference_for_every_type_an_iceberg_table_gives_it(spark):
    schema = T.StructType([T.StructField(name, spark_type) for name, spark_type, _ in _FIELDS])
    row = tuple(T.Row(**v) if isinstance(v, dict) and isinstance(t, T.StructType) else v for _, t, v in _FIELDS)
    spark.createDataFrame([row, row], schema).createOrReplaceTempView("typed")
    one = row_hash(v for _, _, v in _FIELDS)
    assert _reader(spark)._count_and_hash("typed") == (2, (2 * one) % HASH_MODULUS)


def test_spark_hashes_temporal_values_at_and_past_the_ends_of_python_s_years_at_top_level_and_nested(spark):
    late, early = f"timestamp_micros({YEAR_10000_MICROS})", f"timestamp_micros({YEAR_0_MICROS})"
    last, first = f"timestamp_micros({LAST_MICROS})", f"timestamp_micros({FIRST_MICROS})"
    late_day, early_day = f"date_from_unix_date({YEAR_10000_DAYS})", f"date_from_unix_date({YEAR_0_DAYS})"
    spark.sql(
        f"SELECT {late} AS a, {early} AS b, {last} AS c, {first} AS d, CAST({late_day} AS TIMESTAMP_NTZ) AS e, {early_day} AS f, "
        f"array({late}, {early}) AS l, named_struct('t', CAST({early_day} AS TIMESTAMP_NTZ), 'd', {late_day}) AS s, "
        f"map({late_day}, {late}) AS m"
    ).createOrReplaceTempView("far")
    expected = row_hash([
        Epoch(b"T", YEAR_10000_MICROS), Epoch(b"T", YEAR_0_MICROS), Epoch(b"T", LAST_MICROS), Epoch(b"T", FIRST_MICROS),
        Epoch(b"T", YEAR_10000_MICROS), Epoch(b"d", YEAR_0_DAYS), [Epoch(b"T", YEAR_10000_MICROS), Epoch(b"T", YEAR_0_MICROS)],
        {"t": Epoch(b"T", YEAR_0_MICROS), "d": Epoch(b"d", YEAR_10000_DAYS)},
        MapPairs([(Epoch(b"d", YEAR_10000_DAYS), Epoch(b"T", YEAR_10000_MICROS))]),
    ])
    assert _reader(spark)._count_and_hash("far") == (1, expected)


def test_spark_hashes_maps_keyed_by_lists_and_structs(spark):
    spark.sql(
        "SELECT map(array(1, 2), 'a') AS by_list, map(named_struct('x', 1), 'b') AS by_struct, "
        "map(named_struct('d', CAST(1.50 AS DECIMAL(10, 2)), 't', timestamp_micros(0)), 'c') AS by_typed_struct"
    ).createOrReplaceTempView("keyed")
    assert _reader(spark)._count_and_hash("keyed") == (1, row_hash([
        MapPairs([([1, 2], "a")]), MapPairs([({"x": 1}, "b")]),
        MapPairs([({"d": decimal.Decimal("1.50"), "t": Epoch(b"T", 0)}, "c")]),
    ]))


def test_spark_hashes_timestamps_alike_in_any_session_time_zone(spark):
    schema = T.StructType([T.StructField("ts", T.TimestampType()), T.StructField("ntz", T.TimestampNTZType())])
    instant = datetime.datetime(2024, 7, 1, 12, 0, tzinfo=datetime.timezone.utc)
    spark.createDataFrame([(instant, instant.replace(tzinfo=None))], schema).createOrReplaceTempView("zoned")
    far = _reader(spark)._count_and_hash("zoned")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    try:
        assert _reader(spark)._count_and_hash("zoned") == far == (1, row_hash([instant, instant.replace(tzinfo=None)]))
    finally:
        spark.conf.set("spark.sql.session.timeZone", FAR_ZONE)


def test_decimals_pyspark_hands_in_engineering_notation_hash_at_their_scale():
    convert = _to_canonical(T.DecimalType(38, 10))
    wide = decimal.Decimal("-9999999999999999999999999999.9999999999")
    assert convert(decimal.Decimal("-100E-12")).as_tuple() == decimal.Decimal("-1E-10").as_tuple()
    assert convert(wide).as_tuple() == wide.as_tuple()
