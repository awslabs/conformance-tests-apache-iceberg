"""Spark's TIMESTAMP values hash the same as Arrow's at any depth, whatever the local time zone."""

import datetime
import os
import time

import pyarrow as pa
from pyspark.sql import Row
from pyspark.sql.types import ArrayType, IntegerType, MapType, StringType, StructField, StructType, TimestampType

from iceberg_fuzzer.backend.canonical import hash_batches, hash_rows
from iceberg_fuzzer.backend.pyspark.validation import _as_utc, _to_utc

# British Summer Time, so local time differs from UTC.
INSTANT = datetime.datetime(2024, 7, 1, 12, 0, tzinfo=datetime.timezone.utc)


def test_nested_timestamps_hash_like_arrow_outside_utc(monkeypatch):
    monkeypatch.setenv("TZ", "Europe/London")
    time.tzset()
    try:
        local = INSTANT.astimezone().replace(tzinfo=None)
        schema = StructType([
            StructField("s", StructType([StructField("a", IntegerType()), StructField("t", TimestampType())])),
            StructField("l", ArrayType(TimestampType())),
            StructField("m", MapType(StringType(), TimestampType())),
        ])
        spark_row = [Row(a=1, t=local), [local, None], {"k": local}]
        converters = [(i, c) for i, f in enumerate(schema.fields) if (c := _to_utc(f.dataType))]
        timestamp = pa.timestamp("us", "UTC")
        arrow = pa.table({
            "s": pa.array([{"a": 1, "t": INSTANT}], pa.struct([("a", pa.int32()), ("t", timestamp)])),
            "l": pa.array([[INSTANT, None]], pa.list_(timestamp)),
            "m": pa.array([[("k", INSTANT)]], pa.map_(pa.string(), timestamp)),
        })
        assert hash_rows(_as_utc([spark_row], converters)) == hash_batches(arrow.to_batches())
    finally:
        os.environ.pop("TZ", None)
        time.tzset()


def test_columns_without_timestamps_need_no_conversion():
    assert _to_utc(StructType([StructField("a", IntegerType())])) is None
    assert _to_utc(ArrayType(StringType())) is None
