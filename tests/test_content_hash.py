"""Unit tests for the shared, order-independent, duplicate-sensitive content hash."""

import datetime
import decimal
import math
import uuid

import duckdb
import pyarrow as pa
import pytest

from iceberg_fuzzer.backend.canonical import encode_value, hash_batches, row_hash
from iceberg_fuzzer.backend.duckdb import _count_and_hash


def _duckdb_hash(rows: list[tuple]) -> tuple[int, int]:
    conn = duckdb.connect()
    conn.execute("CREATE TABLE t (a VARCHAR, b VARCHAR, flag BOOLEAN)")
    if rows:
        conn.executemany("INSERT INTO t VALUES (?, ?, ?)", rows)
    return _count_and_hash(conn, "t")


def test_empty_table():
    assert _duckdb_hash([]) == (0, 0)


def test_row_order_does_not_matter():
    rows = [("x", "1", True), ("y", "2", False)]
    assert _duckdb_hash(rows) == _duckdb_hash(list(reversed(rows)))


def test_replacing_a_duplicate_pair_changes_the_hash():
    assert _duckdb_hash([("x", "1", True)] * 2 + [("y", "2", True)]) != _duckdb_hash(
        [("x", "1", True)] + [("y", "2", True)] * 2
    )


def test_null_differs_from_empty_string_and_from_the_string_null():
    assert len({_duckdb_hash([(v, "1", True)]) for v in (None, "", "NULL")}) == 3


def test_null_boolean_differs_from_false():
    assert _duckdb_hash([("x", "1", None)]) != _duckdb_hash([("x", "1", False)])


def test_field_boundaries_are_kept():
    assert _duckdb_hash([("ab", "c", True)]) != _duckdb_hash([("a", "bc", True)])


def test_signed_zero_and_nan():
    assert encode_value(-0.0) != encode_value(0.0)
    assert encode_value(math.nan) == encode_value(float("nan"))


def test_decimal_keeps_its_scale():
    assert encode_value(decimal.Decimal("1.50")) != encode_value(decimal.Decimal("1.5"))


def test_uuid_matches_its_string_form():
    value = uuid.UUID("550e8400-e29b-41d4-a716-446655440000")
    assert encode_value(value) == encode_value(str(value))


def test_aware_and_naive_utc_timestamps_match():
    naive = datetime.datetime(2026, 1, 1, 12, 0, 0, 5)
    assert encode_value(naive) == encode_value(naive.replace(tzinfo=datetime.timezone.utc))


def test_unsupported_type_raises():
    with pytest.raises(TypeError):
        encode_value(object())


# One row of every type an engine can hand over, as DuckDB SQL and as the Python values the
# shared encoder expects. DuckDB's Arrow export and the Python reference must hash alike.
_TYPED_COLUMNS = [
    ("b BOOLEAN", "true", True),
    ("i INTEGER", "-7", -7),
    ("l BIGINT", "9007199254740993", 9007199254740993),
    ("f FLOAT", "CAST('NaN' AS FLOAT)", math.nan),
    # A literal -0.0 is folded to 0.0, so the value goes in through a cast.
    ("d DOUBLE", "CAST('-0.0' AS DOUBLE)", -0.0),
    ("dec DECIMAL(10, 2)", "12.50", decimal.Decimal("12.50")),
    ("s VARCHAR", "''", ""),
    ("bin BLOB", "'\\x00\\xff'::BLOB", b"\x00\xff"),
    ("dt DATE", "DATE '1969-12-31'", datetime.date(1969, 12, 31)),
    ("tm TIME", "TIME '23:59:59.000001'", datetime.time(23, 59, 59, 1)),
    ("ts TIMESTAMP", "TIMESTAMP '2026-02-28 01:02:03.000004'", datetime.datetime(2026, 2, 28, 1, 2, 3, 4)),
    ("u UUID", "'550e8400-e29b-41d4-a716-446655440000'", "550e8400-e29b-41d4-a716-446655440000"),
    ("lst INTEGER[]", "[1, NULL, 3]", [1, None, 3]),
    ("m MAP(VARCHAR, INTEGER)", "MAP {'b': 2, 'a': 1}", {"a": 1, "b": 2}),
    ("st STRUCT(x INTEGER, y VARCHAR)", "{'x': 1, 'y': NULL}", {"x": 1, "y": None}),
]


def test_duckdb_matches_the_python_reference_for_every_type():
    conn = duckdb.connect()
    conn.execute(f"CREATE TABLE t ({', '.join(c for c, _, _ in _TYPED_COLUMNS)})")
    conn.execute(f"INSERT INTO t VALUES ({', '.join(sql for _, sql, _ in _TYPED_COLUMNS)})")
    expected = (1, row_hash(v for _, _, v in _TYPED_COLUMNS))
    assert _count_and_hash(conn, "t") == expected


def test_arrow_map_pairs_hash_like_a_dict():
    batch = pa.RecordBatch.from_pydict(
        {"m": pa.array([[("a", 1), ("b", 2)]], type=pa.map_(pa.string(), pa.int32()))}
    )
    assert hash_batches([batch]) == (1, row_hash([{"b": 2, "a": 1}]))
