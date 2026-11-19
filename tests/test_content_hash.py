"""Unit tests for the shared, order-independent, duplicate-sensitive content hash."""

import datetime
import decimal
import math
import uuid

import duckdb
import pyarrow as pa
import pytest

from iceberg_fuzzer.backend.canonical import Epoch, MapPairs, encode_value, hash_batches, row_hash
from iceberg_fuzzer.backend.duckdb import _connect, _count_and_hash

# Microseconds and days since the epoch of the first instant of year 10000 and of year 0, which
# Python's datetime cannot hold, and of the ends of the range it can.
YEAR_10000_MICROS, YEAR_0_MICROS = 253_402_300_800_000_000, -62_167_219_200_000_000
YEAR_10000_DAYS, YEAR_0_DAYS = 2_932_897, -719_528
LAST_MICROS, FIRST_MICROS = 253_402_300_799_999_999, -62_135_596_800_000_000


def _duckdb_hash(rows: list[tuple]) -> tuple[int, int]:
    conn = duckdb.connect()
    conn.execute("CREATE TABLE t (a VARCHAR, b VARCHAR, flag BOOLEAN)")
    if rows:
        conn.executemany("INSERT INTO t VALUES (?, ?, ?)", rows)
    return _count_and_hash(conn, "t")


@pytest.fixture(params=["plain", "as_validation_connects"])
def conn(request):
    """A plain DuckDB connection, and one built as DuckDBValidation builds its own, which hands
    strings, blobs, uuids and lists over in their large Arrow forms."""
    return duckdb.connect() if request.param == "plain" else _connect()


def _hash_of(conn, select: str) -> tuple[int, int]:
    conn.execute(f"CREATE TABLE t AS {select}")
    return _count_and_hash(conn, "t")


def test_empty_table():
    assert _duckdb_hash([]) == (0, 0)


def test_row_order_does_not_matter():
    rows = [("x", "1", True), ("y", "2", False)]
    assert _duckdb_hash(rows) == _duckdb_hash(list(reversed(rows)))


def test_one_pair_of_equal_rows_replaced_by_another_changes_the_hash():
    x, y, z = ("x", "1", True), ("y", "2", True), ("z", "3", True)
    assert _duckdb_hash([x, x, z]) != _duckdb_hash([y, y, z])


def test_null_differs_from_empty_string_and_from_the_string_null():
    assert len({_duckdb_hash([(v, "1", True)]) for v in (None, "", "NULL")}) == 3


def test_null_boolean_differs_from_false():
    assert _duckdb_hash([("x", "1", None)]) != _duckdb_hash([("x", "1", False)])


def test_field_boundaries_are_kept():
    assert _duckdb_hash([("a|b", "c", True)]) != _duckdb_hash([("a", "b|c", True)])


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


def test_temporal_values_encode_as_their_epoch_integers():
    instant = datetime.datetime(2026, 2, 28, 1, 2, 3, 4, tzinfo=datetime.timezone.utc)
    micros = (instant - datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)) // datetime.timedelta(microseconds=1)
    assert encode_value(instant) == encode_value(Epoch(b"T", micros))
    assert encode_value(datetime.date(1969, 12, 31)) == encode_value(Epoch(b"d", -1))
    assert encode_value(datetime.time(0, 0, 1, 5)) == encode_value(Epoch(b"t", 1_000_005))


def test_a_map_encodes_its_pairs_in_any_order_and_unlike_a_list_of_them():
    pairs = [([1, 2], "a"), ({"x": 1}, "b")]
    assert encode_value(MapPairs(pairs)) == encode_value(MapPairs(reversed(pairs)))
    assert encode_value(MapPairs(pairs)) != encode_value(pairs)


def test_arrow_temporal_columns_of_every_unit_hash_as_their_stored_integers():
    batch = pa.RecordBatch.from_pydict({
        "s": pa.array([2], pa.timestamp("s")),
        "ms": pa.array([5], pa.timestamp("ms")),
        "us": pa.array([7], pa.timestamp("us", tz="UTC")),
        "ns": pa.array([1_000_000_000_000_000_005], pa.timestamp("ns", tz="UTC")),
        "day32": pa.array([-1], pa.date32()),
        "day64": pa.array([3 * 86_400_000], pa.date64()),
        "time_s": pa.array([61], pa.time32("s")),
        "time_ms": pa.array([61_005], pa.time32("ms")),
        "time_us": pa.array([9], pa.time64("us")),
        "time_ns": pa.array([3_600_000_000_005], pa.time64("ns")),
    })
    assert hash_batches([batch]) == (1, row_hash([
        Epoch(b"T", 2_000_000), Epoch(b"T", 5_000), Epoch(b"T", 7), Epoch(b"P", 1_000_000_000_000_000_005),
        Epoch(b"d", -1), Epoch(b"d", 3), Epoch(b"t", 61_000_000), Epoch(b"t", 61_005_000), Epoch(b"t", 9),
        Epoch(b"p", 3_600_000_000_005),
    ]))


def test_unsupported_type_raises():
    with pytest.raises(TypeError):
        encode_value(object())


# One row of every type an Iceberg table gives DuckDB, as DuckDB SQL and as the Python values the
# shared encoder expects. DuckDB's Arrow export and the Python reference must hash alike.
_TYPED_COLUMNS = [
    ("b BOOLEAN", "true", True),
    ("i INTEGER", "-7", -7),
    ("l BIGINT", "9007199254740993", 9007199254740993),
    ("f FLOAT", "CAST('NaN' AS FLOAT)", math.nan),
    # A literal -0.0 is folded to 0.0, so the value goes in through a cast.
    ("d DOUBLE", "CAST('-0.0' AS DOUBLE)", -0.0),
    ("dec DECIMAL(10, 2)", "12.50", decimal.Decimal("12.50")),
    ("tiny DECIMAL(38, 10)", "-0.0000000001", decimal.Decimal("-1E-10")),
    ("wide DECIMAL(38, 10)", "-9999999999999999999999999999.9999999999",
     decimal.Decimal("-9999999999999999999999999999.9999999999")),
    ("s VARCHAR", "''", ""),
    ("bin BLOB", "'\\x00\\xff'::BLOB", b"\x00\xff"),
    ("dt DATE", "DATE '1969-12-31'", datetime.date(1969, 12, 31)),
    ("tm TIME", "TIME '23:59:59.000001'", datetime.time(23, 59, 59, 1)),
    ("ts TIMESTAMP", "TIMESTAMP '2026-02-28 01:02:03.000004'", datetime.datetime(2026, 2, 28, 1, 2, 3, 4)),
    ("tz TIMESTAMPTZ", "TIMESTAMPTZ '2026-02-28 01:02:03.000004+00'",
     datetime.datetime(2026, 2, 28, 1, 2, 3, 4, tzinfo=datetime.timezone.utc)),
    ("u UUID", "'550e8400-e29b-41d4-a716-446655440000'", "550e8400-e29b-41d4-a716-446655440000"),
    ("lst INTEGER[]", "[1, NULL, 3]", [1, None, 3]),
    ("m MAP(VARCHAR, INTEGER)", "MAP {'b': 2, 'a': 1}", MapPairs([("a", 1), ("b", 2)])),
    ("st STRUCT(x INTEGER, y VARCHAR)", "{'x': 1, 'y': NULL}", {"x": 1, "y": None}),
]


def test_duckdb_matches_the_python_reference_for_every_type_an_iceberg_table_gives_it(conn):
    conn.execute(f"CREATE TABLE t ({', '.join(c for c, _, _ in _TYPED_COLUMNS)})")
    conn.execute(f"INSERT INTO t VALUES ({', '.join(sql for _, sql, _ in _TYPED_COLUMNS)})")
    expected = (1, row_hash(v for _, _, v in _TYPED_COLUMNS))
    assert _count_and_hash(conn, "t") == expected


def test_duckdb_hashes_timestamptz_at_both_ends_of_python_s_range_on_a_connection_far_from_utc(conn):
    # Pacific/Kiritimati is west of UTC in year 1 and fourteen hours east of it in 9999, so on its
    # wall clock the first instant falls in year 0 and the last in year 10000.
    conn.execute("SET TimeZone = 'Pacific/Kiritimati'")
    first, last = "TIMESTAMPTZ '0001-01-01 00:00:00+00'", "TIMESTAMPTZ '9999-12-31 23:59:59.999999+00'"
    assert _hash_of(conn, f"SELECT {first} AS a, {last} AS b, [{first}, {last}] AS l, {{'t': {last}}} AS s") == (
        1, row_hash([Epoch(b"T", FIRST_MICROS), Epoch(b"T", LAST_MICROS), [Epoch(b"T", FIRST_MICROS), Epoch(b"T", LAST_MICROS)],
                     {"t": Epoch(b"T", LAST_MICROS)}]))


def test_duckdb_hashes_timestamps_and_dates_outside_python_s_years_at_top_level_and_nested(conn):
    late, early = "TIMESTAMP '10000-01-01 00:00:00'", "TIMESTAMP '0000-01-01 00:00:00'"
    late_day, early_day = "DATE '10000-01-01'", "DATE '0000-01-01'"
    select = (f"SELECT {late} AS a, {early} AS b, {late_day} AS c, {early_day} AS d, [{late}, {early}] AS l, "
              f"{{'t': {early}, 'd': {late_day}}} AS s, MAP {{{late_day}: {late}}} AS m")
    assert _hash_of(conn, select) == (1, row_hash([
        Epoch(b"T", YEAR_10000_MICROS), Epoch(b"T", YEAR_0_MICROS), Epoch(b"d", YEAR_10000_DAYS), Epoch(b"d", YEAR_0_DAYS),
        [Epoch(b"T", YEAR_10000_MICROS), Epoch(b"T", YEAR_0_MICROS)],
        {"t": Epoch(b"T", YEAR_0_MICROS), "d": Epoch(b"d", YEAR_10000_DAYS)},
        MapPairs([(Epoch(b"d", YEAR_10000_DAYS), Epoch(b"T", YEAR_10000_MICROS))]),
    ]))


def test_duckdb_hashes_maps_keyed_by_lists_and_structs(conn):
    select = ("SELECT MAP {[1, 2]: 'a'} AS by_list, MAP {{'x': 1}: 'b'} AS by_struct, "
              "MAP {{'d': 1.50::DECIMAL(10, 2), 't': TIMESTAMP '1970-01-01 00:00:00'}: 'c'} AS by_typed_struct")
    assert _hash_of(conn, select) == (1, row_hash([
        MapPairs([([1, 2], "a")]), MapPairs([({"x": 1}, "b")]),
        MapPairs([({"d": decimal.Decimal("1.50"), "t": Epoch(b"T", 0)}, "c")]),
    ]))
