"""The content hash every validation engine computes, implemented once.

Engines pass their rows to `hash_rows`, or Arrow record batches to `hash_batches`, so the
encoding lives in one place instead of once per engine's SQL dialect.

Each value is encoded with a type tag and its length. NULL, an empty string and the string
'NULL' all differ, as do -0.0 and 0.0, and ('a|b', 'c') differs from ('a', 'b|c'). Every NaN
encodes the same. A timestamp encodes as its microseconds since the epoch, or its nanoseconds
where its type keeps them, counted for one without a zone from its wall clock read as UTC, a
date as its days since the epoch and a time as its microseconds or nanoseconds since midnight.
Engines hand these over as the integers they store, in an `Epoch`, so a value outside the years
Python's datetime holds hashes too; Python's datetime, date and time values encode the same. A
map encodes as its key and value pairs in the order of their encodings, carried as `MapPairs`,
so its keys need not be hashable, and a struct as its field names and values the same way. A
decimal keeps the scale of its type.

A row hashes to the first 64 bits of the SHA-256 of its encoded values in column order, and a
table hashes to the sum of its row hashes modulo 2**64. A sum does not depend on row order, and
unlike XOR it changes when one pair of equal rows is replaced by another at the same row count.
"""

import datetime
import decimal
import hashlib
import math
import struct
import uuid
from collections.abc import Iterable
from dataclasses import dataclass

HASH_MODULUS = 1 << 64

_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
_EPOCH_DATE = datetime.date(1970, 1, 1)
# Microseconds in each unit an Arrow timestamp or time may carry, nanoseconds kept apart.
_MICROS = {"s": 1_000_000, "ms": 1_000, "us": 1}
_DAY_MILLIS = 86_400_000


@dataclass(frozen=True)
class Epoch:
    """A temporal value as the integer an engine stores it as, under the tag of its kind: T for a
    timestamp's microseconds since the epoch, P for its nanoseconds, d for a date's days since the
    epoch, t for a time's microseconds since midnight and p for its nanoseconds."""

    tag: bytes
    value: int


class MapPairs(tuple):
    """A map's key and value pairs."""


def _frame(tag: bytes, payload: bytes) -> bytes:
    return tag + struct.pack(">I", len(payload)) + payload


def _entries(items) -> bytes:
    return b"".join(sorted(encode_value(k) + encode_value(v) for k, v in items))


def encode_value(value) -> bytes:
    if value is None:
        return _frame(b"N", b"")
    if isinstance(value, bool):
        return _frame(b"B", b"\x01" if value else b"\x00")
    if isinstance(value, Epoch):
        return _frame(value.tag, str(value.value).encode())
    if isinstance(value, int):
        return _frame(b"I", str(value).encode())
    if isinstance(value, float):
        return _frame(b"F", b"nan" if math.isnan(value) else struct.pack(">d", value))
    if isinstance(value, decimal.Decimal):
        sign, digits, exponent = value.as_tuple()
        unscaled = int("".join(map(str, digits)) or "0") * (-1 if sign else 1)
        return _frame(b"D", f"{unscaled}e{exponent}".encode())
    if isinstance(value, uuid.UUID):
        # Spark and DuckDB hand a uuid over as its string form, so that is the canonical one.
        value = str(value)
    if isinstance(value, str):
        return _frame(b"S", value.encode())
    if isinstance(value, (bytes, bytearray, memoryview)):
        return _frame(b"Y", bytes(value))
    if isinstance(value, datetime.datetime):
        aware = value if value.tzinfo else value.replace(tzinfo=datetime.timezone.utc)
        return _frame(b"T", str((aware - _EPOCH) // datetime.timedelta(microseconds=1)).encode())
    if isinstance(value, datetime.date):
        return _frame(b"d", str((value - _EPOCH_DATE).days).encode())
    if isinstance(value, datetime.time):
        micros = ((value.hour * 60 + value.minute) * 60 + value.second) * 1_000_000 + value.microsecond
        return _frame(b"t", str(micros).encode())
    if isinstance(value, MapPairs):
        return _frame(b"M", _entries(value))
    if hasattr(value, "asDict"):
        # A PySpark Row is a tuple, so it has to be caught before the list case below.
        return encode_value(value.asDict(recursive=False))
    if isinstance(value, dict):
        # A struct arrives as a dict keyed by field name.
        return _frame(b"M", _entries(value.items()))
    if isinstance(value, (list, tuple)):
        return _frame(b"L", b"".join(encode_value(item) for item in value))
    raise TypeError(f"No canonical encoding for {type(value).__name__}")


def row_hash(values: Iterable) -> int:
    digest = hashlib.sha256(b"".join(encode_value(v) for v in values)).digest()
    return int.from_bytes(digest[:8], "big")


def _stored(arrow_type):
    """arrow_type with each timestamp, date and time in it, at any depth, as the integer it is
    stored as. A view as this type reads the stored integers without a cast, and PyArrow 23
    aborts the process when it casts the key type of a map DuckDB hands over."""
    import pyarrow as pa

    if pa.types.is_timestamp(arrow_type) or pa.types.is_date64(arrow_type) or pa.types.is_time64(arrow_type):
        return pa.int64()
    if pa.types.is_date32(arrow_type) or pa.types.is_time32(arrow_type):
        return pa.int32()
    if pa.types.is_map(arrow_type):
        return pa.map_(_stored_field(arrow_type.key_field), _stored_field(arrow_type.item_field), arrow_type.keys_sorted)
    if pa.types.is_struct(arrow_type):
        return pa.struct([_stored_field(f) for f in arrow_type])
    if pa.types.is_large_list(arrow_type):
        return pa.large_list(_stored_field(arrow_type.value_field))
    if pa.types.is_list(arrow_type):
        return pa.list_(_stored_field(arrow_type.value_field))
    return arrow_type


def _stored_field(field):
    return field.with_type(_stored(field.type))


def _converter(arrow_type):
    """A function that puts a value of arrow_type, read as its stored integers, see _stored, in the
    form encode_value takes, or None if it needs none: timestamps, dates and times tagged, and maps
    as their pairs, at any depth."""
    import pyarrow as pa

    if pa.types.is_timestamp(arrow_type):
        if arrow_type.unit == "ns":
            return lambda nanos: Epoch(b"P", nanos)
        scale = _MICROS[arrow_type.unit]
        return lambda value: Epoch(b"T", value * scale)
    if pa.types.is_time32(arrow_type) or pa.types.is_time64(arrow_type):
        if arrow_type.unit == "ns":
            return lambda nanos: Epoch(b"p", nanos)
        scale = _MICROS[arrow_type.unit]
        return lambda value: Epoch(b"t", value * scale)
    if pa.types.is_date32(arrow_type):
        return lambda days: Epoch(b"d", days)
    if pa.types.is_date64(arrow_type):
        return lambda millis: Epoch(b"d", millis // _DAY_MILLIS)
    if pa.types.is_map(arrow_type):
        key, item = _converter(arrow_type.key_type), _converter(arrow_type.item_type)
        return lambda pairs: MapPairs((_apply(key, k), _apply(item, v)) for k, v in pairs)
    if pa.types.is_struct(arrow_type):
        fields = [(f.name, _converter(f.type)) for f in arrow_type]
        if not any(convert for _, convert in fields):
            return None
        return lambda row: {name: _apply(convert, row[name]) for name, convert in fields}
    if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        element = _converter(arrow_type.value_type)
        return element and (lambda values: [_apply(element, v) for v in values])
    return None


def _apply(convert, value):
    return value if convert is None or value is None else convert(value)


def hash_rows(rows: Iterable) -> tuple[int, int]:
    """Row count and content hash of rows, each a sequence of values in column order."""
    count = 0
    total = 0
    for row in rows:
        count += 1
        total = (total + row_hash(row)) % HASH_MODULUS
    return count, total


def hash_batches(batches: Iterable) -> tuple[int, int]:
    """Row count and content hash of Arrow record batches, columns in schema order."""
    def values(column):
        stored, convert = _stored(column.type), _converter(column.type)
        listed = (column.view(stored) if stored != column.type else column).to_pylist()
        return listed if convert is None else [_apply(convert, v) for v in listed]

    def rows():
        for batch in batches:
            columns = [values(column) for column in batch.columns]
            if not columns:
                yield from ((),) * batch.num_rows
            else:
                yield from zip(*columns)

    return hash_rows(rows())
