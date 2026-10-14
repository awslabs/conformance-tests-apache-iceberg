"""The content hash every validation engine computes, implemented once.

Engines pass their rows to `hash_rows`, or Arrow record batches to `hash_batches`, so the
encoding lives in one place instead of once per engine's SQL dialect.

Each value is encoded with a type tag and its length. NULL, an empty string and the string
'NULL' all differ, as do -0.0 and 0.0, and ('ab', 'c') differs from ('a', 'bc'). Every NaN
encodes the same. Values are normalized first, because engines hand over the same Iceberg
value as different Arrow types: a timestamptz arrives as an aware datetime and a timestamp as
a naive one, which is read as UTC; a map arrives as key-value pairs; a decimal keeps the scale
of its type.

A row hashes to the first 64 bits of the SHA-256 of its encoded values in column order, and a
table hashes to the sum of its row hashes modulo 2**64. A sum, unlike XOR, changes when a
duplicated row pair is replaced by another pair, and it does not depend on row order.
"""

import datetime
import decimal
import hashlib
import math
import struct
import uuid
from collections.abc import Iterable

HASH_MODULUS = 1 << 64

_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
_EPOCH_DATE = datetime.date(1970, 1, 1)


def _frame(tag: bytes, payload: bytes) -> bytes:
    return tag + struct.pack(">I", len(payload)) + payload


def encode_value(value) -> bytes:
    if value is None:
        return _frame(b"N", b"")
    if isinstance(value, bool):
        return _frame(b"B", b"\x01" if value else b"\x00")
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
    if hasattr(value, "asDict"):
        # A PySpark Row is a tuple, so it has to be caught before the list case below.
        return encode_value(value.asDict(recursive=False))
    if isinstance(value, dict):
        # A struct arrives as a dict keyed by field name, and a map as key-value pairs that
        # _normalize turns into a dict. Both encode as sorted key-value pairs.
        return _frame(b"M", b"".join(sorted(encode_value(k) + encode_value(v) for k, v in value.items())))
    if isinstance(value, (list, tuple)):
        return _frame(b"L", b"".join(encode_value(item) for item in value))
    raise TypeError(f"No canonical encoding for {type(value).__name__}")


def row_hash(values: Iterable) -> int:
    digest = hashlib.sha256(b"".join(encode_value(v) for v in values)).digest()
    return int.from_bytes(digest[:8], "big")


def _normalize(value, arrow_type):
    import pyarrow as pa

    if value is None:
        return None
    if pa.types.is_map(arrow_type):
        return {_normalize(k, arrow_type.key_type): _normalize(v, arrow_type.item_type) for k, v in value}
    if pa.types.is_struct(arrow_type):
        return {f.name: _normalize(value[f.name], f.type) for f in arrow_type}
    if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        return [_normalize(v, arrow_type.value_type) for v in value]
    return value


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
    def rows():
        for batch in batches:
            columns = [
                [_normalize(v, field.type) for v in batch.column(i).to_pylist()]
                for i, field in enumerate(batch.schema)
            ]
            if not columns:
                yield from ((),) * batch.num_rows
            else:
                yield from zip(*columns)

    return hash_rows(rows())
