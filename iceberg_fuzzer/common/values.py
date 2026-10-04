"""The values generated for each primitive type, defined once.

Cross-engine validation only means anything while every backend writes the same
value for a row and the validator expects that same value. Each of them used to
carry its own copy of these formulas, so a backend and the validator could drift
apart silently and the suite would report the disagreement as a defect in the
table rather than in itself. That is how the type and row-id defects fixed in #92
and #93 arose.

Values are derived from the row ID alone, so a reader can predict any row's value
from the ID it reads back without knowing which batch wrote it.
"""

import uuid as uuid_lib
from datetime import date, time
from decimal import Decimal

from iceberg_fuzzer.common.timestamps import timestamp_value, timestamptz_value

# Types `primitive_value` covers by name. `decimal(P,S)` and `fixed(L)` are
# covered too, by prefix, since they carry a parameter. A caller handling a type
# outside all of that is responsible for its own value, which is the case for
# the time and fixed(L) columns the PySpark backend keys off a column name.
SUPPORTED_TYPES = frozenset({
    'boolean', 'int', 'long', 'float', 'double', 'string', 'binary',
    'date', 'timestamp', 'timestamptz', 'uuid', 'time',
})


def primitive_value(iceberg_type, row_id):
    """The value a row of `iceberg_type` holds, by row ID.

    Raises KeyError for a type with no formula, rather than returning a
    placeholder: a silent fallback is what let missing types be written as
    `value_0` strings and still validate.
    """
    if iceberg_type == 'boolean':
        return (row_id % 2) == 0
    if iceberg_type == 'int':
        return row_id
    if iceberg_type == 'long':
        return row_id * 1000
    if iceberg_type == 'float':
        return float(row_id) + 0.5
    if iceberg_type == 'double':
        return float(row_id) * 1.5
    if iceberg_type == 'string':
        return f"value_{row_id}"
    if iceberg_type == 'binary' or iceberg_type.startswith('binary_'):
        return f"value_{row_id}".encode()
    if iceberg_type == 'date':
        # Capped at the 28th so every month holds the day and no year is a
        # special case.
        return date(2023, 1, min(28, (row_id % 28) + 1))
    if iceberg_type == 'timestamp':
        return timestamp_value(row_id)
    if iceberg_type == 'timestamptz':
        # common/timestamps.py owns both timestamp types: the zone it varies by
        # row is the whole reason they are not a one-line formula here.
        return timestamptz_value(row_id)
    if iceberg_type == 'uuid':
        return uuid_lib.UUID(int=row_id).bytes
    if iceberg_type == 'time':
        return time(hour=(row_id % 24), minute=(row_id % 60))
    if iceberg_type.startswith('decimal'):
        return Decimal(f"{row_id}.{row_id % 100:02d}")
    if iceberg_type.startswith('fixed('):
        # Padded to exactly L bytes, because Iceberg's fixed(L) and Arrow's
        # fixed_size_binary(L) both reject anything else.
        length = int(iceberg_type[len('fixed('):-1])
        return f"fixed_{row_id}".ljust(length, '0')[:length].encode()
    raise KeyError(f"No value formula for Iceberg type {iceberg_type!r}")
