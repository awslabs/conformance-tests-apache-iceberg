"""Shared row data generation utilities for PySpark backends.

Used by both table generation (builders.py) and mutation (mutation.py)
to produce deterministic row values from a row ID.
"""

from datetime import date
from decimal import Decimal

from iceberg_fuzzer.common.timestamps import timestamp_value, timestamptz_value


def generate_value(field_name: str, spark_type_str: str, row_id: int):
    """Generate a deterministic value for a field given its name, type, and row ID.

    Handles common Iceberg primitive types. Returns None for unrecognized types.

    Args:
        field_name: Column name.
        spark_type_str: Spark type as a string (e.g., 'int', 'string', 'bigint').
        row_id: The row's unique ID, used to derive the value.
    """
    if field_name == "id":
        return row_id

    # Normalize Spark type strings
    t = spark_type_str.lower()

    if t in ("boolean", "bool"):
        return (row_id % 2) == 0
    elif t in ("int", "integer"):
        return row_id
    elif t in ("long", "bigint"):
        return row_id * 1000
    elif t == "float":
        return float(row_id) + 0.5
    elif t == "double":
        return float(row_id) * 1.5
    elif t == "string":
        return f"value_{row_id}"
    elif t == "binary":
        return f"value_{row_id}".encode()
    elif t == "date":
        return date(2023, 1, 1).replace(day=min(28, (row_id % 28) + 1))
    elif t == "timestamp":
        return timestamptz_value(row_id)
    elif t == "timestamp_ntz":
        return timestamp_value(row_id)
    elif t.startswith("decimal"):
        return Decimal(f"{row_id}.{row_id % 100:02d}")
    else:
        return None
