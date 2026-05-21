"""Base class for canonical type formatting used in cross-engine content hashing.

Each engine backend implements CanonicalCast to produce an engine-native
expression that formats a column value as a deterministic string. All
implementations must produce identical output for the same input value
so that content hashes agree across engines.

Iceberg type to canonical string format:

    boolean         'true' | 'false' (lowercase)
    int, long       Decimal integer (e.g., '42', '-7')
    float, double   Default string cast (e.g., '3.0', '1.5', '0.5')
    decimal(p,s)    Fixed-scale string (e.g., '42.50')
    string          Raw value, no quoting
    date            'YYYY-MM-DD'
    time            'HH:MM:SS.ffffff' (microsecond precision, zero-padded)
    timestamp       'YYYY-MM-DD HH:MM:SS.ffffff'
    timestamptz     'YYYY-MM-DD HH:MM:SS.ffffff' (converted to UTC, no tz suffix)
    binary, fixed   Lowercase hex (e.g., '68656c6c6f')
    uuid            Lowercase hyphenated (e.g., '550e8400-e29b-41d4-a716-446655440000')
    list            JSON array (e.g., '[1,2,3]')
    map             JSON object (e.g., '{"1":"hello","2":"world"}')
    struct          JSON object (e.g., '{"name":"alice","age":30}')
    NULL            Empty string ''

Row serialization: canonical(col1) || '|' || canonical(col2) || ...
"""

from abc import ABC, abstractmethod
from typing import Any


class CanonicalCast(ABC):
    """Produces an engine-native expression for canonical string formatting of a column.

    Subclasses return whatever type their engine uses to represent expressions
    (e.g., SQL string for DuckDB, Column object for PySpark).
    """

    @abstractmethod
    def cast(self, col_name: str, col_type: Any) -> Any:
        """Return an expression that formats col_name as a canonical string.

        Args:
            col_name: Column name.
            col_type: Engine-specific type descriptor (e.g., DuckDB type string,
                      PySpark DataType instance).

        Returns:
            Engine-native expression. The expression must wrap NULLs as empty string.
        """
        pass
