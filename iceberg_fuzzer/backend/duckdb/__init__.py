"""DuckDB backend — validation via iceberg_scan()."""

import os

import duckdb

from iceberg_fuzzer.backend import ValidationEngine, Checkpoint, AssertionResult
from iceberg_fuzzer.backend.duckdb.canonical import DuckDBCanonicalCast

_iceberg_installed = False


def _ensure_iceberg_extension():
    global _iceberg_installed
    if not _iceberg_installed:
        try:
            with duckdb.connect() as conn:
                conn.execute("INSTALL iceberg;")
        except Exception as e:
            raise RuntimeError(f"Failed to install DuckDB iceberg extension: {e}") from e
        _iceberg_installed = True


def _get_row_count(conn, table_path: str) -> int:
    """Return the total row count for a table via iceberg_scan()."""
    return conn.execute(
        f"SELECT COUNT(*) FROM iceberg_scan('{table_path}')"
    ).fetchone()[0]


def _get_content_hash(conn, table_path: str) -> int:
    """Return an order-independent content hash for a table.

    Uses canonical type formatting so that all query engines produce the same hash for the same data.

    Algorithm: SHA256 of pipe-delimited canonical strings per row,
    first 15 hex chars (60 bits) cast to BIGINT, XOR across all rows.
    60 bits ensures the value fits in a signed BIGINT.
    """
    schema_query = f"DESCRIBE SELECT * FROM iceberg_scan('{table_path}')"
    columns = [(row[0], row[1]) for row in conn.execute(schema_query).fetchall()]

    canonical = DuckDBCanonicalCast()
    concat_expr = " || '|' || ".join(
        canonical.cast(name, dtype) for name, dtype in columns
    )
    query = (
        f"SELECT bit_xor(CAST('0x' || substr(sha256({concat_expr}), 1, 15) AS BIGINT)) "
        f"FROM iceberg_scan('{table_path}')"
    )
    return conn.execute(query).fetchone()[0] or 0


class DuckDBValidation(ValidationEngine):
    """DuckDB-based validation engine using iceberg_scan() for table access."""

    def _connect(self):
        _ensure_iceberg_extension()
        conn = duckdb.connect()
        conn.execute("LOAD iceberg;")
        return conn

    def _resolve_path(self, warehouse: str, namespace: str, table_name: str) -> str:
        return os.path.join(warehouse, namespace, table_name).replace("'", "''")

    def capture_checkpoint(self, warehouse: str, namespace: str, table_name: str) -> Checkpoint:
        path = self._resolve_path(warehouse, namespace, table_name)
        with self._connect() as conn:
            return Checkpoint(
                row_count=_get_row_count(conn, path),
                content_hash=_get_content_hash(conn, path),
            )

    def validate(self, checkpoint: Checkpoint, warehouse: str, namespace: str, table_name: str) -> list[AssertionResult]:
        path = self._resolve_path(warehouse, namespace, table_name)
        with self._connect() as conn:
            return [
                AssertionResult.row_count(checkpoint.row_count, _get_row_count(conn, path)),
                AssertionResult.content_hash(checkpoint.content_hash, _get_content_hash(conn, path)),
            ]
