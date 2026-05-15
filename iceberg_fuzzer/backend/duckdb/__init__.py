"""DuckDB backend — validation via iceberg_scan()."""

import os

import duckdb

from iceberg_fuzzer.backend import ValidationEngine, Checkpoint, AssertionResult

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

    Computes SHA256 of each row's concatenated column values, then XORs
    the 60 least-significant bits across all rows for a commutative aggregate.
    We use the 60 least-significant bits (or 15 hexadecimal digits) as this avoids
    hitting issues when casting from unsigned to signed integers.

    Known limitations:
    - VARCHAR casting is engine-specific. Cross-engine validation will require
      a canonical format per type (e.g., fixed timestamp format, controlled
      float precision). For now, this is only used within a single engine
      (DuckDB before and after), so consistency is guaranteed.
    - The '|' separator can collide if column values contain '|'. Acceptable
      for compaction testing where values are generated deterministically.
    - 60-bit XOR space has birthday-problem collisions around ~2^30 rows.
      Sufficient for test tables (typically <1M rows). The row count assertion
      guards against row duplication/loss; the hash guards against value corruption.
    """
    schema_query = f"DESCRIBE SELECT * FROM iceberg_scan('{table_path}')"
    columns = [row[0] for row in conn.execute(schema_query).fetchall()]

    concat_expr = " || '|' || ".join(
        f"COALESCE(CAST(\"{col}\" AS VARCHAR), '')" for col in columns
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
