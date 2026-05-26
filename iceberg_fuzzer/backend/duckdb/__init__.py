"""DuckDB backend — validation engine."""

import os

import duckdb

from iceberg_fuzzer.backend import ValidationEngine, Checkpoint, AssertionResult, is_s3_tables_warehouse
from iceberg_fuzzer.backend.duckdb.canonical import DuckDBCanonicalCast


def _extract_region(warehouse_path: str) -> str:
    """Extract region from ARN: arn:aws:s3tables:region:account:bucket/name"""
    return warehouse_path.split(":")[3]

_iceberg_installed = False


def _ensure_iceberg_extension():
    global _iceberg_installed
    if not _iceberg_installed:
        try:
            with duckdb.connect() as conn:
                conn.execute("INSTALL iceberg;")
        except Exception as e:
            raise RuntimeError(
                f"Failed to install DuckDB iceberg extension: {e}"
            ) from e
        _iceberg_installed = True


def _get_row_count(conn, source: str) -> int:
    """Return the total row count for a table."""
    return conn.execute(f"SELECT COUNT(*) FROM {source}").fetchone()[0]


def _get_content_hash(conn, source: str) -> int:
    """Return an order-independent content hash for a table.

    Uses canonical type formatting so that all query engines produce the same hash for the same data.

    Algorithm: SHA256 of pipe-delimited canonical strings per row,
    first 15 hex chars (60 bits) cast to BIGINT, XOR across all rows.
    60 bits ensures the value fits in a signed BIGINT.
    """
    schema_query = f"DESCRIBE SELECT * FROM {source}"
    columns = [(row[0], row[1]) for row in conn.execute(schema_query).fetchall()]

    canonical = DuckDBCanonicalCast()
    concat_expr = " || '|' || ".join(
        canonical.cast(name, dtype) for name, dtype in columns
    )
    query = (
        f"SELECT bit_xor(CAST('0x' || substr(sha256({concat_expr}), 1, 15) AS BIGINT)) "
        f"FROM {source}"
    )
    return conn.execute(query).fetchone()[0] or 0


def _escape_identifier(name: str) -> str:
    """Double-quote a SQL identifier, escaping any embedded double quotes."""
    return '"' + name.replace('"', '""') + '"'


class DuckDBValidation(ValidationEngine):
    """DuckDB-based validation engine for local and S3 Tables warehouses."""

    def __init__(self, warehouse_path: str):
        self._warehouse_path = warehouse_path
        self._is_s3 = is_s3_tables_warehouse(warehouse_path)
        self._aws_region = _extract_region(warehouse_path) if self._is_s3 else None
        self._conn = None

    def _get_conn(self):
        if self._conn is not None:
            return self._conn

        _ensure_iceberg_extension()
        conn = duckdb.connect()
        conn.execute("LOAD iceberg;")

        if self._is_s3:
            region = self._aws_region
            conn.execute("INSTALL httpfs; LOAD httpfs;")
            conn.execute(f"CREATE OR REPLACE SECRET secret (TYPE s3, PROVIDER credential_chain, REGION '{region}');")

            conn.execute(f"""
                ATTACH '{self._warehouse_path}' AS s3tables (
                    TYPE ICEBERG,
                    ENDPOINT 's3tables.{region}.amazonaws.com/iceberg',
                    AUTHORIZATION_TYPE 'sigv4',
                    SIGV4_REGION '{region}',
                    SIGV4_SERVICE 's3tables'
                );
            """)

        self._conn = conn
        return conn

    def _table_source(self, warehouse: str, namespace: str, table_name: str) -> str:
        """Return the SQL FROM source for a table."""
        if self._is_s3:
            return f"s3tables.{_escape_identifier(namespace)}.{_escape_identifier(table_name)}"
        # Escape single quotes for SQL string interpolation in iceberg_scan()
        path = os.path.join(warehouse, namespace, table_name).replace("'", "''")
        return f"iceberg_scan('{path}')"

    def capture_checkpoint(
        self, warehouse: str, namespace: str, table_name: str
    ) -> Checkpoint:
        source = self._table_source(warehouse, namespace, table_name)
        conn = self._get_conn()
        return Checkpoint(
            row_count=_get_row_count(conn, source),
            content_hash=_get_content_hash(conn, source),
        )

    def validate(
        self, checkpoint: Checkpoint, warehouse: str, namespace: str, table_name: str
    ) -> list[AssertionResult]:
        source = self._table_source(warehouse, namespace, table_name)
        conn = self._get_conn()
        return [
            AssertionResult.row_count(checkpoint.row_count, _get_row_count(conn, source)),
            AssertionResult.content_hash(checkpoint.content_hash, _get_content_hash(conn, source)),
        ]

    def close(self):
        if self._conn is not None:
            self._conn.close()
            self._conn = None
