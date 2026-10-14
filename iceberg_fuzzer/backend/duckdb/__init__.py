"""DuckDB backend — validation engine."""

import os

import duckdb

from iceberg_fuzzer.backend import (
    ValidationEngine,
    Checkpoint,
    AssertionResult,
    ColumnFilter,
    ColumnName,
    ColTypeStr,
    is_s3_tables_warehouse,
)
from iceberg_fuzzer.backend.canonical import hash_batches
from iceberg_fuzzer.backend.point_query import format_sql_literal


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


def _count_and_hash(conn, source: str) -> tuple[int, int]:
    """Row count and content hash of a table, using the shared encoding in canonical.py."""
    return hash_batches(conn.execute(f"SELECT * FROM {source}").fetch_record_batch())


def _escape_identifier(name: str) -> str:
    """Double-quote a SQL identifier, escaping any embedded double quotes."""
    return '"' + name.replace('"', '""') + '"'


class DuckDBValidation(ValidationEngine):
    """DuckDB-based validation engine for local and S3 Tables warehouses.

    extension_directory points DuckDB at extensions installed ahead of time, for hosts that
    cannot download them. Extensions built outside DuckDB's release process are unsigned, so
    loading them also allows unsigned extensions. endpoint overrides the S3 Tables Iceberg REST
    endpoint, for example for a non-production stage.
    """

    def __init__(self, warehouse_path: str, extension_directory: str | None = None, endpoint: str | None = None):
        self._warehouse_path = warehouse_path
        self._is_s3 = is_s3_tables_warehouse(warehouse_path)
        self._aws_region = _extract_region(warehouse_path) if self._is_s3 else None
        self._extension_directory = extension_directory
        self._endpoint = endpoint
        self._conn = None

    def _connect(self):
        if self._extension_directory is None:
            _ensure_iceberg_extension()
            return duckdb.connect()
        return duckdb.connect(config={
            "extension_directory": self._extension_directory,
            "allow_unsigned_extensions": "true",
            "autoinstall_known_extensions": "false",
        })

    def _load(self, conn, name: str):
        if self._extension_directory is None:
            conn.execute(f"INSTALL {name}; LOAD {name};")
        else:
            conn.execute(f"LOAD {name};")

    def _get_conn(self):
        if self._conn is not None:
            return self._conn

        conn = self._connect()
        conn.execute("LOAD iceberg;")

        if self._is_s3:
            region = self._aws_region
            self._load(conn, "httpfs")
            if self._extension_directory is not None:
                # credential_chain comes from the aws extension, which autoinstall would fetch.
                self._load(conn, "aws")
            conn.execute(f"CREATE OR REPLACE SECRET secret (TYPE s3, PROVIDER credential_chain, REGION '{region}');")

            endpoint = (self._endpoint or f"https://s3tables.{region}.amazonaws.com").removeprefix("https://").rstrip("/")
            conn.execute(f"""
                ATTACH '{self._warehouse_path}' AS s3tables (
                    TYPE ICEBERG,
                    ENDPOINT '{endpoint}/iceberg',
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
        row_count, content_hash = _count_and_hash(conn, source)
        return Checkpoint(row_count=row_count, content_hash=content_hash)

    def validate(
        self, checkpoint: Checkpoint, warehouse: str, namespace: str, table_name: str
    ) -> list[AssertionResult]:
        source = self._table_source(warehouse, namespace, table_name)
        row_count, content_hash = _count_and_hash(self._get_conn(), source)
        return [
            AssertionResult.row_count(checkpoint.row_count, row_count),
            AssertionResult.content_hash(checkpoint.content_hash, content_hash),
        ]

    def filtered_count(
        self,
        warehouse: str,
        namespace: str,
        table_name: str,
        column_filter: ColumnFilter,
    ) -> int:
        source = self._table_source(warehouse, namespace, table_name)
        conn = self._get_conn()
        col = _escape_identifier(column_filter.column)
        if column_filter.is_null:
            where = f"{col} IS NULL"
        else:
            where = f"{col} = {format_sql_literal(column_filter.value)}"
        query = f"SELECT COUNT(*) FROM {source} WHERE {where}"
        return conn.execute(query).fetchone()[0]

    def get_columns(
        self, warehouse: str, namespace: str, table_name: str
    ) -> list[tuple[ColumnName, ColTypeStr]]:
        source = self._table_source(warehouse, namespace, table_name)
        conn = self._get_conn()
        query = f"DESCRIBE SELECT * FROM {source}"
        rows = conn.execute(query).fetchall()
        return [
            (col_name, col_type) for col_name, col_type, *_ in rows if col_name != "id"
        ]

    def get_row_ids(self, warehouse: str, namespace: str, table_name: str) -> set[int]:
        source = self._table_source(warehouse, namespace, table_name)
        conn = self._get_conn()
        rows = conn.execute(f"SELECT id FROM {source}").fetchall()
        return {row[0] for row in rows}

    def close(self):
        if self._conn is not None:
            self._conn.close()
            self._conn = None
