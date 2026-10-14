"""Daft backend — validation via Daft's Iceberg scan.

Daft plans the scan through PyIceberg but reads Parquet and applies deletes with its own Rust
reader, so it decodes files through a different code path than PyArrow, Polars, Spark or
DuckDB.
"""

import daft
import pyarrow as pa
from pyiceberg.table import Table

from iceberg_fuzzer.backend.arrow_reader import ArrowValidation


class DaftValidation(ArrowValidation):
    """Validation engine that reads through Daft."""

    def scan(self, table: Table, row_filter: str | None = None) -> pa.Table:
        frame = daft.read_iceberg(table)
        if row_filter:
            frame = frame.where(daft.sql_expr(row_filter))
        return frame.to_arrow()
