"""Polars backend — validation via Polars' native Iceberg scan.

Polars plans the scan through PyIceberg but decodes Parquet and applies deletes with its own
reader, so it reads files through a different code path than PyArrow, Spark or DuckDB.
"""

import polars as pl
import pyarrow as pa
from pyiceberg.table import Table

from iceberg_fuzzer.backend.arrow_reader import ArrowValidation


class PolarsValidation(ArrowValidation):
    """Validation engine that reads through Polars."""

    def scan(self, table: Table, row_filter: str | None = None) -> pa.Table:
        frame = pl.scan_iceberg(table, reader_override="native")
        if row_filter:
            frame = frame.filter(pl.sql_expr(row_filter))
        return frame.collect().to_arrow()
