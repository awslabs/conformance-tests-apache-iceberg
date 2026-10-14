"""iceberg-rust backend — validation via iceberg-rust's DataFusion table provider.

pyiceberg-core serves an Iceberg table to DataFusion through iceberg-rust, which plans the
scan, applies deletes and decodes Parquet itself, so this reader shares no read code with
PyIceberg, PyArrow, Spark or DuckDB. pyiceberg-core 0.10 needs DataFusion 52 or later, which
passes a session to the table provider.
"""

import pyarrow as pa
from datafusion import SessionContext
from pyiceberg.table import Table
from pyiceberg_core.datafusion import IcebergDataFusionTable

from iceberg_fuzzer.backend.arrow_reader import ArrowValidation


class _Provider:
    """The table as a DataFusion table provider. iceberg-rust needs the metadata location as a
    URI, while local tables carry a bare path."""

    def __init__(self, table: Table):
        location = table.metadata_location
        self._table = IcebergDataFusionTable(
            identifier=table.name(),
            metadata_location=location if "://" in location else f"file://{location}",
            file_io_properties=table.io.properties,
        )

    def __datafusion_table_provider__(self, session):
        return self._table.__datafusion_table_provider__(session)


class IcebergRustValidation(ArrowValidation):
    """Validation engine that reads through iceberg-rust and DataFusion."""

    def scan(self, table: Table, row_filter: str | None = None) -> pa.Table:
        context = SessionContext()
        context.register_table("scanned", _Provider(table))
        where = f" WHERE {row_filter}" if row_filter else ""
        return context.sql(f"SELECT * FROM scanned{where}").to_arrow_table()
