"""PyIceberg backend — validation via PyIceberg table scans.

PyIceberg plans scans differently from Spark and DuckDB. It uses each manifest's
min_sequence_number to skip delete manifests that cannot apply, so a wrong minimum in the
manifest list shows up here as a deleted row read back, often only when a partition filter
prunes other manifests. `row_filters` reads the table through such filters as well.
"""

import pyarrow as pa
from pyiceberg.table import Table

from iceberg_fuzzer.backend.arrow_reader import ArrowValidation


class PyIcebergValidation(ArrowValidation):
    """Validation engine that reads through PyIceberg's own scan and PyArrow."""

    def scan(self, table: Table, row_filter: str | None = None) -> pa.Table:
        scan = table.scan(row_filter=row_filter) if row_filter else table.scan()
        # to_arrow_batch_reader aborts on map columns with some native Arrow builds.
        return scan.to_arrow()
