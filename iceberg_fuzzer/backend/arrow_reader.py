"""Readers that return a table's rows as Arrow.

Most Iceberg readers can hand back an Arrow table. ArrowValidation computes the row count, the
content hash and filtered reads from that Arrow output with the shared encoder, so every such
reader is judged the same way, and adding one means implementing `scan` only.
"""

from abc import abstractmethod
from collections.abc import Callable

import pyarrow as pa
from pyiceberg.table import Table

from iceberg_fuzzer.backend import (
    AssertionResult,
    Checkpoint,
    ColTypeStr,
    ColumnFilter,
    ColumnName,
    ValidationEngine,
)
from iceberg_fuzzer.backend.canonical import hash_batches
from iceberg_fuzzer.backend.point_query import format_sql_literal

LoadTable = Callable[[str, str], Table]


def _as_map(column: pa.ChunkedArray, map_type: pa.MapType) -> pa.ChunkedArray:
    """A list of key and value structs, as readers without a map type return maps, as a map."""
    chunks = []
    for chunk in column.chunks:
        entries = chunk.values
        chunks.append(pa.MapArray.from_arrays(
            chunk.offsets.cast(pa.int32()), entries.field(0), entries.field(1), type=map_type, mask=chunk.is_null(),
        ))
    return pa.chunked_array(chunks, type=map_type)


def conform_maps(rows: pa.Table, table: Table) -> pa.Table:
    """Top-level map columns that a reader returned as lists of entries, back as maps."""
    expected = table.schema().as_arrow()
    for i, field in enumerate(rows.schema):
        target = expected.field(field.name).type if field.name in expected.names else None
        if isinstance(target, pa.MapType) and not isinstance(field.type, pa.MapType):
            rows = rows.set_column(i, field.name, _as_map(rows.column(i), target))
    return rows


class ArrowValidation(ValidationEngine):
    """A validation engine for a reader that returns rows as Arrow.

    Args:
        load_table: Returns the current PyIceberg table for a namespace and table name, for
            example from a catalog or from a metadata file location.
        row_filters: Predicates such as "region = 'us'" to read the table through in addition
            to the full scan, since a filter changes which files and deletes a reader plans.
    """

    reports_snapshots = True

    def __init__(self, load_table: LoadTable, row_filters: tuple[str, ...] = ()):
        self._load_table = load_table
        self._row_filters = row_filters

    def current_snapshot(self, warehouse: str, namespace: str, table_name: str) -> tuple[int | None, str | None]:
        snapshot = self._load_table(namespace, table_name).current_snapshot()
        if snapshot is None:
            return None, None
        return snapshot.snapshot_id, snapshot.summary.operation.value if snapshot.summary else None

    @abstractmethod
    def scan(self, table: Table, row_filter: str | None = None) -> pa.Table:
        """Every row the reader returns, optionally through a predicate such as "region = 'us'"
        or "region IS NULL"."""

    def _count_and_hash(self, table: Table, row_filter: str | None = None) -> tuple[int, int]:
        return hash_batches(conform_maps(self.scan(table, row_filter), table).to_batches())

    def capture_checkpoint(self, warehouse: str, namespace: str, table_name: str) -> Checkpoint:
        table = self._load_table(namespace, table_name)
        row_count, content_hash = self._count_and_hash(table)
        filtered = {f: self._count_and_hash(table, f) for f in self._row_filters}
        return Checkpoint(row_count=row_count, content_hash=content_hash, filtered=filtered)

    def validate(self, checkpoint: Checkpoint, warehouse: str, namespace: str, table_name: str) -> list[AssertionResult]:
        current = self.capture_checkpoint(warehouse, namespace, table_name)
        assertions = [
            AssertionResult.row_count(checkpoint.row_count, current.row_count),
            AssertionResult.content_hash(checkpoint.content_hash, current.content_hash),
        ]
        for row_filter, (rows, content_hash) in checkpoint.filtered.items():
            now_rows, now_hash = current.filtered[row_filter]
            assertions.append(AssertionResult.equal(f"row_count where {row_filter}", rows, now_rows))
            assertions.append(AssertionResult.equal(f"content_hash where {row_filter}", content_hash, now_hash))
        return assertions

    def filtered_count(self, warehouse: str, namespace: str, table_name: str, column_filter: ColumnFilter) -> int:
        column = column_filter.column
        predicate = f"{column} IS NULL" if column_filter.is_null else f"{column} = {format_sql_literal(column_filter.value)}"
        return self.scan(self._load_table(namespace, table_name), predicate).num_rows

    def get_columns(self, warehouse: str, namespace: str, table_name: str) -> list[tuple[ColumnName, ColTypeStr]]:
        schema = self._load_table(namespace, table_name).schema()
        return [(f.name, str(f.field_type)) for f in schema.fields if f.name != "id"]

    def get_row_ids(self, warehouse: str, namespace: str, table_name: str) -> set[int]:
        return set(self.scan(self._load_table(namespace, table_name)).column("id").to_pylist())
