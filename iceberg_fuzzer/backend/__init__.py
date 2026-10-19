"""
Backend abstractions for iceberg-fuzzer.

Each backend (duckdb, pyspark, polars, athena) lives in its own subpackage
and implements one or more of: generation, validation, mutation.
"""

import random
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

ColumnName = str
# TODO: Once get_columns uses PyIceberg, consider using PyIceberg's type classes
# (e.g. IntegerType, TimestamptzType) directly instead of strings. str() on those
# already gives canonical names like "int", "timestamptz".
ColTypeStr = str


@dataclass
class ColumnFilter:
    """A single-column equality or IS NULL filter for point queries."""

    column: str
    value: Any = None
    is_null: bool = False

    @staticmethod
    def eq(column: str, value: Any) -> "ColumnFilter":
        return ColumnFilter(column=column, value=value)

    @staticmethod
    def null(column: str) -> "ColumnFilter":
        return ColumnFilter(column=column, is_null=True)

    def __repr__(self) -> str:
        if self.is_null:
            return f"{self.column} IS NULL"
        return f"{self.column} = {self.value}"


@dataclass
class Checkpoint:
    """Captured table state before an operation, used to verify integrity after.

    Attributes:
        row_count: Total number of rows in the table.
        content_hash: Order-independent hash of all row data, see backend/canonical.py.
        filtered: Row count and content hash per filter, for engines that also read the table
            through row filters.
    """
    row_count: int
    content_hash: int
    filtered: dict[str, tuple[int, int]] = field(default_factory=dict)


@dataclass
class AssertionResult:
    """Result of a single assertion check within a workflow step.

    Attributes:
        name: Identifier for the assertion, such as 'row_count', 'pyspark.content_hash' or
            'column_statistics.lower_bound'. Built from the fields below when they are given.
        passed: Whether the assertion passed. An assertion that was not applicable passes.
        expected: The expected value.
        actual: The actual value observed.
        check: The check that made the assertion, such as 'row_count' or 'column_statistics'.
        problem: The kind of problem within the check, such as 'lower_bound', when a check
            reports each kind as its own assertion.
        reader: The reader whose rows the assertion is about, such as 'pyspark'.
        row_filter: The predicate the reader read the table through.
        applicable: False when there was nothing to check, such as a codec check after a step
            that wrote no files, so a pass is not counted as tested.
    """
    name: str
    passed: bool
    expected: Any = None
    actual: Any = None
    check: str | None = None
    problem: str | None = None
    reader: str | None = None
    row_filter: str | None = None
    applicable: bool = True

    def __post_init__(self):
        # An assertion built from its name alone, as validation engines outside this package may
        # build them, gets its check and problem from the name.
        if self.check is None:
            check, _, problem = self.name.split(" where ", 1)[0].partition(".")
            self.check, self.problem = check, self.problem or problem or None

    @classmethod
    def of(cls, check: str, passed: bool, expected=None, actual=None, problem: str | None = None,
           reader: str | None = None, row_filter: str | None = None, applicable: bool = True):
        """An assertion named from its reader, check, problem and row filter, as in
        'pyspark.row_count where id < 5'."""
        name = ".".join(part for part in (reader, check, problem) if part)
        if row_filter:
            name += f" where {row_filter}"
        return cls(name=name, passed=passed, expected=expected, actual=actual, check=check, problem=problem,
                   reader=reader, row_filter=row_filter, applicable=applicable)

    @classmethod
    def equal(cls, name: str, expected, actual, prefix: str | None = None, row_filter: str | None = None):
        """Assert two values are equal. prefix is the reader the values come from."""
        check, _, problem = name.partition(".")
        return cls.of(check, expected == actual, expected, actual, problem=problem or None, reader=prefix, row_filter=row_filter)

    @classmethod
    def row_count(cls, expected, actual, prefix: str | None = None, row_filter: str | None = None):
        """Assert row count matches."""
        return cls.equal("row_count", expected, actual, prefix=prefix, row_filter=row_filter)

    @classmethod
    def content_hash(cls, expected, actual, prefix: str | None = None, row_filter: str | None = None):
        """Assert content hash matches."""
        return cls.equal("content_hash", expected, actual, prefix=prefix, row_filter=row_filter)

    @classmethod
    def skipped(cls, name: str, reason: str):
        """Record that a check had nothing to check. It passes, and is not counted as tested."""
        check, _, problem = name.partition(".")
        return cls.of(check, True, expected="skipped", actual=reason, problem=problem or None, applicable=False)

    def for_reader(self, reader: str) -> "AssertionResult":
        """The same assertion, attributed to a reader."""
        return AssertionResult.of(self.check or self.name, self.passed, self.expected, self.actual, problem=self.problem,
                                  reader=reader, row_filter=self.row_filter, applicable=self.applicable)


class ValidationEngine(ABC):
    """Captures table state and validates it is preserved after an operation.

    Implementations provide engine-specific query execution (DuckDB, PySpark, Athena)
    while the workflow engine handles orchestration.
    """

    @abstractmethod
    def capture_checkpoint(self, warehouse: str, namespace: str, table_name: str) -> Checkpoint:
        """Capture the current row count and content hash for a table.

        Args:
            warehouse: Warehouse path or ARN.
            namespace: The table's namespace.
            table_name: The table name.

        Returns:
            A Checkpoint with the table's current state.
        """
        pass

    @abstractmethod
    def validate(self, checkpoint: Checkpoint, warehouse: str, namespace: str, table_name: str) -> list[AssertionResult]:
        """Validate that the table's current state matches a previously captured checkpoint.

        Args:
            checkpoint: The expected state captured before the operation.
            warehouse: Warehouse path or ARN.
            namespace: The table's namespace.
            table_name: The table name.

        Returns:
            A list of AssertionResults (one per check, e.g., row_count, content_hash).
        """
        pass

    @abstractmethod
    def filtered_count(
        self,
        warehouse: str,
        namespace: str,
        table_name: str,
        column_filter: ColumnFilter,
    ) -> int:
        """Return the number of rows matching a single-column filter."""
        pass

    @abstractmethod
    def get_columns(
        self, warehouse: str, namespace: str, table_name: str
    ) -> list[tuple[ColumnName, ColTypeStr]]:
        """Return (name, type_string) pairs for all non-id columns."""
        pass

    @abstractmethod
    def get_row_ids(self, warehouse: str, namespace: str, table_name: str) -> set[int]:
        """Return the set of all 'id' column values currently in the table."""
        pass

    reports_snapshots = False

    def current_snapshot(self, warehouse: str, namespace: str, table_name: str) -> tuple[int | None, str | None]:
        """Return the table's current snapshot ID and its operation, or (None, None).

        Implemented only by engines that set `reports_snapshots`.
        """
        raise NotImplementedError(f"{type(self).__name__} does not report snapshots")

    def close(self):
        """Release resources. Called when the workflow completes."""
        pass


def is_s3_tables_warehouse(warehouse_path: str) -> bool:
    """Return True if warehouse_path is an S3 Tables ARN."""
    return warehouse_path.startswith("arn:aws:s3tables:")


@dataclass
class AppendResult:
    """Result of an append operation, including which rows were added."""

    snapshot_id: int
    appended_ids: set[int] = field(default_factory=set)


@dataclass
class DeleteResult:
    """Result of a delete operation, including which rows were removed."""

    snapshot_id: int
    deleted_ids: set[int] = field(default_factory=set)


@dataclass
class SqlResult:
    """Result of running SQL statements, as the writer sees the table afterwards."""

    snapshot_id: int | None
    row_count: int


class MutationEngine(ABC):
    """Performs data mutations on Iceberg tables (append, delete, etc.).

    Implementations use engine-specific APIs (e.g., PySpark DataFrameWriterV2)
    to modify table data as part of a workflow.
    """

    def __init__(self, seed: int | None = None):
        self._rng = random.Random(seed)

    @abstractmethod
    def append_rows(self, namespace: str, table_name: str, num_rows: int) -> AppendResult:
        """Append num_rows to an existing table."""
        pass

    @abstractmethod
    def delete_rows(self, namespace: str, table_name: str, num_rows: int) -> DeleteResult:
        """Delete rows from an existing table.

        Selects num_rows rows at random (using the instance's RNG) and removes them.

        Args:
            namespace: The table's namespace.
            table_name: The table name.
            num_rows: Number of rows to delete.

        Returns:
            The snapshot ID after the delete completes.
        """
        pass

    @abstractmethod
    def rollback_to_snapshot(self, namespace: str, table_name: str, snapshot_id: int) -> int:
        """Roll back the table to a specific snapshot.

        Returns:
            The current snapshot ID after the rollback completes.
        """
        pass

    def run_sql(self, namespace: str, table_name: str, statements: list[str]) -> SqlResult:
        """Run SQL statements in order.

        `${table}` stands for the table's qualified name, and `${namespace}` and `${table_name}`
        for its parts. `$$` becomes `$`, and any other `$` is left as written.
        """
        raise NotImplementedError(f"{type(self).__name__} does not run SQL")

    def close(self):
        """Release resources. Called when the workflow completes."""
        pass
