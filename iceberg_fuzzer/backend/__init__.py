"""
Backend abstractions for iceberg-fuzzer.

Each backend (duckdb, pyspark, polars, athena) lives in its own subpackage
and implements one or more of: generation, validation, mutation.
"""

import random
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass
class Checkpoint:
    """Captured table state before an operation, used to verify integrity after.

    Attributes:
        row_count: Total number of rows in the table.
        content_hash: Order-independent hash of all row data (XOR of per-row SHA256).
    """
    row_count: int
    content_hash: int


@dataclass
class AssertionResult:
    """Result of a single assertion check within a workflow step.

    Attributes:
        name: Identifier for the assertion (e.g., 'row_count', 'content_hash').
        passed: Whether the assertion passed.
        expected: The expected value.
        actual: The actual value observed.
    """
    name: str
    passed: bool
    expected: Any = None
    actual: Any = None

    @classmethod
    def row_count(cls, expected, actual):
        """Assert row count matches."""
        return cls(name='row_count', passed=expected == actual, expected=expected, actual=actual)

    @classmethod
    def content_hash(cls, expected, actual):
        """Assert content hash matches."""
        return cls(name='content_hash', passed=expected == actual, expected=expected, actual=actual)


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

    def close(self):
        """Release resources. Called when the workflow completes."""
        pass


class MutationEngine(ABC):
    """Performs data mutations on Iceberg tables (append, delete, etc.).

    Implementations use engine-specific APIs (e.g., PySpark DataFrameWriterV2)
    to modify table data as part of a workflow.
    """

    def __init__(self, seed: int | None = None):
        self._rng = random.Random(seed)

    @abstractmethod
    def append_rows(self, namespace: str, table_name: str, num_rows: int):
        """Append rows to an existing table.

        Args:
            namespace: The table's namespace.
            table_name: The table name.
            num_rows: Number of rows to append.
        """
        pass

    @abstractmethod
    def delete_rows(self, namespace: str, table_name: str, num_rows: int):
        """Delete rows from an existing table.

        Selects num_rows rows at random (using the instance's RNG) and removes them.

        Args:
            namespace: The table's namespace.
            table_name: The table name.
            num_rows: Number of rows to delete.
        """
        pass

    def close(self):
        """Release resources. Called when the workflow completes."""
        pass
