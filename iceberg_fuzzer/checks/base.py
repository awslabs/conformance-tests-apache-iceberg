"""What every table check shares: its metadata, how it reports, and the helpers that find the files
a step introduced."""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.manifest import DataFileContent
from pyiceberg.table import Table

from iceberg_fuzzer.backend import AssertionResult
from iceberg_fuzzer.backend.arrow_reader import LoadTable

SPEC = "https://iceberg.apache.org/spec/"
CONFIGURATION = "https://iceberg.apache.org/docs/latest/configuration/#write-properties"
ICEBERG_JAVA = "https://github.com/apache/iceberg/blob/apache-iceberg-1.10.0/"
# How many problems an assertion lists before it only counts the rest.
LISTED_PROBLEMS = 5


class Basis(str, Enum):
    """What a check holds the table to."""

    SPEC = "Iceberg spec"
    ICEBERG_JAVA = "Iceberg Java behavior"
    TABLE_PROPERTY = "table property"
    # A compaction rewrites files without changing the table's rows, history or definition.
    COMPACTION = "compaction contract"


class ParquetCache:
    """The most recently read files, shared by the checks one iceberg_metadata_checks call builds,
    since several of them inspect the same new files."""

    def __init__(self, size: int = 32):
        self._size = size
        self._tables: dict[str, pa.Table] = {}

    def read(self, table: Table, path: str) -> pa.Table:
        if path not in self._tables:
            if len(self._tables) >= self._size:
                self._tables.pop(next(iter(self._tables)))
            with table.io.new_input(path).open() as stream:
                self._tables[path] = pq.read_table(stream)
        return self._tables[path]


def file_name(path: str) -> str:
    return path.rsplit("/", 1)[-1]


def listed(problems: list[str]) -> str:
    more = len(problems) - LISTED_PROBLEMS
    return "; ".join(problems[:LISTED_PROBLEMS]) + (f"; and {more} more" if more > 0 else "")


class TableCheck(ABC):
    """A check on the table itself around a workflow step. It captures what it needs in before()
    and asserts in after(). Its class attributes describe it, for reports and `checks list`."""

    name: str
    # One line saying what the check asserts.
    summary: str
    basis: Basis
    # A link to the rule the check holds the table to.
    source: str
    # Operations the check runs after. None means every operation.
    operations: tuple[str, ...] | None = None

    def __init__(self, load_table: LoadTable, files: ParquetCache | None = None):
        self._load_table = load_table
        self._files = files or ParquetCache()

    def applies_to(self, operation: str) -> bool:
        return self.operations is None or operation in self.operations

    def before(self, namespace: str, table_name: str) -> Any:
        """State to compare against after the step. Returns None when nothing is needed."""
        return None

    @abstractmethod
    def after(self, state: Any, namespace: str, table_name: str) -> list[AssertionResult]:
        """Assertions about the table after the step."""

    @classmethod
    def info(cls) -> dict[str, str]:
        return {
            "name": cls.name, "summary": cls.summary, "basis": cls.basis.value, "source": cls.source,
            "operations": ", ".join(cls.operations) if cls.operations else "every step",
        }

    def _result(self, problems: list[str], expected: str, ok: str = "as expected", problem: str | None = None) -> AssertionResult:
        """Passes when there are no problems, and lists the first few when there are."""
        return AssertionResult.of(self.name, not problems, expected, listed(problems) or ok, problem=problem)

    def _not_applicable(self, reason: str) -> AssertionResult:
        return AssertionResult.of(self.name, True, "skipped", reason, applicable=False)


def live_entries(table: Table) -> list:
    """Every live manifest entry, ADDED or EXISTING, of the current snapshot."""
    snapshot = table.current_snapshot()
    if snapshot is None:
        return []
    return [entry for manifest in snapshot.manifests(table.io) for entry in manifest.fetch_manifest_entry(table.io)]


def live_paths(table: Table) -> set[str]:
    return {entry.data_file.file_path for entry in live_entries(table)}


def new_entries(table: Table, paths_before: set[str], content: DataFileContent | None = None) -> list:
    """Live entries, of one content type when given, whose files were not in the table before the
    step, found by path so the result does not depend on the entries' own status or snapshot ID."""
    return [
        entry for entry in live_entries(table)
        if entry.data_file.file_path not in paths_before and (content is None or entry.data_file.content == content)
    ]


class NewFilesCheck(TableCheck):
    """A check on the files a step introduces. before() records the live files' paths."""

    def before(self, namespace: str, table_name: str) -> set[str]:
        return live_paths(self._load_table(namespace, table_name))


class CommitCheck(TableCheck):
    """A check on what a step committed: the current snapshot or metadata file. A step that left
    the table as it was, such as a declined compaction, committed nothing to check."""

    def before(self, namespace: str, table_name: str) -> str:
        return self._load_table(namespace, table_name).metadata_location

    def after(self, state, namespace: str, table_name: str) -> list[AssertionResult]:
        table = self._load_table(namespace, table_name)
        if table.metadata_location == state:
            return [self._not_applicable("the step committed nothing")]
        return self._check_commit(table)

    @abstractmethod
    def _check_commit(self, table: Table) -> list[AssertionResult]:
        """Assertions about the table the step committed."""
