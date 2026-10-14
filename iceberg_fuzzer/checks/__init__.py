"""Table checks: assertions about a table's metadata and files around a workflow step.

Validation engines compare rows. Table checks look at the table itself, such as its manifests,
properties, files and earlier snapshots. Each check captures what it needs before a step and
asserts after it, and describes itself with a summary, the basis it holds the table to and a
source link, which `iceberg-fuzzer checks list` prints.

A new check is a TableCheck subclass in one of this package's modules, added to TABLE_CHECKS.
"""

from collections.abc import Iterable

from iceberg_fuzzer.backend.arrow_reader import LoadTable
from iceberg_fuzzer.checks.base import Basis, CommitCheck, NewFilesCheck, ParquetCache, TableCheck
from iceberg_fuzzer.checks.files import DataFileCodec, DataPathLocation, DeleteFileCodec, DeleteFileOrder
from iceberg_fuzzer.checks.manifests import (
    ManifestListConventions, ManifestListHeader, ManifestMinimumSequence, NewFileEntries, avro_header,
)
from iceberg_fuzzer.checks.statistics import ColumnMetricsPresent, ColumnStatistics, columns_by_field_id
from iceberg_fuzzer.checks.table import (
    MetadataCodec, PartitionSummaries, PropertiesUnchanged, RetainedSnapshots, TableDefinitionUnchanged,
)

__all__ = [
    "Basis", "CommitCheck", "NewFilesCheck", "TableCheck", "TABLE_CHECKS", "iceberg_metadata_checks", "avro_header",
    "columns_by_field_id", "ColumnMetricsPresent", "ColumnStatistics", "DataFileCodec", "DataPathLocation", "DeleteFileCodec",
    "DeleteFileOrder", "ManifestListConventions", "ManifestListHeader", "ManifestMinimumSequence", "MetadataCodec",
    "NewFileEntries", "PartitionSummaries", "PropertiesUnchanged", "RetainedSnapshots", "TableDefinitionUnchanged",
]

TABLE_CHECKS: dict[str, type[TableCheck]] = {
    check.name: check
    for check in (
        ManifestMinimumSequence, NewFileEntries, ManifestListHeader, ManifestListConventions,
        DeleteFileOrder, DataFileCodec, DeleteFileCodec, DataPathLocation,
        ColumnStatistics, ColumnMetricsPresent,
        RetainedSnapshots, PropertiesUnchanged, TableDefinitionUnchanged, MetadataCodec, PartitionSummaries,
    )
}


def iceberg_metadata_checks(load_table: LoadTable, names: Iterable[str] | None = None) -> list[TableCheck]:
    """The named table checks, or every one for None or ["all"]. Checks that read data files share
    one cache."""
    names = list(TABLE_CHECKS) if names is None or list(names) == ["all"] else list(names)
    unknown = [n for n in names if n not in TABLE_CHECKS]
    if unknown:
        raise ValueError(f"Unknown table checks: {', '.join(unknown)}. Known: {', '.join(TABLE_CHECKS)}")
    files = ParquetCache()
    return [TABLE_CHECKS[n](load_table, files) for n in names]
