"""Checks on the column counts and bounds manifests record for data files."""

import math
from typing import NamedTuple

import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.conversions import from_bytes
from pyiceberg.manifest import DataFileContent
from pyiceberg.table import Table
from pyiceberg.types import (
    BinaryType, DateType, DoubleType, FixedType, FloatType, IntegerType, LongType, PrimitiveType, StringType, TimestampType,
    TimestamptzType,
)

from iceberg_fuzzer.backend import AssertionResult
from iceberg_fuzzer.checks.base import ICEBERG_JAVA, SPEC, Basis, NewFilesCheck, file_name, listed, new_entries


class Problem(NamedTuple):
    kind: str
    message: str
    # A count or bound the metrics mode keeps is absent. The spec allows that, so only
    # ColumnMetricsPresent reports it.
    missing: bool = False


def _metrics_mode(properties: dict, column: str) -> str:
    return properties.get(
        f"write.metadata.metrics.column.{column}",
        properties.get("write.metadata.metrics.default", "truncate(16)"),
    )


def _comparable(values: pa.Array, field_type: PrimitiveType) -> pa.Array:
    """Values in the form PyIceberg decodes bounds into."""
    if isinstance(field_type, (TimestampType, TimestamptzType)):
        return values.cast(pa.int64())
    if isinstance(field_type, DateType):
        return values.cast(pa.int32())
    return values


def columns_by_field_id(rows: pa.Table, table: Table) -> dict[int, pa.ChunkedArray]:
    """A file's top-level columns by field ID: from the Parquet field IDs, else from the table's
    name mapping, which keeps a renamed column's earlier names. A file written before a rename or
    an added column has other names or fewer columns than the current schema."""
    mapping = table.metadata.name_mapping()
    by_name = {name: f.field_id for f in mapping for name in f.names} if mapping is not None else {}
    current = {f.name: f.field_id for f in table.schema().fields}
    columns = {}
    for i, column in enumerate(rows.schema):
        field_id = (column.metadata or {}).get(b"PARQUET:field_id")
        if field_id is not None:
            columns[int(field_id)] = rows.column(i)
        elif column.name in by_name or column.name in current:
            columns[by_name.get(column.name, current.get(column.name))] = rows.column(i)
    return columns


def _decode_bound(field_type, bound: bytes):
    """A bound of a column promoted from int or float keeps the 4-byte encoding in files written
    before the promotion."""
    if len(bound) == 4 and isinstance(field_type, LongType):
        return from_bytes(IntegerType(), bound)
    if len(bound) == 4 and isinstance(field_type, DoubleType):
        return from_bytes(FloatType(), bound)
    return from_bytes(field_type, bound)


def _float_key(value: float) -> tuple[float, float]:
    """Iceberg's float order, in which -0.0 sorts below 0.0."""
    return (value, math.copysign(1.0, value))


def _untruncatable_upper(value, mode: str) -> bool:
    """Whether truncation can leave a string or binary column without an upper bound: the value
    is longer than the truncation length and its prefix cannot be incremented."""
    if not mode.startswith("truncate("):
        return False
    length = int(mode[len("truncate("):-1])
    if len(value) <= length:
        return False
    prefix = value[:length]
    return all(c == "\U0010ffff" for c in prefix) if isinstance(value, str) else all(b == 0xFF for b in prefix)


def _column_problems(file: str, field, values: pa.Array, data_file, mode: str) -> list[Problem]:
    fid, where = field.field_id, f"{file} {field.name}"
    problems = []
    floating = pa.types.is_floating(values.type)
    counts = {
        "value_count": ((data_file.value_counts or {}).get(fid), len(values)),
        "null_count": ((data_file.null_value_counts or {}).get(fid), values.null_count),
    }
    if floating:
        nans = int(pc.sum(pc.is_nan(values.drop_null())).as_py() or 0)
        counts["nan_count"] = ((data_file.nan_value_counts or {}).get(fid), nans)
    for kind, (recorded, actual) in counts.items():
        label = kind.replace("_", " ")
        if recorded is not None and recorded != actual:
            problems.append(Problem(kind, f"{where} {label} {recorded}, file has {actual}"))
        if recorded is None and mode != "none" and kind != "nan_count":
            problems.append(Problem(f"missing_{kind}", f"{where} has no {label}", missing=True))
        if recorded is not None and mode == "none":
            problems.append(Problem("counts_in_none_mode", f"{where} has a {label} in none mode"))

    lower = (data_file.lower_bounds or {}).get(fid)
    upper = (data_file.upper_bounds or {}).get(fid)
    if mode in ("counts", "none") and (lower is not None or upper is not None):
        problems.append(Problem(f"bounds_in_{mode}_mode", f"{where} has bounds in {mode} mode"))
    present = values.drop_null()
    if floating:
        present = present.filter(pc.invert(pc.is_nan(present)))
    if len(present) == 0:
        for name, bound in (("lower", lower), ("upper", upper)):
            if bound is not None and floating and math.isnan(_decode_bound(field.field_type, bound)):
                problems.append(Problem("nan_bound", f"{where} {name} bound is NaN"))
        return problems

    if floating:
        ordered = sorted(present.to_pylist(), key=_float_key)
        low, high = ordered[0], ordered[-1]
        key = _float_key
    else:
        extremes = pc.min_max(_comparable(present, field.field_type))
        low, high = extremes["min"].as_py(), extremes["max"].as_py()
        key = lambda value: value  # noqa: E731
    keeps_bounds = mode not in ("counts", "none")
    for name, bound_bytes, extreme in (("lower", lower, low), ("upper", upper, high)):
        if bound_bytes is None:
            missing_allowed = name == "upper" and isinstance(field.field_type, (StringType, BinaryType, FixedType)) \
                and _untruncatable_upper(high, mode)
            if keeps_bounds and not missing_allowed:
                problems.append(Problem(f"missing_{name}_bound", f"{where} has no {name} bound", missing=True))
            continue
        bound = _decode_bound(field.field_type, bound_bytes)
        if floating and math.isnan(bound):
            problems.append(Problem("nan_bound", f"{where} {name} bound is NaN"))
            continue
        beyond = key(bound) > key(extreme) if name == "lower" else key(bound) < key(extreme)
        if beyond or (mode == "full" and key(bound) != key(extreme)):
            problems.append(Problem(f"{name}_bound", f"{where} {name} bound {bound!r}, {'minimum' if name == 'lower' else 'maximum'} {extreme!r}"))
    return problems


class ColumnStatistics(NewFilesCheck):
    """Each data file a step introduces has counts and bounds that match its Parquet contents,
    recomputed per top-level primitive column. Every count or bound that is present must be
    correct whatever the metrics mode. Columns in none mode carry no counts, and columns in counts
    or none mode carry no bounds. Bounds come from the non-null, non-NaN values in Iceberg's
    order, with lower at most the minimum and upper at least the maximum, and equal to them in
    full mode.

    Each kind of problem is its own assertion, so one kind cannot hide another. Counts and bounds
    are optional in the spec, so a missing one is reported only by ColumnMetricsPresent, after
    compactions."""

    name = "column_statistics"
    summary = "Every new data file's counts and bounds match its contents and its columns' metrics modes"
    basis = Basis.SPEC
    source = SPEC + "#manifests"
    # Whether the check reports missing counts and bounds rather than wrong ones.
    reports_missing = False
    expected = "statistics match the file contents and metrics modes"

    def after(self, state, namespace: str, table_name: str) -> list[AssertionResult]:
        table = self._load_table(namespace, table_name)
        entries = new_entries(table, state, DataFileContent.DATA)
        if not entries:
            return [self._not_applicable("no new data files")]
        schema = table.schema()
        by_kind: dict[str, list[str]] = {}
        for entry in entries:
            data_file = entry.data_file
            columns = columns_by_field_id(self._files.read(table, data_file.file_path), table)
            for field in schema.fields:
                if not isinstance(field.field_type, PrimitiveType) or field.field_id not in columns:
                    continue
                mode = _metrics_mode(table.properties, field.name)
                for problem in _column_problems(file_name(data_file.file_path), field, columns[field.field_id].combine_chunks(), data_file, mode):
                    if problem.missing == self.reports_missing:
                        by_kind.setdefault(problem.kind, []).append(problem.message)
        if not by_kind:
            return [self._result([], self.expected, ok="match")]
        return [
            AssertionResult.of(self.name, False, self.expected, listed(messages), problem=kind)
            for kind, messages in by_kind.items()
        ]


class ColumnMetricsPresent(ColumnStatistics):
    """Each data file a compaction introduces has counts and bounds wherever its metrics mode
    keeps them, as Iceberg Java writes them. Readers use them to skip files. A string or binary
    column may lack an upper bound only when truncation cannot form one."""

    name = "column_metrics_present"
    summary = "Every data file a compaction writes has the counts and bounds its columns' metrics modes keep"
    basis = Basis.ICEBERG_JAVA
    source = ICEBERG_JAVA + "core/src/main/java/org/apache/iceberg/MetricsConfig.java"
    operations = ("replace",)
    reports_missing = True
