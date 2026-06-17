"""Point query validation for Iceberg tables."""

import random
from collections import Counter
from enum import Enum
from typing import Any, Sequence

from iceberg_fuzzer.backend import (
    AssertionResult,
    ColTypeStr,
    ColumnFilter,
    ColumnName,
    ValidationEngine,
)
from iceberg_fuzzer.backend.pyspark.data import generate_value


class ProbeReason(Enum):
    """Why a probe value was generated."""

    BELOW_MIN = "below_min"
    ABOVE_MAX = "above_max"
    WITHIN_BOUNDS = "within_bounds"


# Types that support point query validation.
# Includes engine-specific synonyms since get_columns() returns engine-native names.
POINT_QUERYABLE_TYPES = frozenset(
    {
        "int",
        "integer",  # Iceberg int; DuckDB returns "INTEGER"
        "long",
        "bigint",  # Iceberg long; DuckDB returns "BIGINT"
    }
)


def format_sql_literal(value: Any) -> str:
    """Format a Python value as a SQL literal."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    return str(value)


def _supports_point_query(type_str: ColTypeStr) -> bool:
    t = type_str.lower().strip()
    return t in POINT_QUERYABLE_TYPES


def _generate_probe_values(
    existing_values: list[Any], type_str: ColTypeStr
) -> list[tuple[Any, ProbeReason]]:
    """Generate values known not to exist (below min, above max, within gaps)."""
    non_null = [v for v in existing_values if v is not None]
    if not non_null:
        return []
    min_val, max_val = min(non_null), max(non_null)

    existing_set = set(non_null)
    probes = []
    t = type_str.lower().strip()

    if t in POINT_QUERYABLE_TYPES:
        probes.append((min_val - 1, ProbeReason.BELOW_MIN))
        probes.append((max_val + 1, ProbeReason.ABOVE_MAX))
        # Find any gap between min and max (a value that doesn't exist)
        for candidate in range(min_val + 1, max_val):
            if candidate not in existing_set:
                probes.append((candidate, ProbeReason.WITHIN_BOUNDS))
                break

    return probes


def _sample_filters_for_column(
    column_name: str,
    alive_ids: set[int],
    type_str: ColTypeStr,
    sample_size: int = 10,
    seed: int | None = None,
) -> list[tuple[ColumnFilter, int]]:
    """Build sampled (filter, expected_count) pairs for a single column.

    Sampling priority (fills up to sample_size):
      1. IS NULL check
      2. Min and max existing values (presence checks)
      3. Non-existent probe values from _generate_probe_values (absence checks)
      4. Random middle values from the existing set (presence checks)
    """
    if not alive_ids or not _supports_point_query(type_str):
        return []

    value_by_id = {
        row_id: generate_value(column_name, type_str, row_id) for row_id in alive_ids
    }
    value_counts = Counter(value_by_id.values())
    null_count = value_counts.pop(None, 0)
    non_null_values = [v for v in value_by_id.values() if v is not None]
    sorted_distinct = sorted(set(non_null_values))

    filters: list[tuple[ColumnFilter, int]] = []

    # 1. NULL check
    filters.append((ColumnFilter.null(column_name), null_count))

    if not sorted_distinct:
        return filters

    # 2. Min/max presence checks
    min_val = sorted_distinct[0]
    max_val = sorted_distinct[-1]
    filters.append((ColumnFilter.eq(column_name, min_val), value_counts[min_val]))
    selected: set[Any] = {min_val}
    if max_val != min_val:
        filters.append((ColumnFilter.eq(column_name, max_val), value_counts[max_val]))
        selected.add(max_val)

    # 3. Absence checks (values that should NOT exist in the table)
    for probe_val, _ in _generate_probe_values(non_null_values, type_str):
        if len(filters) >= sample_size:
            break
        filters.append((ColumnFilter.eq(column_name, probe_val), 0))

    # 4. Random existing values (fills remaining budget, excluding min/max)
    if len(filters) < sample_size:
        remaining = [v for v in sorted_distinct if v not in selected]
        budget = min(sample_size - len(filters), len(remaining))
        rng = random.Random(seed)
        for val in rng.sample(remaining, budget):
            filters.append((ColumnFilter.eq(column_name, val), value_counts[val]))

    return filters


def validate_point_queries(
    engines: Sequence[tuple[str, ValidationEngine]],
    warehouse: str,
    namespace: str,
    table_name: str,
    alive_ids: set[int],
    columns: list[tuple[ColumnName, ColTypeStr]],
    seed: int | None = None,
) -> list[AssertionResult]:
    """Run point query validation across all queryable columns and engines.

    # TODO: Add a budget/sample_size parameter to cap total queries for tables
    # with many columns. Currently generates ~6-10 queries per queryable column.
    """
    queryable_columns = [(name, t) for name, t in columns if _supports_point_query(t)]
    skipped_columns = [(name, t) for name, t in columns if not _supports_point_query(t)]

    assertions: list[AssertionResult] = []

    for col_name, col_type in skipped_columns:
        assertions.append(
            AssertionResult.skipped(
                f"point_query.{col_name}", f"unsupported type: {col_type}"
            )
        )

    if not queryable_columns:
        return assertions

    all_filters: list[tuple[ColumnFilter, int]] = []

    if not alive_ids:
        for col_name, _ in queryable_columns:
            all_filters.append((ColumnFilter.null(col_name), 0))
    else:
        for col_name, col_type in queryable_columns:
            col_filters = _sample_filters_for_column(
                col_name,
                alive_ids,
                col_type,
                seed=seed,
            )
            all_filters.extend(col_filters)

    for filt, expected_count in all_filters:
        for engine_name, engine in engines:
            actual_count = engine.filtered_count(warehouse, namespace, table_name, filt)
            assertion_name = f"point_query.{engine_name}.{filt!r}"
            assertions.append(
                AssertionResult(
                    name=assertion_name,
                    passed=(actual_count == expected_count),
                    expected=expected_count,
                    actual=actual_count,
                )
            )

    return assertions
