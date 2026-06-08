"""Point query validation for Iceberg tables.

Validates that column-level metadata (min/max stats, null counts, predicate
pushdown) is consistent with actual data by running equality and IS NULL
predicates across multiple query engines and asserting agreement.
"""

import random
from collections import Counter
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from iceberg_fuzzer.backend import AssertionResult, ValidationEngine
from iceberg_fuzzer.backend.pyspark.data import generate_value


class ColumnFilter:
    """Engine-agnostic column predicate for point queries."""

    def __init__(self, column: str, value: Any, is_null: bool = False):
        self.column = column
        self.value = value
        self.is_null = is_null

    @staticmethod
    def eq(column: str, value: Any) -> "ColumnFilter":
        return ColumnFilter(column, value)

    @staticmethod
    def null(column: str) -> "ColumnFilter":
        return ColumnFilter(column, None, is_null=True)

    def __repr__(self):
        if self.is_null:
            return f"{self.column} IS NULL"
        return f"{self.column} = {self.value!r}"


def format_sql_literal(value: Any) -> str:
    """Format a Python value as a SQL literal."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    if isinstance(value, (date, datetime)):
        return f"'{value}'"
    if isinstance(value, Decimal):
        return str(value)
    return str(value)


_SUPPORTED_TYPES = frozenset(
    {
        "boolean",
        "bool",
        "int",
        "integer",
        "long",
        "bigint",
        "float",
        "double",
        "string",
        "date",
        "timestamp",
        "timestamp_ntz",
    }
)


def _supports_point_query(type_str: str) -> bool:
    """True for primitive types with reliable equality semantics across engines.

    # TODO: Revisit complex types (list, map, struct, binary, uuid, fixed)
    # once engine-specific literal formatting is implemented.
    """
    t = type_str.lower().strip()
    if t in _SUPPORTED_TYPES:
        return True
    if t.startswith("decimal"):
        return True
    return False


def _generate_probe_values(
    existing_values: list[Any], type_str: str
) -> list[tuple[Any, str]]:
    """Generate values known not to exist (below min, above max, within gaps)."""
    non_null = [v for v in existing_values if v is not None]
    if not non_null:
        return []
    try:
        min_val, max_val = min(non_null), max(non_null)
    except TypeError:
        return []

    existing_set = set(non_null)
    probes: list[tuple[Any, str]] = []
    t = type_str.lower().strip()

    if t in ("int", "integer", "long", "bigint"):
        probes.append((min_val - 1, "below_min"))
        probes.append((max_val + 1, "above_max"))
        if max_val > min_val + 1:
            mid = (min_val + max_val) // 2
            if mid not in existing_set:
                probes.append((mid, "within_bounds"))
            else:
                for offset in range(1, min(1000, (max_val - min_val) // 2)):
                    for candidate in (mid - offset, mid + offset):
                        if (
                            min_val < candidate < max_val
                            and candidate not in existing_set
                        ):
                            probes.append((candidate, "within_bounds"))
                            break
                    else:
                        continue
                    break
    elif t in ("float", "double"):
        probes.append((min_val - 1.0, "below_min"))
        probes.append((max_val + 1.0, "above_max"))
        if max_val > min_val:
            mid = (min_val + max_val) / 2.0
            if mid in existing_set:
                mid = mid + (max_val - min_val) * 1e-9
            if mid not in existing_set and min_val < mid < max_val:
                probes.append((mid, "within_bounds"))
    elif t == "string":
        probes.append(("", "below_min"))
        probes.append((max_val + "z", "above_max"))
    elif t.startswith("decimal"):
        probes.append((min_val - Decimal("1"), "below_min"))
        probes.append((max_val + Decimal("1"), "above_max"))

    return [(v, desc) for v, desc in probes if v not in existing_set]


def _sample_filters(
    alive_ids: set[int],
    type_str: str,
    sample_size: int = 10,
    seed: int | None = None,
) -> list[tuple[ColumnFilter, int]]:
    """Build sampled (filter, expected_count) pairs from deterministic values.

    Computes expected values from generate_value() over alive_ids, then selects
    a representative sample including min, max, null check, non-existent probes,
    and random existing values.
    """
    if not alive_ids or not _supports_point_query(type_str):
        return []

    value_by_id = {rid: generate_value("value", type_str, rid) for rid in alive_ids}
    value_counts = Counter(value_by_id.values())
    all_values = list(value_by_id.values())

    filters: list[tuple[ColumnFilter, int]] = []

    null_count = value_counts.pop(None, 0)
    filters.append((ColumnFilter.null("value"), null_count))

    non_null_values = [v for v in all_values if v is not None]
    distinct_non_null = list(set(non_null_values))

    if not distinct_non_null:
        return filters

    try:
        sorted_distinct = sorted(distinct_non_null)
    except TypeError:
        sorted_distinct = distinct_non_null

    # Always include min and max
    filters.append(
        (ColumnFilter.eq("value", sorted_distinct[0]), value_counts[sorted_distinct[0]])
    )
    if len(sorted_distinct) > 1:
        filters.append(
            (
                ColumnFilter.eq("value", sorted_distinct[-1]),
                value_counts[sorted_distinct[-1]],
            )
        )

    # Non-existent probe values (expected count = 0)
    probes = _generate_probe_values(non_null_values, type_str)
    for probe_val, _ in probes[:2]:
        filters.append((ColumnFilter.eq("value", probe_val), 0))

    # Fill remaining slots with random existing values
    remaining_slots = sample_size - len(filters)
    if remaining_slots > 0 and len(sorted_distinct) > 2:
        middle_values = sorted_distinct[1:-1]
        rng = random.Random(seed)
        sampled = rng.sample(middle_values, min(remaining_slots, len(middle_values)))
        for val in sampled:
            filters.append((ColumnFilter.eq("value", val), value_counts[val]))

    return filters[:sample_size]


def validate_point_queries(
    engines: list[tuple[str, ValidationEngine]],
    warehouse: str,
    namespace: str,
    table_name: str,
    alive_ids: set[int],
    type_str: str,
    sample_size: int = 10,
    seed: int | None = None,
) -> list[AssertionResult]:
    """Run point query validation on the value column across all engines.

    Computes expected values deterministically from alive row IDs,
    samples filters, runs each on all engines, returns AssertionResults.
    """
    if not _supports_point_query(type_str):
        return [AssertionResult.skipped("point_query", f"unsupported type: {type_str}")]

    filters = _sample_filters(alive_ids, type_str, sample_size=sample_size, seed=seed)
    if not filters:
        return []

    assertions: list[AssertionResult] = []

    for filt, expected_count in filters:
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
