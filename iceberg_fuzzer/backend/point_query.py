"""Point query validation for Iceberg tables.

Validates that column-level metadata (min/max stats, null counts, predicate
pushdown) is consistent with actual data by running equality and IS NULL
predicates across multiple query engines and asserting agreement.

Operates on all columns with supported types, distributing a fixed query
budget across them.
"""

import random
from collections import Counter
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from iceberg_fuzzer.backend import AssertionResult, ColumnFilter, ValidationEngine
from iceberg_fuzzer.backend.pyspark.data import generate_value


def format_sql_literal(value: Any) -> str:
    """Format a Python value as a SQL literal."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    if isinstance(value, datetime):
        return f"TIMESTAMP '{value}'"
    if isinstance(value, date):
        return f"DATE '{value}'"
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
        "varchar",
        "date",
        "timestamp",
        "timestamp_ntz",
    }
)

_KNOWN_UNSUPPORTED_EXACT = frozenset(
    {"binary", "uuid", "time", "timestamptz"}
)

_KNOWN_UNSUPPORTED_PREFIXES = (
    "fixed", "list", "map", "struct", "array", "timestamp with",
)


def _supports_point_query(type_str: str) -> bool:
    """True for primitive types with reliable equality semantics across engines."""
    t = type_str.lower().strip()
    if t in _SUPPORTED_TYPES:
        return True
    if t.startswith("decimal"):
        return True
    return False


def _is_known_unsupported(type_str: str) -> bool:
    """True for types we intentionally skip (no equality semantics or no literal format)."""
    t = type_str.lower().strip()
    if t in _KNOWN_UNSUPPORTED_EXACT:
        return True
    return any(t.startswith(prefix) for prefix in _KNOWN_UNSUPPORTED_PREFIXES)


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
    elif t in ("string", "varchar"):
        probes.append(("", "below_min"))
        probes.append((max_val + "z", "above_max"))
    elif t.startswith("decimal"):
        probes.append((min_val - Decimal("1"), "below_min"))
        probes.append((max_val + Decimal("1"), "above_max"))
    elif t in ("date",):
        from datetime import timedelta
        probes.append((min_val - timedelta(days=1), "below_min"))
        probes.append((max_val + timedelta(days=1), "above_max"))
    elif t in ("timestamp", "timestamp_ntz"):
        from datetime import timedelta
        probes.append((min_val - timedelta(seconds=1), "below_min"))
        probes.append((max_val + timedelta(seconds=1), "above_max"))
    elif t in ("boolean", "bool"):
        # For boolean, probe the opposite value if only one exists
        if True not in existing_set:
            probes.append((True, "missing_true"))
        if False not in existing_set:
            probes.append((False, "missing_false"))

    return [(v, desc) for v, desc in probes if v not in existing_set]


def _sample_filters_for_column(
    column_name: str,
    alive_ids: set[int],
    type_str: str,
    sample_size: int = 10,
    seed: int | None = None,
) -> list[tuple[ColumnFilter, int]]:
    """Build sampled (filter, expected_count) pairs for a single column.

    Computes expected values from generate_value() over alive_ids, then selects
    a representative sample including min, max, null check, non-existent probes,
    and random existing values.
    Always produces exactly sample_size filters unless there aren't enough
    distinct values and probes to fill it.
    """
    if not alive_ids or not _supports_point_query(type_str):
        return []

    value_by_id = {row_id: generate_value(column_name, type_str, row_id) for row_id in alive_ids}
    value_counts = Counter(value_by_id.values())
    all_values = list(value_by_id.values())

    filters: list[tuple[ColumnFilter, int]] = []
    seen: set = set()

    def _add(filt: ColumnFilter, expected: int) -> bool:
        key = repr(filt)
        if key in seen:
            return False
        seen.add(key)
        filters.append((filt, expected))
        return True

    null_count = value_counts.pop(None, 0)
    _add(ColumnFilter.null(column_name), null_count)

    non_null_values = [v for v in all_values if v is not None]
    distinct_non_null = list(set(non_null_values))

    if not distinct_non_null:
        return filters[:sample_size]

    try:
        sorted_distinct = sorted(distinct_non_null)
    except TypeError:
        sorted_distinct = distinct_non_null

    _add(
        ColumnFilter.eq(column_name, sorted_distinct[0]),
        value_counts[sorted_distinct[0]],
    )
    if len(sorted_distinct) > 1:
        _add(
            ColumnFilter.eq(column_name, sorted_distinct[-1]),
            value_counts[sorted_distinct[-1]],
        )

    probes = _generate_probe_values(non_null_values, type_str)
    for probe_val, _ in probes:
        if len(filters) >= sample_size:
            break
        _add(ColumnFilter.eq(column_name, probe_val), 0)

    if len(filters) < sample_size and len(sorted_distinct) > 2:
        middle_values = sorted_distinct[1:-1]
        rng = random.Random(seed)
        rng.shuffle(middle_values)
        for val in middle_values:
            if len(filters) >= sample_size:
                break
            _add(ColumnFilter.eq(column_name, val), value_counts[val])

    return filters[:sample_size]


def validate_point_queries(
    engines: list[tuple[str, ValidationEngine]],
    warehouse: str,
    namespace: str,
    table_name: str,
    alive_ids: set[int],
    columns: list[tuple[str, str]],
    sample_size: int = 10,
    seed: int | None = None,
) -> list[AssertionResult]:
    """Run point query validation across all queryable columns and engines.

    Distributes sample_size queries across all columns with supported types.
    Columns with unsupported types are reported as skipped.

    Args:
        engines: List of (name, engine) tuples.
        warehouse: Warehouse path or ARN.
        namespace: Table namespace.
        table_name: Table name.
        alive_ids: Set of row IDs currently in the table.
        columns: List of (column_name, type_str) for all non-id columns.
        sample_size: Total number of point queries to run.
        seed: RNG seed for deterministic sampling.

    Returns:
        List of AssertionResults, one per (filter, engine) combination,
        plus skipped assertions for unsupported columns.
    """
    queryable = [(name, t) for name, t in columns if _supports_point_query(t)]
    skipped = [(name, t) for name, t in columns if not _supports_point_query(t)]

    assertions: list[AssertionResult] = []

    for col_name, col_type in skipped:
        if _is_known_unsupported(col_type):
            assertions.append(
                AssertionResult.skipped(
                    f"point_query.{col_name}", f"unsupported complex type: {col_type}"
                )
            )
        else:
            raise ValueError(
                f"Unrecognized column type '{col_type}' for column '{col_name}'. "
                f"Add it to _SUPPORTED_TYPES or _KNOWN_UNSUPPORTED_PREFIXES."
            )

    if not queryable:
        return assertions

    all_filters: list[tuple[ColumnFilter, int]] = []

    if not alive_ids:
        # Empty table: verify that IS NULL returns 0 for each column
        for col_name, _ in queryable:
            all_filters.append((ColumnFilter.null(col_name), 0))
    else:
        # Distribute budget across queryable columns, capping total at sample_size
        per_column = max(1, sample_size // len(queryable))
        remainder = max(0, sample_size - per_column * len(queryable))

        for i, (col_name, col_type) in enumerate(queryable):
            col_budget = per_column + (1 if i < remainder else 0)
            col_filters = _sample_filters_for_column(
                col_name,
                alive_ids,
                col_type,
                sample_size=col_budget,
                seed=seed,
            )
            all_filters.extend(col_filters)

        all_filters = all_filters[:sample_size]

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
