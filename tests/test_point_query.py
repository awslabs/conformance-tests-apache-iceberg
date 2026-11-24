"""Tests for point query validation logic."""

import pytest
from unittest.mock import MagicMock

from iceberg_conformance.backend import ColumnFilter
from iceberg_conformance.backend.point_query import (
    ProbeReason,
    _generate_probe_values,
    _sample_filters_for_column,
    _supports_point_query,
    format_sql_literal,
    validate_point_queries,
)

TEST_COL = "value"
TEST_WAREHOUSE = "/test-warehouse"
TEST_NAMESPACE = "test-namespace"
TEST_TABLE = "test-table"
TEST_ENGINE_1 = "test_engine_1"
TEST_ENGINE_2 = "test_engine_2"


class TestColumnFilter:
    def test_eq(self):
        filt = ColumnFilter.eq("col1", 42)
        assert filt.column == "col1"
        assert filt.value == 42
        assert filt.is_null is False

    def test_null(self):
        filt = ColumnFilter.null("col1")
        assert filt.column == "col1"
        assert filt.is_null is True

    def test_repr_eq(self):
        filt = ColumnFilter.eq("col1", 42)
        assert repr(filt) == "col1 = 42"

    def test_repr_null(self):
        filt = ColumnFilter.null("col1")
        assert repr(filt) == "col1 IS NULL"


class TestFormatSqlLiteral:
    def test_none(self):
        assert format_sql_literal(None) == "NULL"

    def test_bool_true(self):
        assert format_sql_literal(True) == "TRUE"

    def test_bool_false(self):
        assert format_sql_literal(False) == "FALSE"

    def test_int(self):
        assert format_sql_literal(42) == "42"

    def test_float(self):
        assert format_sql_literal(3.14) == "3.14"

    def test_string(self):
        assert format_sql_literal("hello") == "'hello'"

    def test_string_with_quotes(self):
        assert format_sql_literal("it's") == "'it''s'"


class TestSupportsPointQuery:
    @pytest.mark.parametrize(
        "type_str", ["int", "integer", "long", "bigint", "INT", "Long"]
    )
    def test_supported(self, type_str):
        assert _supports_point_query(type_str) is True

    @pytest.mark.parametrize(
        "type_str",
        [
            "string",
            "float",
            "double",
            "boolean",
            "date",
            "timestamp",
            "decimal(10,2)",
            "binary",
            "list<int>",
            "map<string,int>",
        ],
    )
    def test_unsupported(self, type_str):
        assert _supports_point_query(type_str) is False


class TestGenerateProbeValues:
    def test_int_probes(self):
        existing = [1, 2, 3, 5, 9, 10]
        probes = _generate_probe_values(existing, "int")

        below_min_probes = [v for v, desc in probes if desc == ProbeReason.BELOW_MIN]
        assert len(below_min_probes) > 0, (
            "must have at least one probe below minimum value"
        )
        for value in below_min_probes:
            assert value < min(existing)

        above_max_probes = [v for v, desc in probes if desc == ProbeReason.ABOVE_MAX]
        assert len(above_max_probes) > 0, (
            "must have at least one probe above maximum value"
        )
        for value in above_max_probes:
            assert value > max(existing)

        within = [v for v, desc in probes if desc == ProbeReason.WITHIN_BOUNDS]
        assert len(within) >= 1, (
            "must produce probe for value within bounds with test input"
        )
        for v in within:
            assert min(existing) < v < max(existing), (
                "value probed for must be within bounds"
            )

    def test_no_within_bounds(self):
        existing = [1, 2, 3, 4, 5]
        probes = _generate_probe_values(existing, "int")
        within = [v for v, desc in probes if desc == ProbeReason.WITHIN_BOUNDS]
        assert len(within) == 0, "contiguous range has no gaps to probe"

    def test_empty_list(self):
        probes = _generate_probe_values([], "int")
        assert probes == []

    def test_all_none(self):
        probes = _generate_probe_values([None, None], "int")
        assert probes == []


class TestSampleFiltersForColumn:
    def test_basic_int(self):
        alive_ids = set(range(1, 11))
        filters = _sample_filters_for_column(TEST_COL, alive_ids, "int", sample_size=10)
        by_value = {
            filt.value: expected for filt, expected in filters if not filt.is_null
        }

        null_filters = [(filt, expected) for filt, expected in filters if filt.is_null]
        assert len(null_filters) == 1, "should have exactly one NULL check"
        assert null_filters[0][1] == 0, "int type never generates None"

        assert by_value[1] == 1, "min value (1) should exist once"
        assert by_value[10] == 1, "max value (10) should exist once"
        assert by_value[0] == 0, "probe below min (min-1=0) expects 0 rows"
        assert by_value[11] == 0, "probe above max (max+1=11) expects 0 rows"

    def test_no_duplicates(self):
        alive_ids = set(range(1, 51))
        filters = _sample_filters_for_column(TEST_COL, alive_ids, "int", sample_size=10)
        reprs = [repr(filt) for filt, _ in filters]
        assert len(reprs) == len(set(reprs)), f"duplicate filters: {reprs}"

    def test_exact_sample_size(self):
        alive_ids = set(range(1, 101))
        filters = _sample_filters_for_column(TEST_COL, alive_ids, "long", sample_size=5)
        assert len(filters) <= 5

    def test_min_max_always_included(self):
        alive_ids = set(range(1, 21))
        filters = _sample_filters_for_column(TEST_COL, alive_ids, "int", sample_size=10)
        filter_values = [filt.value for filt, _ in filters if not filt.is_null]
        # Values are deterministic, copied from row ID for int type
        expected_min_val, expected_max_val = min(alive_ids), max(alive_ids)
        assert expected_min_val in filter_values
        assert expected_max_val in filter_values

    def test_probes_have_zero_expected(self):
        alive_ids = set(range(1, 21))
        filters = _sample_filters_for_column(TEST_COL, alive_ids, "int", sample_size=10)
        zero_count_filters = [
            filt
            for filt, expected_count in filters
            if expected_count == 0 and not filt.is_null
        ]
        assert len(zero_count_filters) > 0, (
            "there must be at least one filter returning zero results"
        )

    def test_empty_ids_returns_empty(self):
        filters = _sample_filters_for_column(TEST_COL, set(), "int", sample_size=10)
        assert filters == [], (
            "there must be no filters returning results when there are no rows"
        )

    def test_deterministic_with_seed(self):
        alive_ids = set(range(1, 101))
        filters1 = _sample_filters_for_column(
            TEST_COL, alive_ids, "int", sample_size=10, seed=42
        )
        filters2 = _sample_filters_for_column(
            TEST_COL, alive_ids, "int", sample_size=10, seed=42
        )
        assert filters1 == filters2


class TestValidatePointQueries:
    def _make_engine(self, return_count=0):
        engine = MagicMock()
        engine.filtered_count.return_value = return_count
        return engine

    def test_basic_pass(self):
        def correct_count(_warehouse, _namespace, _table_name, column_filter):
            if column_filter.is_null:
                return 0
            if column_filter.value == 1:
                return 1  # row with id=1 exists
            return 0

        engine = MagicMock()
        engine.filtered_count.side_effect = correct_count
        engines = [(TEST_ENGINE_1, engine)]
        results = validate_point_queries(
            engines,
            TEST_WAREHOUSE,
            TEST_NAMESPACE,
            TEST_TABLE,
            alive_ids={1},
            columns=[(TEST_COL, "int")],
        )
        assert all(r.passed for r in results), (
            f"failures: {[(r.name, r.expected, r.actual) for r in results if not r.passed]}"
        )

    def test_basic_fail(self):
        engine = self._make_engine(return_count=999)
        engines = [(TEST_ENGINE_1, engine)]
        results = validate_point_queries(
            engines,
            TEST_WAREHOUSE,
            TEST_NAMESPACE,
            TEST_TABLE,
            alive_ids={1, 2, 3},
            columns=[(TEST_COL, "int")],
        )
        assert all(not r.passed for r in results), (
            f"unexpected passes: {[(r.name, r.expected, r.actual) for r in results if r.passed]}"
        )

    def test_multi_engine_dispatch(self):
        # Verifies both engines are queried and appear in results.
        # Correctness of pass/fail logic is covered by test_basic_pass/test_basic_fail.
        engine1 = self._make_engine(return_count=1)
        engine2 = self._make_engine(return_count=1)
        engines = [(TEST_ENGINE_1, engine1), (TEST_ENGINE_2, engine2)]
        results = validate_point_queries(
            engines,
            TEST_WAREHOUSE,
            TEST_NAMESPACE,
            TEST_TABLE,
            alive_ids={1},
            columns=[(TEST_COL, "int")],
        )
        assert engine1.filtered_count.called
        assert engine2.filtered_count.called
        names = [r.name for r in results]
        assert any(TEST_ENGINE_1 in n for n in names)
        assert any(TEST_ENGINE_2 in n for n in names)

    def test_unsupported_type_skipped(self):
        engine = self._make_engine()
        engines = [(TEST_ENGINE_1, engine)]
        results = validate_point_queries(
            engines,
            TEST_WAREHOUSE,
            TEST_NAMESPACE,
            TEST_TABLE,
            alive_ids={1, 2, 3},
            columns=[("col_x", "not_a_real_type")],
        )
        assert len(results) == 1
        assert results[0].passed is True
        assert results[0].expected == "skipped"
        assert "unsupported type" in results[0].actual

    def test_empty_alive_ids(self):
        engine = self._make_engine(return_count=0)
        engines = [(TEST_ENGINE_1, engine)]
        results = validate_point_queries(
            engines,
            TEST_WAREHOUSE,
            TEST_NAMESPACE,
            TEST_TABLE,
            alive_ids=set(),
            columns=[(TEST_COL, "int")],
        )
        assert len(results) == 1, "must return 1 filter (the null filter)"
        assert results[0].passed is True, (
            "must pass as we created mock engine that returns 0 rows"
        )
        assert results[0].expected == 0

    def test_multi_column(self):
        def correct_count(_warehouse, _namespace, _table_name, column_filter):
            if column_filter.is_null:
                return 0
            if column_filter.value in (1, 2, 3):
                return 1
            return 0

        engine = MagicMock()
        engine.filtered_count.side_effect = correct_count
        engines = [(TEST_ENGINE_1, engine)]
        results = validate_point_queries(
            engines,
            TEST_WAREHOUSE,
            TEST_NAMESPACE,
            TEST_TABLE,
            alive_ids={1, 2, 3},
            columns=[("col_a", "int"), ("col_b", "int")],
        )
        col_a_results = [r for r in results if "col_a" in r.name]
        col_b_results = [r for r in results if "col_b" in r.name]
        assert len(col_a_results) > 0, "must be at least 1 assertion for column a"
        assert len(col_b_results) > 0, "must be at least 1 assertion for column b"
        assert all(r.passed for r in results), (
            f"failures: {[(r.name, r.expected, r.actual) for r in results if not r.passed]}"
        )
