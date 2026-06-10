"""Unit tests for point query validation."""

from unittest.mock import MagicMock

import pytest

from iceberg_fuzzer.backend.point_query import (
    ColumnFilter,
    _generate_probe_values,
    _sample_filters_for_column,
    _supports_point_query,
    format_sql_literal,
    validate_point_queries,
)


class TestColumnFilter:
    def test_eq(self):
        f = ColumnFilter.eq("value", 42)
        assert f.column == "value"
        assert f.value == 42
        assert f.is_null is False

    def test_null(self):
        f = ColumnFilter.null("value")
        assert f.column == "value"
        assert f.value is None
        assert f.is_null is True

    def test_repr_eq(self):
        f = ColumnFilter.eq("value", 42)
        assert repr(f) == "value = 42"

    def test_repr_null(self):
        f = ColumnFilter.null("value")
        assert repr(f) == "value IS NULL"

    def test_repr_string_value(self):
        f = ColumnFilter.eq("value", "hello")
        assert repr(f) == "value = 'hello'"


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
        "type_str",
        [
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
            "decimal(10,2)",
            "DECIMAL(5,0)",
        ],
    )
    def test_supported_types(self, type_str):
        assert _supports_point_query(type_str) is True

    @pytest.mark.parametrize(
        "type_str",
        [
            "binary",
            "uuid",
            "fixed(16)",
            "list<int>",
            "map<int, string>",
            "struct<field:int>",
            "array<string>",
        ],
    )
    def test_unsupported_types(self, type_str):
        assert _supports_point_query(type_str) is False


class TestGenerateProbeValues:
    def test_int_probes(self):
        existing = [1, 2, 3, 4, 5]
        probes = _generate_probe_values(existing, "int")
        values = [v for v, _ in probes]
        descriptions = [d for _, d in probes]
        assert 0 in values  # below_min
        assert 6 in values  # above_max
        assert "below_min" in descriptions
        assert "above_max" in descriptions

    def test_int_within_bounds(self):
        existing = [1, 10]
        probes = _generate_probe_values(existing, "int")
        within = [(v, d) for v, d in probes if d == "within_bounds"]
        assert len(within) == 1
        assert 1 < within[0][0] < 10

    def test_float_probes(self):
        existing = [1.0, 2.0, 3.0]
        probes = _generate_probe_values(existing, "float")
        values = [v for v, _ in probes]
        assert any(v < 1.0 for v in values)
        assert any(v > 3.0 for v in values)

    def test_string_probes(self):
        existing = ["apple", "banana", "cherry"]
        probes = _generate_probe_values(existing, "string")
        values = [v for v, _ in probes]
        assert "" in values  # below_min
        assert any(v > "cherry" for v in values)  # above_max

    def test_empty_list(self):
        assert _generate_probe_values([], "int") == []

    def test_all_none(self):
        assert _generate_probe_values([None, None], "int") == []


class TestSampleFiltersForColumn:
    def test_basic_int(self):
        alive_ids = {1, 2, 3, 4, 5}
        filters = _sample_filters_for_column(
            "value", alive_ids, "int", sample_size=10, seed=42
        )
        assert len(filters) > 0
        null_filters = [(f, c) for f, c in filters if f.is_null]
        assert len(null_filters) == 1
        assert null_filters[0][1] == 0  # generate_value never returns None for int

    def test_no_duplicates(self):
        alive_ids = set(range(1, 100))
        filters = _sample_filters_for_column(
            "value", alive_ids, "int", sample_size=10, seed=42
        )
        reprs = [repr(f) for f, _ in filters]
        assert len(reprs) == len(set(reprs))

    def test_exact_sample_size(self):
        alive_ids = set(range(1, 1000))
        filters = _sample_filters_for_column(
            "value", alive_ids, "int", sample_size=10, seed=42
        )
        assert len(filters) == 10

    def test_fewer_than_sample_size_when_insufficient_values(self):
        alive_ids = {1, 2}
        filters = _sample_filters_for_column(
            "value", alive_ids, "boolean", sample_size=10, seed=42
        )
        assert len(filters) < 10
        reprs = [repr(f) for f, _ in filters]
        assert len(reprs) == len(set(reprs))

    def test_min_max_always_included(self):
        alive_ids = set(range(1, 100))
        filters = _sample_filters_for_column(
            "value", alive_ids, "int", sample_size=10, seed=42
        )
        filter_values = [f.value for f, _ in filters if not f.is_null]
        # generate_value("value", "int", rid) returns rid for int type
        assert 1 in filter_values  # min
        assert 99 in filter_values  # max

    def test_probes_have_zero_expected(self):
        alive_ids = {1, 2, 3, 4, 5}
        filters = _sample_filters_for_column(
            "value", alive_ids, "int", sample_size=10, seed=42
        )
        probe_filters = [(f, c) for f, c in filters if c == 0 and not f.is_null]
        assert len(probe_filters) >= 1

    def test_unsupported_type_returns_empty(self):
        alive_ids = {1, 2, 3}
        filters = _sample_filters_for_column(
            "value", alive_ids, "binary", sample_size=10
        )
        assert filters == []

    def test_empty_ids_returns_empty(self):
        filters = _sample_filters_for_column("value", set(), "int", sample_size=10)
        assert filters == []

    def test_sample_size_respected(self):
        alive_ids = set(range(1, 1000))
        filters = _sample_filters_for_column(
            "value", alive_ids, "int", sample_size=5, seed=42
        )
        assert len(filters) == 5

    def test_deterministic_with_seed(self):
        alive_ids = set(range(1, 100))
        filters_a = _sample_filters_for_column(
            "value", alive_ids, "int", sample_size=10, seed=42
        )
        filters_b = _sample_filters_for_column(
            "value", alive_ids, "int", sample_size=10, seed=42
        )
        assert [(repr(f), c) for f, c in filters_a] == [
            (repr(f), c) for f, c in filters_b
        ]

    def test_boolean_type(self):
        alive_ids = {1, 2, 3, 4}
        filters = _sample_filters_for_column(
            "value", alive_ids, "boolean", sample_size=10, seed=42
        )
        assert len(filters) > 0
        eq_filters = [(f, c) for f, c in filters if not f.is_null]
        values = [f.value for f, _ in eq_filters]
        assert True in values or False in values

    def test_uses_column_name(self):
        alive_ids = {1, 2, 3}
        filters = _sample_filters_for_column(
            "int_col", alive_ids, "int", sample_size=5, seed=42
        )
        for filt, _ in filters:
            assert filt.column == "int_col"


class TestValidatePointQueries:
    def _make_engine(self, name, count_map):
        """Create a mock engine that returns counts from a map."""
        engine = MagicMock()
        engine.filtered_count.side_effect = lambda w, ns, tn, filt: count_map.get(
            repr(filt), 0
        )
        return (name, engine)

    def test_all_pass(self):
        alive_ids = {1, 2, 3}
        columns = [("value", "int")]
        count_map = {
            "value IS NULL": 0,
            "value = 1": 1,
            "value = 2": 1,
            "value = 3": 1,
            "value = 0": 0,
            "value = 4": 0,
        }
        engines = [
            self._make_engine("duckdb", count_map),
            self._make_engine("pyspark", count_map),
        ]
        results = validate_point_queries(
            engines, "/tmp/wh", "ns", "tbl", alive_ids, columns, seed=42
        )
        assert all(r.passed for r in results)

    def test_mismatch_detected(self):
        alive_ids = {1, 2, 3}
        columns = [("value", "int")]
        correct_map = {
            "value IS NULL": 0,
            "value = 1": 1,
            "value = 2": 1,
            "value = 3": 1,
            "value = 0": 0,
            "value = 4": 0,
        }
        wrong_map = dict(correct_map)
        wrong_map["value = 2"] = 99

        engines = [
            self._make_engine("duckdb", correct_map),
            self._make_engine("pyspark", wrong_map),
        ]
        results = validate_point_queries(
            engines, "/tmp/wh", "ns", "tbl", alive_ids, columns, seed=42
        )
        failed = [r for r in results if not r.passed]
        assert len(failed) >= 1
        assert any("pyspark" in r.name for r in failed)

    def test_unsupported_type_returns_skipped(self):
        alive_ids = {1, 2, 3}
        columns = [("binary_col", "binary")]
        engines = [self._make_engine("duckdb", {})]
        results = validate_point_queries(
            engines, "/tmp/wh", "ns", "tbl", alive_ids, columns, seed=42
        )
        assert len(results) == 1
        assert results[0].passed is True
        assert "binary_col" in results[0].name
        assert "unsupported type" in results[0].actual

    def test_mixed_columns(self):
        alive_ids = {1, 2, 3}
        columns = [("int_col", "int"), ("binary_col", "binary")]
        count_map = {
            "int_col IS NULL": 0,
            "int_col = 1": 1,
            "int_col = 2": 1,
            "int_col = 3": 1,
            "int_col = 0": 0,
            "int_col = 4": 0,
        }
        engines = [self._make_engine("duckdb", count_map)]
        results = validate_point_queries(
            engines, "/tmp/wh", "ns", "tbl", alive_ids, columns, seed=42
        )
        skipped = [r for r in results if "skipped" in str(r.expected)]
        queried = [r for r in results if "skipped" not in str(r.expected)]
        assert len(skipped) == 1
        assert "binary_col" in skipped[0].name
        assert len(queried) > 0
        assert all(r.passed for r in queried)

    def test_budget_distributed_across_columns(self):
        alive_ids = set(range(1, 100))
        columns = [("col_a", "int"), ("col_b", "int")]
        count_map = {}
        for col in ["col_a", "col_b"]:
            count_map[f"{col} IS NULL"] = 0
            for i in range(1, 100):
                count_map[f"{col} = {i}"] = 1
            count_map[f"{col} = 0"] = 0
            count_map[f"{col} = 100"] = 0
        engines = [self._make_engine("duckdb", count_map)]
        results = validate_point_queries(
            engines, "/tmp/wh", "ns", "tbl", alive_ids, columns, sample_size=10, seed=42
        )
        # 10 total queries across 2 columns = 5 each, 1 engine
        queried = [r for r in results if "skipped" not in str(r.expected)]
        assert len(queried) == 10
        col_a_queries = [r for r in queried if "col_a" in r.name]
        col_b_queries = [r for r in queried if "col_b" in r.name]
        assert len(col_a_queries) == 5
        assert len(col_b_queries) == 5
