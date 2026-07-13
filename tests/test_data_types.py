"""Tests for the data-types dimension (INT-only scope).

Only ``int`` is supported for now; additional types (long, string, complex,
...) bring their own generate_value logic and tests in follow-up changes. Two
build invariants are also guarded here: the schema is single-column, and any
non-int type is rejected at construction.
"""

import random

import pytest
import pyarrow as pa

from iceberg_fuzzer.exhaustive.dimensions.data_types import (
    DataTypeDimension,
    DataTypeChoice,
    TypedValueFiller,
    generate_value,
)


class TestGenerateValue:
    """Module-level generate_value (int only)."""

    def test_int_returns_row_id(self):
        assert generate_value("int", 0) == 0
        assert generate_value("int", 42) == 42

    def test_integer_alias(self):
        assert generate_value("integer", 7) == 7

    def test_non_int_raises(self):
        with pytest.raises(ValueError, match="Unsupported data type"):
            generate_value("string", 0)


class TestTypedValueFiller:
    """Typed value filling logic (int only)."""

    def test_row_numbers_all_distinct(self):
        """row_numbers strategy with int produces distinct values matching row_id."""
        config = {
            "value_strategy": "row_numbers",
            "data_files": [
                {"rows": [{"row_id": 0, "value": 0},
                          {"row_id": 1, "value": 1},
                          {"row_id": 2, "value": 2}]}
            ],
        }
        filler = TypedValueFiller()
        result = filler.fill(config, DataTypeChoice([("value", "int")]))

        values = [row["typed_value"] for row in result["data_files"][0]["rows"]]
        assert values == [0, 1, 2]
        assert len(set(values)) == 3  # all distinct

    def test_nulls_strategy_marks_none_valued_rows(self):
        """nulls strategy nulls exactly the rows the shape marked value=None.

        This mirrors what the generator emits: null rows carry value=None,
        non-null rows carry value=row_id. Nullness is read from the row itself,
        not from a separate null_rows index.
        """
        config = {
            "value_strategy": "nulls",
            "data_files": [
                {"rows": [
                    {"row_id": 0, "value": 0},
                    {"row_id": 1, "value": None},
                    {"row_id": 2, "value": 2},
                    {"row_id": 3, "value": None},
                ]}
            ],
        }
        filler = TypedValueFiller()
        result = filler.fill(config, DataTypeChoice([("value", "int")]))

        values = [row["typed_value"] for row in result["data_files"][0]["rows"]]
        assert values[0] == 0      # not null
        assert values[1] is None   # null
        assert values[2] == 2      # not null
        assert values[3] is None   # null

    def test_nulls_strategy_single_row_without_null_rows_index(self):
        """A single-row nulls config (value=None, no null_rows key) is still nulled.

        The generator's single-row path sets value=None but never populates a
        null_rows index. The filler must still produce NULL — it reads the row's
        own value, so the missing index is irrelevant.
        """
        config = {
            "value_strategy": "nulls",
            "data_files": [{"rows": [{"row_id": 0, "value": None}]}],
        }
        filler = TypedValueFiller()
        result = filler.fill(config, DataTypeChoice([("value", "int")]))

        assert result["data_files"][0]["rows"][0]["typed_value"] is None

    def test_nulls_strategy_no_none_values_behaves_like_row_numbers(self):
        """A nulls config with no value=None rows fills like row_numbers."""
        config = {
            "value_strategy": "nulls",
            "data_files": [
                {"rows": [{"row_id": 0, "value": 0}, {"row_id": 1, "value": 1}]}
            ],
        }
        filler = TypedValueFiller()
        result = filler.fill(config, DataTypeChoice([("value", "int")]))

        values = [row["typed_value"] for row in result["data_files"][0]["rows"]]
        assert values == [0, 1]
        assert None not in values

    def test_repetition_same_value_same_typed_value(self):
        """repetition strategy assigns the same typed value to rows sharing an abstract value."""
        config = {
            "value_strategy": "repetition",
            "data_files": [
                {"rows": [
                    {"row_id": 0, "value": 10},
                    {"row_id": 1, "value": 20},
                    {"row_id": 2, "value": 10},  # same abstract value as row 0
                    {"row_id": 3, "value": 30},
                ]}
            ],
        }
        filler = TypedValueFiller()
        result = filler.fill(config, DataTypeChoice([("value", "int")]))

        rows = result["data_files"][0]["rows"]
        assert rows[0]["typed_value"] == rows[2]["typed_value"]  # same
        assert rows[1]["typed_value"] != rows[0]["typed_value"]  # different
        assert rows[3]["typed_value"] != rows[0]["typed_value"]  # different

    def test_single_column_returns_scalar(self):
        """_generate_typed_value returns a scalar for the single column."""
        filler = TypedValueFiller()
        choice = DataTypeChoice(columns=[("value", "int")])
        assert filler._generate_typed_value(5, choice) == 5

    def test_schema_included_in_result(self):
        """The chosen schema is recorded on the result config."""
        config = {
            "value_strategy": "row_numbers",
            "data_files": [{"rows": [{"row_id": 0, "value": 0}]}],
        }
        filler = TypedValueFiller()
        result = filler.fill(config, DataTypeChoice([("my_col", "int")]))

        assert "schema" in result
        assert result["schema"] == [("my_col", "int")]


class TestDataTypeChoiceSchemaProvider:
    """DataTypeChoice implements the SchemaProvider protocol."""

    def test_isinstance_schema_provider(self):
        from iceberg_fuzzer.exhaustive.dimensions.interfaces import SchemaProvider
        dtc = DataTypeChoice(columns=[("value", "int")])
        assert isinstance(dtc, SchemaProvider)

    def test_isinstance_schema_provider_nullable(self):
        from iceberg_fuzzer.exhaustive.dimensions.interfaces import SchemaProvider
        dtc = DataTypeChoice(columns=[("value", "int")], nullable=True)
        assert isinstance(dtc, SchemaProvider)


class TestDataTypeChoiceGetSparkDdl:
    """DataTypeChoice.get_spark_ddl()."""

    def test_ddl_int(self):
        dtc = DataTypeChoice(columns=[("value", "int")])
        assert dtc.get_spark_ddl() == "value INT NOT NULL"

    def test_ddl_nullable_omits_not_null(self):
        dtc = DataTypeChoice(columns=[("value", "int")], nullable=True)
        assert dtc.get_spark_ddl() == "value INT"


class TestDataTypeChoiceGetArrowSchema:
    """DataTypeChoice.get_arrow_schema(field_ids) — the field-id seam."""

    def test_arrow_int(self):
        """INT produces an int32 field carrying the supplied PARQUET:field_id."""
        dtc = DataTypeChoice(columns=[("value", "int")])
        schema = dtc.get_arrow_schema(dtc.get_field_ids())
        assert len(schema) == 1
        assert schema.field(0).name == "value"
        assert schema.field(0).type == pa.int32()
        assert schema.field(0).nullable is False
        assert schema.field(0).metadata == {b"PARQUET:field_id": b"1"}

    def test_computed_field_ids(self):
        """get_field_ids computes the single column's id (path-keyed)."""
        dtc = DataTypeChoice(columns=[("value", "int")])
        assert dtc.get_field_ids() == {("value",): 1}

    def test_arrow_stamps_supplied_ids(self):
        """The provider stamps whatever id the caller supplies (the seam)."""
        dtc = DataTypeChoice(columns=[("value", "int")])
        schema = dtc.get_arrow_schema({("value",): 42})
        assert schema.field(0).metadata == {b"PARQUET:field_id": b"42"}

    def test_arrow_nullable_true(self):
        dtc = DataTypeChoice(columns=[("value", "int")], nullable=True)
        assert dtc.get_arrow_schema(dtc.get_field_ids()).field(0).nullable is True

    def test_arrow_nullable_false(self):
        dtc = DataTypeChoice(columns=[("value", "int")], nullable=False)
        assert dtc.get_arrow_schema(dtc.get_field_ids()).field(0).nullable is False


class TestDataTypeChoiceGenerateValue:
    """DataTypeChoice.generate_value()."""

    def test_generate_int_value(self):
        dtc = DataTypeChoice(columns=[("value", "int")])
        assert dtc.generate_value("value", 0) == 0
        assert dtc.generate_value("value", 42) == 42

    def test_generate_value_unknown_column_raises(self):
        dtc = DataTypeChoice(columns=[("value", "int")])
        with pytest.raises(ValueError, match="not found"):
            dtc.generate_value("nonexistent", 0)

    def test_generate_value_deterministic(self):
        dtc = DataTypeChoice(columns=[("value", "int")])
        assert dtc.generate_value("value", 10) == dtc.generate_value("value", 10)


class TestDataTypeChoiceNullable:
    """Nullable behavior of DataTypeChoice."""

    def test_default_not_nullable(self):
        assert DataTypeChoice(columns=[("value", "int")]).nullable is False

    def test_nullable_ddl_no_not_null(self):
        dtc = DataTypeChoice(columns=[("value", "int")], nullable=True)
        ddl = dtc.get_spark_ddl()
        assert "NOT NULL" not in ddl
        assert ddl == "value INT"


class TestDataTypeChoiceInvariants:
    """Single-column + int-only invariants specific to this build."""

    def test_multi_column_rejected(self):
        with pytest.raises(ValueError, match="exactly one column"):
            DataTypeChoice(columns=[("a", "int"), ("b", "int")])

    def test_zero_columns_rejected(self):
        with pytest.raises(ValueError, match="exactly one column"):
            DataTypeChoice(columns=[])

    def test_non_int_type_rejected(self):
        with pytest.raises(ValueError, match="Unsupported data type"):
            DataTypeChoice(columns=[("value", "string")])


class TestDataTypeDimension:
    """DataTypeDimension enumeration and sampling (int only)."""

    def test_supported_types_is_int_only(self):
        assert DataTypeDimension.SUPPORTED_TYPES == ["int"]

    def test_enumerate_yields_nullable_and_non_nullable_int(self):
        choices = list(DataTypeDimension().enumerate())
        assert len(choices) == 2
        for c in choices:
            assert len(c.columns) == 1
            assert c.columns[0][1] == "int"
        assert {c.nullable for c in choices} == {False, True}

    def test_sample_generates_valid_int_schema(self):
        choice = DataTypeDimension().sample(random.Random(42))
        assert len(choice.columns) == 1
        assert choice.columns[0][1] == "int"

    def test_sample_nullability_varies(self):
        dim = DataTypeDimension()
        seen = {dim.sample(random.Random(s)).nullable for s in range(50)}
        assert seen == {False, True}
