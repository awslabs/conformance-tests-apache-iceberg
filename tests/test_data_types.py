"""Tests for the data-types dimension.

Covers primitive types (int, long, float, double, string, decimal(P,S),
fixed(L), date/timestamp, ...) and complex types (list/map/struct with
nesting): value generation, Spark DDL, PyArrow schema + breadth-first field
ids, and the single-column / unsupported-type build invariants.
"""

import random

import pytest
import pyarrow as pa

from iceberg_fuzzer.exhaustive.dimensions.data_types import (
    DataTypeDimension,
    DataTypeChoice,
    TypedValueFiller,
    generate_value,
    physical_value,
    check_required_fields,
)


class TestGenerateValue:
    """Module-level generate_value (primitives + complex types)."""

    def test_int_returns_row_id(self):
        assert generate_value("int", 0) == 0
        assert generate_value("int", 42) == 42

    def test_integer_alias(self):
        assert generate_value("integer", 7) == 7

    def test_primitive_types(self):
        assert generate_value("long", 3) == 3000
        assert generate_value("string", 3) == "value_3"
        assert generate_value("boolean", 4) is True
        # float/double use high-significant-digit fractions so float32 and
        # float64 round them differently (see TestFloatPrecision).
        assert generate_value("double", 3) == 3.987654321098765
        assert generate_value("float", 3) == 3.123456789012345

    def test_parametrized_decimal_and_fixed(self):
        from decimal import Decimal
        assert generate_value("decimal(10,2)", 3) == Decimal("3.03")
        assert generate_value("fixed(4)", 3) == bytes([3, 4, 5, 6])

    def test_complex_types_generate_nested_values(self):
        assert generate_value("list<int>", 1) == [10, 11]
        m = generate_value("map<string,int>", 3)
        assert m == {"value_30": 130, "value_31": 131}
        assert generate_value("struct<a:int,b:string>", 3) == {"a": 30, "b": "value_31"}

    def test_unsupported_type_raises(self):
        with pytest.raises(ValueError, match="Unsupported data type"):
            generate_value("not_a_real_type", 0)

    def test_timestamps_vary_at_microsecond_granularity(self):
        # Arrow type is 'us'; generation must produce non-zero, varying
        # sub-second components so microsecond round-tripping is exercised
        # (whole-second values would never test it).
        for t in ("timestamp", "timestamptz"):
            micros = {generate_value(t, i).microsecond for i in range(1, 6)}
            assert micros == {1, 2, 3, 4, 5}, t  # row_id i -> i microseconds

    def test_timestamps_injective_including_subsecond(self):
        # Distinct row ids -> distinct instants across a realistic range.
        for t in ("timestamp", "timestamptz"):
            vals = [generate_value(t, i) for i in range(10000)]
            assert len(set(vals)) == 10000, t


class TestFloatPrecision:
    """float/double generators exercise the 32-bit vs 64-bit precision boundary,
    and physical_value narrows expectations to the physical storage width."""

    def test_float_and_double_differ_for_same_row(self):
        # Distinct constants → a float column and a double column hold different
        # logical values for the same row.
        assert generate_value("float", 5) != generate_value("double", 5)

    def test_float_value_is_not_float32_representable(self):
        # The whole point: the generated value loses precision when narrowed to
        # float32, so writing it to a float column actually tests narrowing.
        v = generate_value("float", 5)
        assert physical_value("float", v) != v, "value must round under float32"

    def test_double_value_is_float64_exact(self):
        v = generate_value("double", 5)
        assert physical_value("double", v) == v, "double must not be narrowed"

    def test_float32_column_stores_the_narrowed_value(self):
        # A float32 column physically stores exactly physical_value('float', v).
        v = generate_value("float", 7)
        stored = pa.array([v], type=pa.float32())[0].as_py()
        assert stored == physical_value("float", v)

    def test_float_and_double_store_different_bits(self):
        # Same logical row → the float column and double column store DIFFERENT
        # physical values (proving precision is genuinely exercised).
        f_stored = pa.array([generate_value("float", 9)], type=pa.float32())[0].as_py()
        d_stored = pa.array([generate_value("double", 9)], type=pa.float64())[0].as_py()
        assert f_stored != d_stored

    def test_physical_value_narrows_nested_floats(self):
        v = generate_value("list<float>", 2)
        narrowed = physical_value("list<float>", v)
        assert narrowed != v  # at least one element rounds under float32
        assert all(e == pa.array([e], type=pa.float32())[0].as_py() for e in narrowed)


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


class TestPrimitiveTypes:
    """Each primitive maps to the expected DDL + Arrow type."""

    def test_ddl_and_arrow(self):
        cases = {
            "long": ("BIGINT", pa.int64()),
            "string": ("STRING", pa.string()),
            "boolean": ("BOOLEAN", pa.bool_()),
            "double": ("DOUBLE", pa.float64()),
            "date": ("DATE", pa.date32()),
            "timestamp": ("TIMESTAMP_NTZ", pa.timestamp("us")),
            "timestamptz": ("TIMESTAMP", pa.timestamp("us", tz="UTC")),
            "decimal(10,2)": ("DECIMAL(10,2)", pa.decimal128(10, 2)),
            "fixed(4)": ("BINARY", pa.binary(4)),
        }
        for type_str, (ddl_type, arrow_type) in cases.items():
            dtc = DataTypeChoice(columns=[("value", type_str)])
            assert dtc.get_spark_ddl() == f"value {ddl_type} NOT NULL", type_str
            assert dtc.get_arrow_schema(dtc.get_field_ids()).field(0).type == arrow_type, type_str


class TestComplexTypes:
    """Nested list/map/struct DDL, breadth-first field ids, and Arrow schema."""

    def test_list_ddl_and_arrow(self):
        dtc = DataTypeChoice(columns=[("value", "list<int>")])
        assert dtc.get_spark_ddl() == "value ARRAY<INT> NOT NULL"
        t = dtc.get_arrow_schema(dtc.get_field_ids()).field(0).type
        assert pa.types.is_list(t)
        assert t.value_type == pa.int32()

    def test_map_ddl_and_arrow(self):
        dtc = DataTypeChoice(columns=[("value", "map<string,int>")])
        assert dtc.get_spark_ddl() == "value MAP<STRING, INT> NOT NULL"
        t = dtc.get_arrow_schema(dtc.get_field_ids()).field(0).type
        assert pa.types.is_map(t)

    def test_struct_ddl_and_arrow(self):
        dtc = DataTypeChoice(columns=[("value", "struct<a:int,b:string>")])
        assert dtc.get_spark_ddl() == "value STRUCT<a: INT, b: STRING> NOT NULL"
        t = dtc.get_arrow_schema(dtc.get_field_ids()).field(0).type
        assert pa.types.is_struct(t)
        assert t.num_fields == 2

    def test_field_ids_breadth_first(self):
        """Roots/level-1 numbered before level-2 — Iceberg's BFS order."""
        dtc = DataTypeChoice(
            columns=[("value", "struct<inner:struct<x:int,y:string>,items:list<int>>")])
        assert dtc.get_field_ids() == {
            ("value",): 1,
            ("value", "inner"): 2,
            ("value", "items"): 3,
            ("value", "inner", "x"): 4,
            ("value", "inner", "y"): 5,
            ("value", "items", "element"): 6,
        }

    def test_nested_arrow_stamps_all_field_ids(self):
        """Every nested Arrow field carries its PARQUET:field_id."""
        dtc = DataTypeChoice(columns=[("value", "struct<a:int,b:string>")])
        struct_type = dtc.get_arrow_schema(dtc.get_field_ids()).field(0).type
        ids = {struct_type.field(i).name: struct_type.field(i).metadata[b"PARQUET:field_id"]
               for i in range(struct_type.num_fields)}
        assert ids == {"a": b"2", "b": b"3"}

    def test_map_key_required_value_optional(self):
        """Iceberg convention: map key required, value optional."""
        dtc = DataTypeChoice(columns=[("value", "map<string,int>")], nullable=True)
        t = dtc.get_arrow_schema(dtc.get_field_ids()).field(0).type
        assert t.key_field.nullable is False
        assert t.item_field.nullable is True


class TestRequiredStructFields:
    """NOT NULL on a struct field → required field (DDL + Arrow nullability)."""

    def test_ddl_emits_not_null_for_required_field(self):
        dtc = DataTypeChoice(columns=[("value", "struct<a:int NOT NULL,b:string>")])
        # 'a' required → NOT NULL; 'b' optional → no marker.
        assert dtc.get_spark_ddl() == \
            "value STRUCT<a: INT NOT NULL, b: STRING> NOT NULL"

    def test_arrow_nullability_reflects_required(self):
        dtc = DataTypeChoice(columns=[("value", "struct<a:int NOT NULL,b:string>")])
        t = dtc.get_arrow_schema(dtc.get_field_ids()).field(0).type
        fields = {t.field(i).name: t.field(i).nullable for i in range(t.num_fields)}
        assert fields == {"a": False, "b": True}

    def test_case_insensitive_marker(self):
        for marker in ["NOT NULL", "not null", "Not Null"]:
            dtc = DataTypeChoice(columns=[("value", f"struct<a:int {marker}>")])
            t = dtc.get_arrow_schema(dtc.get_field_ids()).field(0).type
            assert t.field(0).nullable is False, marker

    def test_all_fields_optional_by_default(self):
        dtc = DataTypeChoice(columns=[("value", "struct<a:int,b:string>")])
        t = dtc.get_arrow_schema(dtc.get_field_ids()).field(0).type
        assert all(t.field(i).nullable for i in range(t.num_fields))

    def test_required_marker_composes_with_nested_type(self):
        # 'items' required (NOT NULL on the field), but the list element stays
        # optional — the marker attaches to the field, not the element.
        dtc = DataTypeChoice(columns=[("value", "struct<items:list<int> NOT NULL>")])
        t = dtc.get_arrow_schema(dtc.get_field_ids()).field(0).type
        items = t.field(0)
        assert items.name == "items"
        assert items.nullable is False              # field required
        assert items.type.value_field.nullable is True  # element still optional
        assert dtc.get_spark_ddl() == \
            "value STRUCT<items: ARRAY<INT> NOT NULL> NOT NULL"

    def test_field_ids_unaffected_by_required(self):
        # Required-ness must not change breadth-first id assignment.
        req = DataTypeChoice(columns=[("value", "struct<a:int NOT NULL,b:string>")])
        opt = DataTypeChoice(columns=[("value", "struct<a:int,b:string>")])
        assert req.get_field_ids() == opt.get_field_ids()

    def test_generate_value_never_none_for_required(self):
        dtc = DataTypeChoice(columns=[("value", "struct<a:int NOT NULL,b:string>")])
        v = dtc.generate_value("value", 5)
        assert v["a"] is not None


class TestRequiredFieldInvariant:
    """check_required_fields locks 'a present struct never nulls a required
    field', so a bad value fails at fill time, not at Iceberg commit."""

    T = "struct<a:int NOT NULL,b:string>"

    def test_present_struct_with_filled_required_ok(self):
        check_required_fields(self.T, {"a": 1, "b": "x"})          # no raise

    def test_whole_value_none_ok(self):
        # Whole-struct null is the top-level nullable case — allowed.
        check_required_fields(self.T, None)                         # no raise

    def test_optional_field_may_be_none(self):
        check_required_fields(self.T, {"a": 1, "b": None})          # no raise

    def test_required_field_none_raises(self):
        with pytest.raises(ValueError, match="Required field value.a is null"):
            check_required_fields(self.T, {"a": None, "b": "x"})

    def test_nested_struct_in_list_enforced(self):
        t = "list<struct<a:int NOT NULL>>"
        check_required_fields(t, [{"a": 1}, {"a": 2}])              # ok
        with pytest.raises(ValueError, match="null"):
            check_required_fields(t, [{"a": 1}, {"a": None}])

    def test_filler_generated_structs_pass(self):
        # The real generation path must satisfy the invariant for every row.
        config = {
            "value_strategy": "row_numbers",
            "data_files": [{"rows": [{"row_id": i, "value": i} for i in range(4)]}],
        }
        TypedValueFiller().fill(config, DataTypeChoice([("value", self.T)]))

    def test_filler_nulls_strategy_allows_whole_struct_null(self):
        # nulls strategy nulls the WHOLE struct on some rows — must not trip the
        # required-field guard (a null struct doesn't implicate inner fields).
        config = {
            "value_strategy": "nulls",
            "data_files": [{"rows": [
                {"row_id": 0, "value": 0},
                {"row_id": 1, "value": None},
            ]}],
        }
        result = TypedValueFiller().fill(
            config, DataTypeChoice([("value", self.T)], nullable=True))
        rows = result["data_files"][0]["rows"]
        assert rows[1]["typed_value"] is None          # whole struct null, allowed
        assert rows[0]["typed_value"]["a"] is not None  # present struct, a filled


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
    """Single-column invariant + unsupported-type rejection."""

    def test_multi_column_rejected(self):
        with pytest.raises(ValueError, match="exactly one column"):
            DataTypeChoice(columns=[("a", "int"), ("b", "int")])

    def test_zero_columns_rejected(self):
        with pytest.raises(ValueError, match="exactly one column"):
            DataTypeChoice(columns=[])

    def test_unsupported_type_rejected(self):
        with pytest.raises(ValueError, match="Unsupported data type"):
            DataTypeChoice(columns=[("value", "not_a_real_type")])

    def test_unsupported_nested_leaf_rejected(self):
        with pytest.raises(ValueError, match="Unsupported data type"):
            DataTypeChoice(columns=[("value", "list<not_a_real_type>")])


class TestDataTypeDimension:
    """DataTypeDimension enumeration and sampling (primitive + complex)."""

    def test_supported_types_include_primitives_and_complex(self):
        assert "int" in DataTypeDimension.SUPPORTED_TYPES
        assert "string" in DataTypeDimension.SUPPORTED_TYPES
        assert "list<int>" in DataTypeDimension.COMPLEX_TYPES

    def test_enumerate_yields_nullable_and_non_nullable_for_each_type(self):
        dim = DataTypeDimension()
        choices = list(dim.enumerate())
        expected_types = dim.SUPPORTED_TYPES + dim.COMPLEX_TYPES
        # Two variants (nullable + non-nullable) per type.
        assert len(choices) == 2 * len(expected_types)
        for c in choices:
            assert len(c.columns) == 1
        assert {c.nullable for c in choices} == {False, True}

    def test_sample_generates_valid_schema(self):
        dim = DataTypeDimension()
        choice = dim.sample(random.Random(42))
        assert len(choice.columns) == 1
        assert choice.columns[0][1] in (dim.SUPPORTED_TYPES + dim.COMPLEX_TYPES)

    def test_sample_nullability_varies(self):
        dim = DataTypeDimension()
        seen = {dim.sample(random.Random(s)).nullable for s in range(50)}
        assert seen == {False, True}
