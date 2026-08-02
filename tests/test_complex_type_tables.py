#!/usr/bin/env python3
"""End-to-end builds of complex (nested) types across value strategies.

Builds REAL Iceberg tables for a matrix of nested types × value strategies,
each with a non-PLAIN encoding and a compression codec, through the same
provider path the build-shapes CLI uses. Each build is validated against its
config, and the physical parquet is inspected to confirm the requested
per-leaf encoding + compression actually landed.

Type matrix (per the feature spec):
  - 3 lists: two with different element primitives, one with a struct element
  - 1 struct: two different primitive fields
  - 3 maps: two with different key/value primitives, one with a struct value

Value strategies use three shapes that share config 94477's layout so the only
variable is the strategy:
  - repetition   -> 79789
  - nulls        -> 79793
  - row_numbers  -> 79825  (all-distinct)

Encoding is chosen per type to be valid for EVERY leaf of that type: uniform
int leaves can take DELTA_BINARY_PACKED; mixed-leaf types (a string leaf, or a
struct) use DICTIONARY, the only non-PLAIN encoding valid across all types.
"""

import glob

from collections import Counter
from decimal import Decimal
from datetime import date, datetime

import duckdb
import pyarrow.parquet as pq
import pytest

from iceberg_fuzzer.exhaustive.table_builder import UnifiedTableBuilder
from iceberg_fuzzer.exhaustive.shape_config_generator import TableConfigGenerator
from iceberg_fuzzer.exhaustive.dimensions.data_types import (
    DataTypeChoice, TypedValueFiller, physical_value)
from iceberg_fuzzer.exhaustive.dimensions.interfaces import WriteOptions
from iceberg_fuzzer.exhaustive.dimensions.encoding import EncodingChoice, is_compatible
from iceberg_fuzzer.exhaustive.dimensions.compression import (
    CompressionChoice, CompressionCodec)


def _canon(v):
    """Deterministic, hashable canonical key for any generated/read-back value.

    Lists/maps/structs come back from DuckDB as list/dict — not hashable and not
    sortable — so we canonicalize recursively to compare value multisets
    (order-independent) without relying on an id column (there isn't one).
    """
    if v is None:
        return None
    if isinstance(v, (list, tuple)):
        return ("L", tuple(_canon(x) for x in v))
    if isinstance(v, dict):
        return ("D", tuple(sorted((_canon(k), _canon(x)) for k, x in v.items())))
    if isinstance(v, bytes):
        return ("B", v)
    if isinstance(v, Decimal):
        return ("N", str(v))
    if isinstance(v, datetime):
        # tz-aware datetimes may come back from a reader in a different zone
        # (e.g. the machine's local tz) for the SAME instant. Normalize to a UTC
        # timestamp so "2023-01-01T00:00Z" == "2023-01-01T01:00+01:00". Naive
        # datetimes (Iceberg 'timestamp'/NTZ) are compared as-is.
        if v.tzinfo is not None:
            return ("T", v.timestamp())
        return ("T", v.isoformat())
    if isinstance(v, date):
        return ("DATE", v.isoformat())  # distinct tag from dict's "D"
    return ("P", v)


def _read_values(builder, namespace, table_name):
    """Read the physical 'value' column back with DuckDB (all real + padding)."""
    table_path = builder._get_table_path(f"local.{namespace}.{table_name}")
    data_glob = str(table_path / "data" / "data-*.parquet")
    rows = duckdb.query(f"SELECT value FROM read_parquet('{data_glob}')").fetchall()
    return [r[0] for r in rows]


def _expected_real_values(config, col_type):
    """The typed_value the filler stamped on every real row, narrowed to the
    column's physical width (physical_value) so float columns compare exactly."""
    nullable = config.get("value_strategy") == "nulls"
    provider = DataTypeChoice([("value", col_type)], nullable=nullable)
    filled = TypedValueFiller().fill(config, provider)
    return [physical_value(col_type, r.get("typed_value"))
            for df in filled["data_files"] for r in df["rows"]]


# Shapes that share config 94477's layout, one per value strategy.
STRATEGY_CONFIGS = {
    "repetition": 79789,
    "nulls": 79793,
    "row_numbers": 79825,
}

# (label, iceberg_type, encoding, compression). Encoding is valid for every
# leaf of the type; compression is a codec compatible with that encoding.
TYPE_CASES = [
    # 3 lists: two different element primitives + one struct element.
    ("list_int", "list<int>", "DELTA_BINARY_PACKED", "GZIP"),
    ("list_string", "list<string>", "DELTA_LENGTH_BYTE_ARRAY", "ZSTD"),
    ("list_struct", "list<struct<a:int,b:string>>", "DICTIONARY", "SNAPPY"),
    # 1 struct: two different primitive fields.
    ("struct_int_string", "struct<a:int,b:string>", "DICTIONARY", "LZ4"),
    # 3 maps: two different key/value primitives + one struct value.
    ("map_string_int", "map<string,int>", "DICTIONARY", "ZSTD"),
    ("map_int_string", "map<int,string>", "DICTIONARY", "GZIP"),
    ("map_string_struct", "map<string,struct<a:int,b:string>>", "DICTIONARY", "SNAPPY"),
]


@pytest.fixture(scope="module")
def config_cache():
    gen = TableConfigGenerator(max_row_count=6, seed=42)
    wanted = set(STRATEGY_CONFIGS.values())
    cache = {}
    for c in gen.generate_all():
        if c["config_id"] in wanted:
            cache[c["config_id"]] = c
            if len(cache) == len(wanted):
                break
    return cache


@pytest.fixture(scope="module")
def builder(tmp_path_factory):
    warehouse = tmp_path_factory.mktemp("complex_types_warehouse")
    b = UnifiedTableBuilder(str(warehouse), target_size_mb=0.01, seed=42)
    yield b
    b.close()


def _build(builder, config, col_type, encoding, compression, namespace, table_name):
    """Build a nested-type table the way the CLI does (nullable for nulls)."""
    nullable = config.get("value_strategy") == "nulls"
    provider = DataTypeChoice([("value", col_type)], nullable=nullable)
    write_options = WriteOptions(
        encoding=EncodingChoice({"value": encoding}),
        compression=CompressionChoice(default_codec=CompressionCodec(compression)),
        schema={"value": col_type})
    filled = TypedValueFiller().fill(config, provider)
    return builder.build_table(
        filled, namespace, table_name,
        schema_provider=provider, write_options=write_options)


def _data_file(builder, namespace, table_name):
    """Return the path of the first physical data parquet file."""
    table_path = builder._get_table_path(f"local.{namespace}.{table_name}")
    files = sorted(glob.glob(str(table_path / "data" / "data-*.parquet")))
    assert files, f"no data parquet written for {table_name}"
    return files[0]


def _leaf_columns(builder, namespace, table_name):
    """Return {physical_leaf_path: (encodings_tuple, codec)} for the data files."""
    md = pq.read_metadata(_data_file(builder, namespace, table_name))
    rg = md.row_group(0)
    return {
        rg.column(i).path_in_schema: (rg.column(i).encodings, rg.column(i).compression)
        for i in range(rg.num_columns)
    }


@pytest.mark.parametrize("strategy,config_id", list(STRATEGY_CONFIGS.items()))
@pytest.mark.parametrize("label,col_type,encoding,compression", TYPE_CASES,
                         ids=[c[0] for c in TYPE_CASES])
def test_build_complex_type(builder, config_cache, strategy, config_id,
                            label, col_type, encoding, compression):
    # Sanity: the chosen encoding is valid for the whole (nested) type.
    assert is_compatible(col_type, encoding), \
        f"{encoding} not valid for all leaves of {col_type}"

    config = config_cache[config_id]
    table_name = f"{label}_{strategy}"
    nullable = config.get("value_strategy") == "nulls"
    stats = _build(builder, config, col_type, encoding, compression,
                   "complex", table_name)

    # Built and validated against the config (sentinel-size WARN is allowed;
    # every other validation must pass).
    non_sentinel = {k: v for k, v in stats.validations.items()
                    if k != "sentinel_file_size"}
    assert all(non_sentinel.values()), stats.validation_messages

    # The written column's type matches the requested --data-type. Compare the
    # read-back Arrow type against the type the requested string produces.
    written_schema = pq.read_schema(_data_file(builder, "complex", table_name))
    expected = DataTypeChoice([("value", col_type)], nullable=nullable)
    expected_type = expected.get_arrow_schema(expected.get_field_ids()).field("value").type
    assert written_schema.field("value").type == expected_type, \
        f"{table_name}: wrote {written_schema.field('value').type}, expected {expected_type}"

    # The requested compression landed on every physical leaf column.
    leaves = _leaf_columns(builder, "complex", table_name)
    for path, (_encodings, codec) in leaves.items():
        assert codec == compression, f"{path}: {codec} != {compression}"

    # A non-DICTIONARY encoding must actually appear on the leaves it applies to
    # (DICTIONARY is expressed via use_dictionary, not the column_encoding list).
    if encoding != "DICTIONARY":
        assert any(encoding in encs for encs, _ in leaves.values()), \
            f"{encoding} not found on any leaf: {leaves}"

    # VALUE CHECK: every real row's expected value (per the value_strategy) must
    # appear in the read-back. Compared as a canonical multiset because nested
    # values (list/dict) are neither sortable nor hashable. The read-back also
    # contains sentinel padding rows (generated from negative ids), which only
    # ADD entries — so expected-real must be a sub-multiset of the read-back.
    expected = Counter(_canon(v) for v in _expected_real_values(config, col_type))
    actual = Counter(_canon(v) for v in _read_values(builder, "complex", table_name))
    missing = expected - actual
    assert not missing, (
        f"{table_name}: {sum(missing.values())} expected value(s) missing from "
        f"read-back; sample={list(missing)[:3]}")
    # Under the nulls strategy, NULLs must survive into the physical file.
    if strategy == "nulls":
        assert None in _read_values(builder, "complex", table_name)


# (label, iceberg_type, encoding, compression) for the primitive types. Encoding
# is a non-PLAIN encoding valid for that type; compression a compatible codec.
PRIMITIVE_CASES = [
    ("int", "int", "DELTA_BINARY_PACKED", "ZSTD"),
    ("long", "long", "DELTA_BINARY_PACKED", "GZIP"),
    ("float", "float", "BYTE_STREAM_SPLIT", "SNAPPY"),
    ("double", "double", "BYTE_STREAM_SPLIT", "ZSTD"),
    ("string", "string", "DELTA_LENGTH_BYTE_ARRAY", "GZIP"),
    ("boolean", "boolean", "RLE", "SNAPPY"),
    ("binary", "binary", "DELTA_LENGTH_BYTE_ARRAY", "ZSTD"),
    ("date", "date", "DELTA_BINARY_PACKED", "GZIP"),
    ("timestamp", "timestamp", "DELTA_BINARY_PACKED", "ZSTD"),
    ("timestamptz", "timestamptz", "DELTA_BINARY_PACKED", "SNAPPY"),
    ("decimal", "decimal(10,2)", "DICTIONARY", "GZIP"),
    ("fixed", "fixed(16)", "DICTIONARY", "ZSTD"),
]


@pytest.mark.parametrize("strategy,config_id", list(STRATEGY_CONFIGS.items()))
@pytest.mark.parametrize("label,col_type,encoding,compression", PRIMITIVE_CASES,
                         ids=[c[0] for c in PRIMITIVE_CASES])
def test_build_primitive_type(builder, config_cache, strategy, config_id,
                              label, col_type, encoding, compression):
    """One test per primitive type × each value strategy: build a real table and
    assert every expected value (float/decimal narrowed to physical width) round-
    trips into the read-back."""
    assert is_compatible(col_type, encoding), \
        f"{encoding} not valid for {col_type}"

    config = config_cache[config_id]
    table_name = f"prim_{label}_{strategy}"
    stats = _build(builder, config, col_type, encoding, compression,
                   "prim", table_name)

    # Structural validation passes (sentinel-size WARN allowed).
    non_sentinel = {k: v for k, v in stats.validations.items()
                    if k != "sentinel_file_size"}
    assert all(non_sentinel.values()), stats.validation_messages

    # Value multiset: expected real values ⊆ read-back (padding only adds).
    expected = Counter(_canon(v) for v in _expected_real_values(config, col_type))
    actual = Counter(_canon(v) for v in _read_values(builder, "prim", table_name))
    missing = expected - actual
    assert not missing, (
        f"{table_name}: {sum(missing.values())} expected value(s) missing; "
        f"sample={list(missing)[:3]}")
    if strategy == "nulls":
        assert None in _read_values(builder, "prim", table_name)
