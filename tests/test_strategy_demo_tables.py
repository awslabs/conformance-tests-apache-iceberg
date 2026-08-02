#!/usr/bin/env python3
"""
Build a small set of REAL Iceberg tables — one per value strategy, plus one
shape (94477's) rendered under all three strategies — into a fixed warehouse
directory so they can be inspected afterwards (e.g. with DuckDB).

Tables are built through the same provider path the build-shapes CLI uses
(int 'value' column, PLAIN encoding, zstd compression, nullable only for the
nulls strategy). The warehouse is left on disk at:

    tests/output/strategy_demo/

Each table's raw data parquet is then read back with DuckDB to prove the files
are openable. DuckDB reads the PHYSICAL data files (no Iceberg extension), so
what it returns is the written rows including sentinel padding and rows that a
merge-on-read delete would hide — not the delete-applied logical view.
"""

import shutil
from pathlib import Path

import duckdb
import pytest

from iceberg_fuzzer.exhaustive.table_builder import UnifiedTableBuilder
from iceberg_fuzzer.exhaustive.shape_config_generator import TableConfigGenerator
from iceberg_fuzzer.exhaustive.dimensions.data_types import (
    DataTypeChoice, TypedValueFiller, physical_value)
from iceberg_fuzzer.exhaustive.dimensions.interfaces import WriteOptions
from iceberg_fuzzer.exhaustive.dimensions.encoding import EncodingChoice
from iceberg_fuzzer.exhaustive.dimensions.compression import CompressionChoice, CompressionCodec


# Fixed, inspectable output location (relative to the repo's tests/ dir).
WAREHOUSE = Path(__file__).parent / "output" / "strategy_demo"

# Real generator configs. Single-row tables can only be row_numbers/nulls
# (repetition needs >=2 rows). 79825/79793/79789 share config 94477's shape
# ((2,1,1,1,1) data files, 2 deletes, 4 snapshots) under each strategy.
DEMO_CONFIGS = {
    "single_row_numbers": 1,
    "single_nulls": 3,
    "shape94477_row_numbers": 79825,
    "shape94477_nulls": 79793,
    "shape94477_repetition": 79789,
}


@pytest.fixture(scope="module")
def config_cache():
    gen = TableConfigGenerator(max_row_count=6, seed=42)
    wanted = set(DEMO_CONFIGS.values())
    cache = {}
    for c in gen.generate_all():
        if c['config_id'] in wanted:
            cache[c['config_id']] = c
            if len(cache) == len(wanted):
                break
    return cache


@pytest.fixture(scope="module")
def builder():
    """Build into the fixed warehouse (wiped once at the start of the run)."""
    if WAREHOUSE.exists():
        shutil.rmtree(WAREHOUSE)
    WAREHOUSE.mkdir(parents=True)
    b = UnifiedTableBuilder(str(WAREHOUSE), target_size_mb=0.01, seed=42)
    yield b
    b.close()


def build_via_cli(builder, config, namespace, table_name):
    """Build a config the way the build-shapes CLI does."""
    nullable = config.get("value_strategy") == "nulls"
    provider = DataTypeChoice([("value", "int")], nullable=nullable)
    write_options = WriteOptions(
        encoding=EncodingChoice({"value": "PLAIN"}),
        compression=CompressionChoice(default_codec=CompressionCodec("ZSTD")),
        schema={"value": "int"})
    filled = TypedValueFiller().fill(config, provider)
    return builder.build_table(
        filled, namespace, table_name,
        schema_provider=provider, write_options=write_options)


def read_data_parquet_with_duckdb(builder, namespace, table_name):
    """Read the table's physical data parquet files with DuckDB."""
    table_path = builder._get_table_path(f"local.{namespace}.{table_name}")
    data_glob = str(table_path / "data" / "data-*.parquet")
    rows = duckdb.query(f"SELECT value FROM read_parquet('{data_glob}')").fetchall()
    return [r[0] for r in rows]


@pytest.mark.parametrize("name,config_id", list(DEMO_CONFIGS.items()))
def test_build_and_read_back_with_duckdb(builder, config_cache, name, config_id):
    config = config_cache[config_id]
    stats = build_via_cli(builder, config, "demo", name)

    # It built and validated.
    assert all(stats.validations.values()), stats.validation_messages

    # DuckDB can open the physical data parquet.
    values = read_data_parquet_with_duckdb(builder, "demo", name)
    assert len(values) > 0

    # Expected physical values are the filler's typed_value for every real row
    # (the filler applies the value_strategy — e.g. repetition remaps abstract
    # values to grouped typed values, nulls become None). Sentinel padding rows
    # are negative and excluded here. Each expected value is narrowed to the
    # column's physical storage width via physical_value (a no-op for int; for a
    # float column it narrows float64 → float32 so the read-back matches).
    provider = DataTypeChoice(
        [("value", "int")],
        nullable=config.get("value_strategy") == "nulls")
    filled = TypedValueFiller().fill(config, provider)
    expected_real = [
        physical_value("int", r.get('typed_value'))
        for df in filled['data_files'] for r in df['rows']
    ]
    physical_real = [v for v in values if v is None or v >= 0]
    assert sorted(v for v in physical_real if v is not None) == \
        sorted(v for v in expected_real if v is not None)
    # NULL rows (nulls strategy) survive as NULLs in the physical file.
    if config.get('value_strategy') == 'nulls':
        assert None in values
