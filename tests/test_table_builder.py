#!/usr/bin/env python3
"""
Unit tests for UnifiedTableBuilder.

These tests exercise logic that doesn't require a Spark session:
config validation and deterministic file naming.

No _measure_bytes_per_row mock is needed — the builder computes bytes-per-row
lazily, so construction does no disk I/O.
"""

import pytest
from unittest.mock import patch

from iceberg_fuzzer.exhaustive.table_builder import UnifiedTableBuilder
from iceberg_fuzzer.exhaustive.dimensions.data_types import DataTypeChoice
from iceberg_fuzzer.exhaustive.dimensions.interfaces import WriteOptions
from iceberg_fuzzer.exhaustive.dimensions.encoding import EncodingChoice
from iceberg_fuzzer.exhaustive.dimensions.compression import (
    CompressionChoice, CompressionCodec)


@pytest.fixture
def builder(tmp_path):
    """Create a builder (lazy measurement → no Spark, no I/O on construction)."""
    return UnifiedTableBuilder(str(tmp_path / "warehouse"), target_size_mb=0.01, seed=42)


@pytest.fixture
def provider():
    """A minimal int schema provider (build_table now requires one)."""
    return DataTypeChoice([("value", "int")])


@pytest.fixture
def write_options():
    """Minimal write options (build_table now requires them)."""
    return WriteOptions(
        encoding=EncodingChoice({"value": "PLAIN"}),
        compression=CompressionChoice(default_codec=CompressionCodec("ZSTD")),
        schema={"value": "int"})


class TestConfigValidation:
    """build_table rejects configs missing required keys."""

    def test_missing_all_keys(self, builder, provider, write_options):
        with pytest.raises(ValueError, match="Config missing required keys"):
            builder.build_table({}, "ns", "tbl", provider, write_options)

    def test_missing_snapshots(self, builder, provider, write_options):
        config = {'data_files': [], 'delete_files': [], 'row_count': 2}
        with pytest.raises(ValueError, match="snapshots"):
            builder.build_table(config, "ns", "tbl", provider, write_options)

    def test_missing_data_files(self, builder, provider, write_options):
        config = {'delete_files': [], 'snapshots': [], 'row_count': 2}
        with pytest.raises(ValueError, match="data_files"):
            builder.build_table(config, "ns", "tbl", provider, write_options)

    def test_missing_row_count(self, builder, provider, write_options):
        config = {'data_files': [], 'delete_files': [], 'snapshots': []}
        with pytest.raises(ValueError, match="row_count"):
            builder.build_table(config, "ns", "tbl", provider, write_options)


class TestDeterministicFileNaming:
    """_next_file_name produces reproducible names from seed + counter."""

    def test_same_seed_same_names(self, tmp_path):
        b1 = UnifiedTableBuilder(str(tmp_path / "w1"), seed=42)
        b2 = UnifiedTableBuilder(str(tmp_path / "w2"), seed=42)
        names1 = [b1._next_file_name("data") for _ in range(5)]
        names2 = [b2._next_file_name("data") for _ in range(5)]
        assert names1 == names2

    def test_different_seed_different_names(self, tmp_path):
        b1 = UnifiedTableBuilder(str(tmp_path / "w1"), seed=42)
        b2 = UnifiedTableBuilder(str(tmp_path / "w2"), seed=99)
        assert b1._next_file_name("data") != b2._next_file_name("data")

    def test_counter_increments(self, builder):
        n1 = builder._next_file_name("data")
        n2 = builder._next_file_name("data")
        n3 = builder._next_file_name("delete")
        assert n1 != n2 != n3

    def test_prefix_in_name(self, builder):
        assert builder._next_file_name("data").startswith("data-")
        assert builder._next_file_name("delete").startswith("delete-")

    def test_parquet_extension(self, builder):
        assert builder._next_file_name("data").endswith(".parquet")


class TestContextManager:
    """Builder supports with-statement; __exit__ stops Spark via close()."""

    def test_enter_returns_self(self, tmp_path):
        builder = UnifiedTableBuilder(str(tmp_path / "w"), seed=1)
        with builder as b:
            assert b is builder

    def test_exit_calls_close(self, tmp_path):
        builder = UnifiedTableBuilder(str(tmp_path / "w"), seed=1)
        with patch.object(builder, 'close') as mock_close:
            with builder:
                pass
        mock_close.assert_called_once()

    def test_exit_calls_close_on_exception(self, tmp_path):
        # Invoke __enter__/__exit__ explicitly (no unreachable code): close must
        # be called even when an exception is passed to __exit__.
        builder = UnifiedTableBuilder(str(tmp_path / "w"), seed=1)
        with patch.object(builder, 'close') as mock_close:
            builder.__enter__()
            builder.__exit__(RuntimeError, RuntimeError("boom"), None)
        mock_close.assert_called_once()



class TestMeasureBytesPerRow:
    """_measure_bytes_per_row returns a positive integer (lazy)."""

    def test_returns_positive(self, tmp_path):
        builder = UnifiedTableBuilder(str(tmp_path / "w"), target_size_mb=0.01, seed=42)
        assert builder.bytes_per_row > 0
        assert isinstance(builder.bytes_per_row, int)
