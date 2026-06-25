#!/usr/bin/env python3
"""
Unit tests for UnifiedTableBuilder.

These tests exercise logic that doesn't require a Spark session:
config validation, deterministic naming, sentinel padding, and
target_row_count isolation between builds.
"""

import pytest
from unittest.mock import patch, MagicMock

from iceberg_fuzzer.exhaustive.table_builder import UnifiedTableBuilder, BuildStats


@pytest.fixture
def builder(tmp_path):
    """Create a builder with mocked Spark (no JVM needed)."""
    with patch.object(UnifiedTableBuilder, '_measure_bytes_per_row', return_value=10):
        b = UnifiedTableBuilder(str(tmp_path / "warehouse"), target_size_mb=0.01, seed=42)
    return b


class TestConfigValidation:
    """build_table rejects configs missing required keys."""

    def test_missing_all_keys(self, builder):
        with pytest.raises(ValueError, match="Config missing required keys"):
            builder.build_table({}, "ns", "tbl")

    def test_missing_snapshots(self, builder):
        config = {'data_files': [], 'delete_files': [], 'row_count': 2}
        with pytest.raises(ValueError, match="snapshots"):
            builder.build_table(config, "ns", "tbl")

    def test_missing_data_files(self, builder):
        config = {'delete_files': [], 'snapshots': [], 'row_count': 2}
        with pytest.raises(ValueError, match="data_files"):
            builder.build_table(config, "ns", "tbl")

    def test_missing_row_count(self, builder):
        config = {'data_files': [], 'delete_files': [], 'snapshots': []}
        with pytest.raises(ValueError, match="row_count"):
            builder.build_table(config, "ns", "tbl")


class TestDeterministicFileNaming:
    """_next_file_name produces reproducible names from seed + counter."""

    def test_same_seed_same_names(self, tmp_path):
        with patch.object(UnifiedTableBuilder, '_measure_bytes_per_row', return_value=10):
            b1 = UnifiedTableBuilder(str(tmp_path / "w1"), seed=42)
            b2 = UnifiedTableBuilder(str(tmp_path / "w2"), seed=42)

        names1 = [b1._next_file_name("data") for _ in range(5)]
        names2 = [b2._next_file_name("data") for _ in range(5)]
        assert names1 == names2

    def test_different_seed_different_names(self, tmp_path):
        with patch.object(UnifiedTableBuilder, '_measure_bytes_per_row', return_value=10):
            b1 = UnifiedTableBuilder(str(tmp_path / "w1"), seed=42)
            b2 = UnifiedTableBuilder(str(tmp_path / "w2"), seed=99)

        name1 = b1._next_file_name("data")
        name2 = b2._next_file_name("data")
        assert name1 != name2

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


class TestSentinelPadding:
    """_pad_sentinel_values pads correctly to reach target_row_count."""

    def test_pads_when_below_target(self, builder):
        builder.target_row_count = 100
        values = [0, 1, 2]
        padded = builder._pad_sentinel_values(values)
        assert len(padded) == 100
        assert padded[:3] == [0, 1, 2]

    def test_padding_values_are_negative(self, builder):
        builder.target_row_count = 5
        padded = builder._pad_sentinel_values([0, 1])
        # Padding starts at -1000000
        assert all(v < 0 for v in padded[2:])
        assert padded[2] == -1000000
        assert padded[3] == -1000001
        assert padded[4] == -1000002

    def test_no_padding_when_at_target(self, builder):
        builder.target_row_count = 3
        values = [0, 1, 2]
        padded = builder._pad_sentinel_values(values)
        assert padded == [0, 1, 2]

    def test_no_padding_when_above_target(self, builder):
        builder.target_row_count = 2
        values = [0, 1, 2, 3]
        padded = builder._pad_sentinel_values(values)
        assert padded == [0, 1, 2, 3]

    def test_does_not_mutate_input(self, builder):
        builder.target_row_count = 5
        original = [0, 1]
        builder._pad_sentinel_values(original)
        assert original == [0, 1]


class TestTargetRowCountIsolation:
    """target_row_count resets between build_table calls (no leak)."""

    def test_reset_on_each_build(self, tmp_path):
        with patch.object(UnifiedTableBuilder, '_measure_bytes_per_row', return_value=10):
            builder = UnifiedTableBuilder(str(tmp_path / "w"), target_size_mb=0.01, seed=42)

        initial = builder.target_row_count
        # Simulate what a sentinel retry would do
        builder.target_row_count += 500
        assert builder.target_row_count == initial + 500

        # Calling build_table should reset — mock Spark to avoid actual build
        config = {
            'data_files': [{'file_id': 'f0', 'file_index': 0, 'rows': [{'row_id': 0, 'value': 0}]}],
            'delete_files': [],
            'snapshots': [{'data_files': ['f0'], 'delete_files': []}],
            'row_count': 1,
        }
        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = [MagicMock(**{'__getitem__': lambda s, i: 0})]

        with patch.object(builder, '_get_spark', return_value=mock_spark), \
             patch.object(builder, '_build_snapshots'), \
             patch.object(builder, '_query_table_metadata', return_value=BuildStats(
                 snapshot_count=1, data_file_count=1, delete_file_count=0,
                 record_count=1, total_size_bytes=100)), \
             patch.object(builder, '_validate_against_config', return_value=({}, [])):
            builder.build_table(config, "ns", "tbl")

        assert builder.target_row_count == initial


class TestContextManager:
    """Builder supports with-statement."""

    def test_enter_returns_self(self, tmp_path):
        with patch.object(UnifiedTableBuilder, '_measure_bytes_per_row', return_value=10):
            builder = UnifiedTableBuilder(str(tmp_path / "w"), seed=1)
        with builder as b:
            assert b is builder

    def test_exit_calls_close(self, tmp_path):
        with patch.object(UnifiedTableBuilder, '_measure_bytes_per_row', return_value=10):
            builder = UnifiedTableBuilder(str(tmp_path / "w"), seed=1)
        with patch.object(builder, 'close') as mock_close:
            with builder:
                pass
        mock_close.assert_called_once()

    def test_exit_calls_close_on_exception(self, tmp_path):
        with patch.object(UnifiedTableBuilder, '_measure_bytes_per_row', return_value=10):
            builder = UnifiedTableBuilder(str(tmp_path / "w"), seed=1)
        with patch.object(builder, 'close') as mock_close:
            with pytest.raises(RuntimeError):
                with builder:
                    raise RuntimeError("boom")
        mock_close.assert_called_once()


class TestIcebergTypeMapping:
    """_iceberg_type_to_arrow maps correctly."""

    def test_known_types(self, builder):
        import pyarrow as pa
        assert builder._iceberg_type_to_arrow("int") == pa.int32()
        assert builder._iceberg_type_to_arrow("long") == pa.int64()
        assert builder._iceberg_type_to_arrow("string") == pa.string()
        assert builder._iceberg_type_to_arrow("boolean") == pa.bool_()
        assert builder._iceberg_type_to_arrow("double") == pa.float64()

    def test_unknown_type_defaults_to_int32(self, builder):
        import pyarrow as pa
        assert builder._iceberg_type_to_arrow("unknown_type") == pa.int32()


class TestMeasureBytesPerRow:
    """_measure_bytes_per_row returns a positive integer."""

    def test_returns_positive(self, tmp_path):
        # Use actual measurement (no mock) — just writes a temp parquet
        builder = UnifiedTableBuilder(str(tmp_path / "w"), target_size_mb=0.01, seed=42)
        assert builder.bytes_per_row > 0
        assert isinstance(builder.bytes_per_row, int)
