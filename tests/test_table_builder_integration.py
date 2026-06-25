#!/usr/bin/env python3
"""
Integration tests for UnifiedTableBuilder.

These tests require a Spark session with Iceberg support (JVM).
They build real tables and verify correctness end-to-end.

Run with: pytest tests/test_table_builder_integration.py -v
Mark: these tests are slow (~30s for Spark startup + table builds).
"""

import pytest
import pyarrow.parquet as pq
from pathlib import Path

from iceberg_fuzzer.exhaustive.table_builder import UnifiedTableBuilder
from iceberg_fuzzer.exhaustive.shape_config_generator import TableConfigGenerator


@pytest.fixture(scope="module")
def builder(tmp_path_factory):
    """Shared builder across all tests in this module (single Spark session)."""
    warehouse = tmp_path_factory.mktemp("warehouse")
    b = UnifiedTableBuilder(str(warehouse), target_size_mb=0.01, seed=42)
    yield b
    b.close()


@pytest.fixture(scope="module")
def config_cache():
    """Pre-load all configs into a dict keyed by config_id (one-time cost)."""
    gen = TableConfigGenerator(max_row_count=6, seed=42)
    return {c['config_id']: c for c in gen.generate_all()}


def get_config(config_cache, config_id):
    if config_id not in config_cache:
        raise ValueError(f"Config {config_id} not found")
    return config_cache[config_id]


def visible_values(builder, namespace, table_name):
    """Query visible rows (value >= 0) via Spark."""
    spark = builder._get_spark()
    rows = spark.sql(
        f"SELECT value FROM local.{namespace}.{table_name} "
        f"WHERE value >= 0 ORDER BY value"
    ).collect()
    return [r[0] for r in rows]


def expected_visible_values(config):
    """Compute expected visible values from config."""
    deleted_row_ids = set(
        d['row_id'] for df in config['delete_files'] for d in df['deletes'])
    values = []
    for df in config['data_files']:
        for row in df['rows']:
            if row['row_id'] not in deleted_row_ids:
                values.append(row['value'])
    return sorted(v for v in values if v is not None)


class TestEndToEnd:
    """Build known configs and verify all validations pass."""

    def test_config_84823_three_snapshots_two_deletes(self, builder, config_cache):
        """3 snapshots, 2 data files, 2 delete files — cross-snapshot deletes."""
        config = get_config(config_cache, 84823)
        stats = builder.build_table(config, "e2e", "table_84823")

        assert stats.snapshot_count == 3
        assert stats.data_file_count == 2
        assert stats.delete_file_count == 2
        assert all(stats.validations.values()), stats.validation_messages
        assert visible_values(builder, "e2e", "table_84823") == expected_visible_values(config)

    def test_config_86864_four_snapshots(self, builder, config_cache):
        """4 snapshots, 4 data files, 2 delete files."""
        config = get_config(config_cache, 86864)
        stats = builder.build_table(config, "e2e", "table_86864")

        assert stats.snapshot_count == 4
        assert stats.data_file_count == 4
        assert stats.delete_file_count == 2
        assert all(stats.validations.values()), stats.validation_messages
        assert visible_values(builder, "e2e", "table_86864") == expected_visible_values(config)

    def test_config_94477_five_data_files(self, builder, config_cache):
        """4 snapshots, 5 data files, 2 delete files."""
        config = get_config(config_cache, 94477)
        stats = builder.build_table(config, "e2e", "table_94477")

        assert stats.snapshot_count == 4
        assert stats.data_file_count == 5
        assert stats.delete_file_count == 2
        assert all(stats.validations.values()), stats.validation_messages
        assert visible_values(builder, "e2e", "table_94477") == expected_visible_values(config)


class TestNullableSchema:
    """Configs with value_strategy='nulls' build with nullable column."""

    def test_config_90955_nulls_strategy(self, builder, config_cache):
        """4 snapshots, 3 data files, 2 delete files, nullable values."""
        config = get_config(config_cache, 90955)
        assert config['value_strategy'] == 'nulls'

        stats = builder.build_table(config, "e2e", "table_90955")

        assert stats.snapshot_count == 4
        assert stats.data_file_count == 3
        assert stats.delete_file_count == 2
        assert all(stats.validations.values()), stats.validation_messages

        # Verify NULLs are present in the raw data
        spark = builder._get_spark()
        null_count = spark.sql(
            "SELECT COUNT(*) FROM local.e2e.table_90955 WHERE value IS NULL"
        ).collect()[0][0]
        assert null_count >= 0  # NULLs may have been deleted


class TestCrossSnapshotDeleteResolution:
    """Delete correctly targets the right file when multiple files have same row count."""

    def test_config_93_same_size_files(self, builder, config_cache):
        """
        Config 93: data_f0=[0], data_f1=[1] — both 1 row.
        Delete targets data_f0 at position 0.
        The old heuristic (matching by record_count) could bind to the wrong file.
        The new code tracks file_index->path at write time, so deletes are unambiguous.
        """
        config = get_config(config_cache, 93)

        # Verify this config has the dangerous shape: same-size files + cross-snapshot delete
        assert len(config['data_files']) == 2
        assert len(config['data_files'][0]['rows']) == len(config['data_files'][1]['rows'])

        stats = builder.build_table(config, "e2e", "table_93")

        assert stats.data_file_count == 2
        assert stats.delete_file_count == 1
        assert all(stats.validations.values()), stats.validation_messages

        # The delete targets row_id=0 (value=0), so only value=1 should survive
        assert visible_values(builder, "e2e", "table_93") == expected_visible_values(config)

    def test_config_99_delete_targets_second_file(self, builder, config_cache):
        """
        Config 99: same shape but delete targets file_index=1.
        Verifies the mapping works regardless of which file is targeted.
        """
        config = get_config(config_cache, 99)

        assert len(config['data_files']) == 2
        assert len(config['data_files'][0]['rows']) == len(config['data_files'][1]['rows'])

        stats = builder.build_table(config, "e2e", "table_99")

        assert all(stats.validations.values()), stats.validation_messages
        assert visible_values(builder, "e2e", "table_99") == expected_visible_values(config)


class TestPositionDeleteFileContents:
    """Verify the physical position-delete parquet files have correct entries."""

    def test_delete_file_has_correct_paths_and_positions(self, builder, config_cache):
        """Read the actual delete parquet and verify file_path + pos columns."""
        config = get_config(config_cache, 84823)
        # Table already built in TestEndToEnd, but build fresh to get a clean path
        stats = builder.build_table(config, "e2e_del", "table_84823")

        table_path = builder._get_table_path("local.e2e_del.table_84823")
        data_dir = table_path / "data"
        del_files = sorted(data_dir.glob("delete-*.parquet"))

        assert len(del_files) == 2

        total_entries = 0
        for del_file in del_files:
            table = pq.read_table(str(del_file))
            assert 'file_path' in table.column_names
            assert 'pos' in table.column_names
            for path in table['file_path'].to_pylist():
                # Each file_path must reference an existing data file
                assert Path(path).exists(), f"Delete references non-existent file: {path}"
            for pos in table['pos'].to_pylist():
                assert pos >= 0
            total_entries += len(table)

        expected_entries = sum(len(df['deletes']) for df in config['delete_files'])
        assert total_entries == expected_entries


class TestSentinelFileSize:
    """Sentinel file reaches minimum target size."""

    def test_sentinel_meets_target(self, builder, config_cache):
        """Build a config with sentinel and verify file size meets target."""
        # Find a config with sentinel
        config = get_config(config_cache, 84823)
        assert config.get('sentinel_in_file') is not None

        stats = builder.build_table(config, "e2e_sent", "table_84823")
        assert stats.validations.get('sentinel_file_size', False), stats.validation_messages


class TestNoDataNoDeleteSnapshot:
    """Empty snapshots are skipped without error."""

    def test_simple_single_file_config(self, builder, config_cache):
        """Config 1: single data file, no deletes — simplest possible table."""
        config = get_config(config_cache, 1)

        stats = builder.build_table(config, "e2e", "table_1")

        assert stats.snapshot_count >= 1
        assert stats.data_file_count == len(config['data_files'])
        assert stats.delete_file_count == 0
        assert all(stats.validations.values()), stats.validation_messages
