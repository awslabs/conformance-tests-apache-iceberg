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


def _write_metadata(table_path, fields):
    """Write a minimal v1.metadata.json with one schema for _harvest_field_ids.

    `fields` is the schema's fields list (Iceberg metadata JSON shape).
    """
    import json
    metadata_dir = table_path / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    metadata = {
        "current-schema-id": 0,
        "schemas": [{"schema-id": 0, "type": "struct", "fields": fields}],
    }
    with open(metadata_dir / "v1.metadata.json", "w") as f:
        json.dump(metadata, f)


class TestHarvestFieldIds:
    """_harvest_field_ids reads Iceberg's assigned ids in the same {path: id}
    shape as compute_field_ids, so the two compare with ==."""

    def test_primitive(self, builder, tmp_path):
        from iceberg_fuzzer.exhaustive.dimensions.data_types import compute_field_ids
        table_path = tmp_path / "t_prim"
        _write_metadata(table_path, [
            {"id": 1, "name": "value", "required": True, "type": "int"},
        ])
        harvested = builder._harvest_field_ids(table_path)
        assert harvested == {("value",): 1}
        assert harvested == compute_field_ids([("value", "int")])

    def test_list(self, builder, tmp_path):
        from iceberg_fuzzer.exhaustive.dimensions.data_types import compute_field_ids
        table_path = tmp_path / "t_list"
        _write_metadata(table_path, [
            {"id": 1, "name": "value", "required": False,
             "type": {"type": "list", "element-id": 2,
                      "element": "int", "element-required": False}},
        ])
        harvested = builder._harvest_field_ids(table_path)
        assert harvested == {("value",): 1, ("value", "element"): 2}
        assert harvested == compute_field_ids([("value", "list<int>")])

    def test_map(self, builder, tmp_path):
        from iceberg_fuzzer.exhaustive.dimensions.data_types import compute_field_ids
        table_path = tmp_path / "t_map"
        _write_metadata(table_path, [
            {"id": 1, "name": "value", "required": False,
             "type": {"type": "map", "key-id": 2, "key": "string",
                      "value-id": 3, "value": "int", "value-required": False}},
        ])
        harvested = builder._harvest_field_ids(table_path)
        assert harvested == {("value",): 1, ("value", "key"): 2, ("value", "value"): 3}
        assert harvested == compute_field_ids([("value", "map<string,int>")])

    def test_struct(self, builder, tmp_path):
        from iceberg_fuzzer.exhaustive.dimensions.data_types import compute_field_ids
        table_path = tmp_path / "t_struct"
        _write_metadata(table_path, [
            {"id": 1, "name": "value", "required": False,
             "type": {"type": "struct", "fields": [
                 {"id": 2, "name": "a", "required": False, "type": "int"},
                 {"id": 3, "name": "b", "required": False, "type": "string"},
             ]}},
        ])
        harvested = builder._harvest_field_ids(table_path)
        assert harvested == {("value",): 1, ("value", "a"): 2, ("value", "b"): 3}
        assert harvested == compute_field_ids([("value", "struct<a:int,b:string>")])

    def test_missing_metadata_raises(self, builder, tmp_path):
        (tmp_path / "empty" / "metadata").mkdir(parents=True)
        with pytest.raises(FileNotFoundError):
            builder._harvest_field_ids(tmp_path / "empty")

    def test_unexpected_complex_kind_raises(self, builder, tmp_path):
        # A dict type_node with an unknown 'type' must fail loudly, not silently
        # return a field-id map missing the subtree.
        table_path = tmp_path / "t_bad"
        _write_metadata(table_path, [
            {"id": 1, "name": "value", "required": False,
             "type": {"type": "union", "fields": []}},  # 'union' is not a real kind
        ])
        with pytest.raises(ValueError, match="Unexpected complex type kind 'union'"):
            builder._harvest_field_ids(table_path)


class TestValidateGate:
    """validate=False skips _validate_against_config entirely (no Spark reads);
    validate=True (default) runs it."""

    def _minimal_config(self):
        return {
            'data_files': [{'file_id': 'f0', 'file_index': 0,
                            'rows': [{'row_id': 0, 'value': 0, 'position': 0}]}],
            'delete_files': [],
            'snapshots': [{'data_files': ['f0'], 'delete_files': []}],
            'row_count': 1,
        }

    def _run(self, builder, provider, write_options, validate):
        """Build with Spark/snapshot/metadata all mocked, returning the stats."""
        from iceberg_fuzzer.exhaustive.table_builder import BuildStats
        from unittest.mock import MagicMock
        mock_spark = MagicMock()
        stats = BuildStats(snapshot_count=1, data_file_count=1, delete_file_count=0,
                           record_count=1, total_size_bytes=100)
        with patch.object(builder, '_get_spark', return_value=mock_spark), \
             patch.object(builder, '_build_snapshots'), \
             patch.object(builder, '_query_table_metadata', return_value=stats), \
             patch.object(builder, '_validate_against_config',
                          return_value=({'row_count': True}, [])) as mock_validate:
            result = builder.build_table(
                self._minimal_config(), "ns", "tbl", provider, write_options,
                validate=validate)
        return result, mock_validate

    def test_validate_false_skips(self, builder, provider, write_options):
        result, mock_validate = self._run(builder, provider, write_options, validate=False)
        mock_validate.assert_not_called()
        assert result.validations == {}
        assert result.validation_messages == []

    def test_validate_true_runs(self, builder, provider, write_options):
        result, mock_validate = self._run(builder, provider, write_options, validate=True)
        mock_validate.assert_called_once()
        assert result.validations == {'row_count': True}

    def test_validate_defaults_true(self, builder, provider, write_options):
        # Omitting the arg must behave like validate=True.
        from iceberg_fuzzer.exhaustive.table_builder import BuildStats
        from unittest.mock import MagicMock
        mock_spark = MagicMock()
        stats = BuildStats(snapshot_count=1, data_file_count=1, delete_file_count=0,
                           record_count=1, total_size_bytes=100)
        with patch.object(builder, '_get_spark', return_value=mock_spark), \
             patch.object(builder, '_build_snapshots'), \
             patch.object(builder, '_query_table_metadata', return_value=stats), \
             patch.object(builder, '_validate_against_config',
                          return_value=({}, [])) as mock_validate:
            builder.build_table(self._minimal_config(), "ns", "tbl",
                                provider, write_options)
        mock_validate.assert_called_once()
