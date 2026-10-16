"""Writer backends and table checks are found by name, so new ones plug in without editing the engine."""

import pytest

from iceberg_fuzzer.checks import TABLE_CHECKS, ColumnStatistics, ManifestMinimumSequence, iceberg_metadata_checks
from iceberg_fuzzer.workflow import engine


class RecordingWriter:
    def __init__(self, warehouse_path, seed=None):
        self.warehouse_path, self.seed = warehouse_path, seed


def test_a_registered_writer_backend_is_created_by_name(monkeypatch):
    monkeypatch.setitem(engine.MUTATION_BACKENDS, "recording", f"{__name__}:RecordingWriter")
    writer = engine._init_mutation_engines("/w", {"recording"}, seed=3)["recording"]
    assert isinstance(writer, RecordingWriter) and (writer.warehouse_path, writer.seed) == ("/w", 3)


def test_an_unknown_writer_backend_is_rejected():
    with pytest.raises(ValueError, match="Unknown backend: nope"):
        engine._init_mutation_engines("/w", {"nope"})


def test_table_checks_are_selected_by_name():
    assert [type(c) for c in iceberg_metadata_checks(None)] == list(TABLE_CHECKS.values())
    chosen = iceberg_metadata_checks(None, ["column_statistics", "manifest_minimum"])
    assert [type(c) for c in chosen] == [ColumnStatistics, ManifestMinimumSequence]
    with pytest.raises(ValueError, match="Unknown table checks: nope"):
        iceberg_metadata_checks(None, ["nope"])
