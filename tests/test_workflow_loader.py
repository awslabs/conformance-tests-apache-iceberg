"""Unit tests for the workflow loader."""

import pytest
import yaml

from iceberg_fuzzer.workflow.loader import load_workflow


def test_missing_name(tmp_path):
    path = tmp_path / "wf.yaml"
    path.write_text(yaml.dump({
        "steps": [{"id": "a", "operation": "append", "config": {"backend": "pyspark", "num_rows": 1}}]
    }))
    with pytest.raises(ValueError, match="missing required 'name' field"):
        load_workflow(str(path))


def test_missing_steps(tmp_path):
    path = tmp_path / "wf.yaml"
    path.write_text(yaml.dump({"name": "test"}))
    with pytest.raises(ValueError, match="missing or empty 'steps' field"):
        load_workflow(str(path))


def test_empty_steps(tmp_path):
    path = tmp_path / "wf.yaml"
    path.write_text(yaml.dump({"name": "test", "steps": []}))
    with pytest.raises(ValueError, match="missing or empty 'steps' field"):
        load_workflow(str(path))


def test_step_missing_id(tmp_path):
    path = tmp_path / "wf.yaml"
    path.write_text(yaml.dump({
        "name": "test",
        "steps": [{"operation": "append", "config": {"backend": "pyspark", "num_rows": 1}}],
    }))
    with pytest.raises(ValueError, match="missing required 'id' field"):
        load_workflow(str(path))


def test_step_missing_operation(tmp_path):
    path = tmp_path / "wf.yaml"
    path.write_text(yaml.dump({
        "name": "test",
        "steps": [{"id": "a", "config": {"backend": "pyspark"}}],
    }))
    with pytest.raises(ValueError, match="missing required 'operation' field"):
        load_workflow(str(path))


def test_unknown_operation(tmp_path):
    path = tmp_path / "wf.yaml"
    path.write_text(yaml.dump({
        "name": "test",
        "steps": [{"id": "a", "operation": "explode", "config": {}}],
    }))
    with pytest.raises(ValueError, match="unknown operation 'explode'"):
        load_workflow(str(path))


def test_step_missing_config(tmp_path):
    path = tmp_path / "wf.yaml"
    path.write_text(yaml.dump({
        "name": "test",
        "steps": [{"id": "a", "operation": "append"}],
    }))
    with pytest.raises(ValueError, match="missing required 'config' field"):
        load_workflow(str(path))


def test_append_missing_num_rows(tmp_path):
    path = tmp_path / "wf.yaml"
    path.write_text(yaml.dump({
        "name": "test",
        "steps": [{"id": "a", "operation": "append", "config": {"backend": "pyspark"}}],
    }))
    with pytest.raises(ValueError, match="missing required config field 'num_rows'"):
        load_workflow(str(path))


def test_append_missing_backend(tmp_path):
    path = tmp_path / "wf.yaml"
    path.write_text(yaml.dump({
        "name": "test",
        "steps": [{"id": "a", "operation": "append", "config": {"num_rows": 5}}],
    }))
    with pytest.raises(ValueError, match="missing required config field 'backend'"):
        load_workflow(str(path))


def test_delete_missing_num_rows(tmp_path):
    path = tmp_path / "wf.yaml"
    path.write_text(yaml.dump({
        "name": "test",
        "steps": [{"id": "a", "operation": "delete", "config": {"backend": "pyspark"}}],
    }))
    with pytest.raises(ValueError, match="missing required config field 'num_rows'"):
        load_workflow(str(path))


def test_replace_missing_command(tmp_path):
    path = tmp_path / "wf.yaml"
    path.write_text(yaml.dump({
        "name": "test",
        "steps": [{"id": "a", "operation": "replace", "config": {}}],
    }))
    with pytest.raises(ValueError, match="missing required config field 'command'"):
        load_workflow(str(path))


def test_valid_append_only(tmp_path):
    path = tmp_path / "wf.yaml"
    path.write_text(yaml.dump({
        "name": "test",
        "steps": [{"id": "a", "operation": "append", "config": {"backend": "pyspark", "num_rows": 10}}],
    }))
    wf = load_workflow(str(path))
    assert wf.name == "test"
    assert len(wf.steps) == 1


def test_valid_full_workflow(tmp_path):
    path = tmp_path / "wf.yaml"
    path.write_text(yaml.dump({
        "name": "full",
        "steps": [
            {"id": "c1", "operation": "replace", "config": {"command": "echo compact", "timeout": 60}},
            {"id": "app", "operation": "append", "config": {"backend": "pyspark", "num_rows": 5}},
            {"id": "del", "operation": "delete", "config": {"backend": "pyspark", "num_rows": 2}},
        ],
    }))
    wf = load_workflow(str(path))
    assert wf.name == "full"
    assert len(wf.steps) == 3


def test_compaction_spec_file():
    wf = load_workflow("specs/workflows/compaction.yaml")
    assert wf.name == "compact_append_delete_compact"
    assert len(wf.steps) == 4
