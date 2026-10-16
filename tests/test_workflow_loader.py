"""Unit tests for the workflow loader."""

import pytest
import yaml

from iceberg_fuzzer.workflow.loader import REPLACE_EXPECTATIONS, load_workflow


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


def test_rollback_missing_to_step(tmp_path):
    path = tmp_path / "wf.yaml"
    path.write_text(yaml.dump({
        "name": "test",
        "steps": [
            {"id": "a", "operation": "append", "config": {"backend": "pyspark", "num_rows": 5}},
            {"id": "b", "operation": "rollback", "config": {"backend": "pyspark"}},
        ],
    }))
    with pytest.raises(ValueError, match="missing required config field 'to_step'"):
        load_workflow(str(path))


def test_rollback_missing_backend(tmp_path):
    path = tmp_path / "wf.yaml"
    path.write_text(yaml.dump({
        "name": "test",
        "steps": [
            {"id": "a", "operation": "append", "config": {"backend": "pyspark", "num_rows": 5}},
            {"id": "b", "operation": "rollback", "config": {"to_step": "a"}},
        ],
    }))
    with pytest.raises(ValueError, match="missing required config field 'backend'"):
        load_workflow(str(path))


def test_rollback_references_replace_step(tmp_path):
    path = tmp_path / "wf.yaml"
    path.write_text(yaml.dump({
        "name": "test",
        "steps": [
            {"id": "compact", "operation": "replace", "config": {"command": "echo hi"}},
            {"id": "rb", "operation": "rollback", "config": {"backend": "pyspark", "to_step": "compact"}},
        ],
    }))
    with pytest.raises(ValueError, match="not a prior append/delete/sql step"):
        load_workflow(str(path))


def test_rollback_references_future_step(tmp_path):
    path = tmp_path / "wf.yaml"
    path.write_text(yaml.dump({
        "name": "test",
        "steps": [
            {"id": "rb", "operation": "rollback", "config": {"backend": "pyspark", "to_step": "app"}},
            {"id": "app", "operation": "append", "config": {"backend": "pyspark", "num_rows": 5}},
        ],
    }))
    with pytest.raises(ValueError, match="not a prior append/delete/sql step"):
        load_workflow(str(path))


def test_rollback_references_nonexistent_step(tmp_path):
    path = tmp_path / "wf.yaml"
    path.write_text(yaml.dump({
        "name": "test",
        "steps": [
            {"id": "app", "operation": "append", "config": {"backend": "pyspark", "num_rows": 5}},
            {"id": "rb", "operation": "rollback", "config": {"backend": "pyspark", "to_step": "ghost"}},
        ],
    }))
    with pytest.raises(ValueError, match="not a prior append/delete/sql step"):
        load_workflow(str(path))


def test_valid_rollback_workflow(tmp_path):
    path = tmp_path / "wf.yaml"
    path.write_text(yaml.dump({
        "name": "rollback_test",
        "steps": [
            {"id": "app", "operation": "append", "config": {"backend": "pyspark", "num_rows": 5}},
            {"id": "del", "operation": "delete", "config": {"backend": "pyspark", "num_rows": 2}},
            {"id": "rb", "operation": "rollback", "config": {"backend": "pyspark", "to_step": "app"}},
        ],
    }))
    wf = load_workflow(str(path))
    assert wf.name == "rollback_test"
    assert len(wf.steps) == 3


def test_rollback_can_target_delete_step(tmp_path):
    path = tmp_path / "wf.yaml"
    path.write_text(yaml.dump({
        "name": "test",
        "steps": [
            {"id": "app", "operation": "append", "config": {"backend": "pyspark", "num_rows": 5}},
            {"id": "del", "operation": "delete", "config": {"backend": "pyspark", "num_rows": 2}},
            {"id": "rb", "operation": "rollback", "config": {"backend": "pyspark", "to_step": "del"}},
        ],
    }))
    wf = load_workflow(str(path))
    assert len(wf.steps) == 3


def test_compaction_spec_file():
    wf = load_workflow("specs/workflows/compaction.yaml")
    assert wf.name == "compact_append_delete_compact"
    assert len(wf.steps) == 4


def test_rollback_spec_file():
    wf = load_workflow("specs/workflows/rollback.yaml")
    assert wf.name == "append_delete_rollback"
    assert len(wf.steps) == 3


def test_valid_sql_workflow_with_setup(tmp_path):
    path = tmp_path / "wf.yaml"
    path.write_text(yaml.dump({
        "name": "test",
        "setup": {"backend": "pyspark", "statements": ["CREATE TABLE ${table} (id BIGINT) USING iceberg"]},
        "steps": [
            {"id": "s", "operation": "sql",
             "config": {"backend": "pyspark", "statements": ["INSERT INTO ${table} VALUES (1)"]}},
            {"id": "r", "operation": "rollback", "config": {"backend": "pyspark", "to_step": "s"}},
        ],
    }))
    workflow = load_workflow(str(path))
    assert workflow.setup["statements"] == ["CREATE TABLE ${table} (id BIGINT) USING iceberg"]
    assert [s["operation"] for s in workflow.steps] == ["sql", "rollback"]


def test_sql_missing_statements(tmp_path):
    path = tmp_path / "wf.yaml"
    path.write_text(yaml.dump({
        "name": "test",
        "steps": [{"id": "s", "operation": "sql", "config": {"backend": "pyspark"}}],
    }))
    with pytest.raises(ValueError, match="missing required config field 'statements'"):
        load_workflow(str(path))


def test_sql_statements_must_be_strings(tmp_path):
    path = tmp_path / "wf.yaml"
    path.write_text(yaml.dump({
        "name": "test",
        "steps": [{"id": "s", "operation": "sql", "config": {"backend": "pyspark", "statements": []}}],
    }))
    with pytest.raises(ValueError, match="non-empty list of SQL strings"):
        load_workflow(str(path))


def test_setup_missing_backend(tmp_path):
    path = tmp_path / "wf.yaml"
    path.write_text(yaml.dump({
        "name": "test",
        "setup": {"statements": ["SELECT 1"]},
        "steps": [{"id": "a", "operation": "append", "config": {"backend": "pyspark", "num_rows": 1}}],
    }))
    with pytest.raises(ValueError, match="setup missing required 'backend'"):
        load_workflow(str(path))


def test_replace_expect_values(tmp_path):
    path = tmp_path / "wf.yaml"
    for expect in REPLACE_EXPECTATIONS:
        path.write_text(yaml.dump({
            "name": "test",
            "steps": [{"id": "c", "operation": "replace", "config": {"command": "true", "expect": expect}}],
        }))
        assert load_workflow(str(path)).steps[0]["config"]["expect"] == expect


def test_replace_unknown_expect(tmp_path):
    path = tmp_path / "wf.yaml"
    path.write_text(yaml.dump({
        "name": "test",
        "steps": [{"id": "c", "operation": "replace", "config": {"command": "true", "expect": "maybe"}}],
    }))
    with pytest.raises(ValueError, match="has expect 'maybe'"):
        load_workflow(str(path))
