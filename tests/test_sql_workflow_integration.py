"""Integration tests for workflows that build their own table with setup and sql steps.

These tests require a Spark session with Iceberg support (JVM).
"""

import yaml

from iceberg_fuzzer.workflow import run_workflow

CREATE = (
    "CREATE TABLE ${table} (id BIGINT, name STRING, note STRING) USING iceberg "
    "TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read')"
)
WRITE = {"id": "write", "operation": "sql", "config": {"backend": "pyspark", "statements": [
    "INSERT INTO ${table} SELECT id, CONCAT('n', id), 'x' FROM range(0, 20)",
    "DELETE FROM ${table} WHERE id = 7"]}}


def _workflow(tmp_path, name, steps):
    path = tmp_path / f"{name}.yaml"
    path.write_text(yaml.safe_dump({
        "name": name,
        "setup": {"backend": "pyspark", "statements": [CREATE]},
        "steps": steps,
    }))
    return str(path)


def _noop(expect, command="true", timeout=300):
    return {"id": "noop", "operation": "replace",
            "config": {"command": command, "expect": expect, "timeout": timeout}}


def _run(tmp_path, workflow_file, table_name, point_queries=False):
    return run_workflow(
        workflow_file=workflow_file,
        warehouse_path=str(tmp_path / "warehouse"),
        table_name=table_name,
        namespace="sql_it",
        output_dir=str(tmp_path / "logs"),
        point_queries=point_queries,
        seed=1,
    )


def _checks(result, step_id):
    return {a.name: a.passed for a in result.step_assertions[step_id]}


def test_noop_replace_passes_when_no_snapshot_expected(tmp_path):
    result = _run(tmp_path, _workflow(tmp_path, "none", [WRITE, _noop("none")]), "expect_none")
    assert result.passed, result
    assert _checks(result, "write").keys() >= {"duckdb.row_count", "pyspark.row_count"}
    assert _checks(result, "noop")["snapshot.unchanged"]


def test_noop_replace_fails_when_replace_expected(tmp_path):
    result = _run(tmp_path, _workflow(tmp_path, "replace", [WRITE, _noop("replace")]), "expect_replace")
    assert not result.passed
    checks = _checks(result, "noop")
    assert checks["snapshot.new"] is False
    assert checks["snapshot.operation"] is False
    assert checks["pyspark.content_hash"]


def test_timed_out_command_fails_even_when_unchanged_expected(tmp_path):
    workflow = _workflow(tmp_path, "timeout", [WRITE, _noop("unchanged", command="sleep 30", timeout=1)])
    result = _run(tmp_path, workflow, "timeout")
    assert "timed out" in result.error


def test_setup_failure_is_reported_not_raised(tmp_path):
    workflow = _workflow(tmp_path, "rerun", [WRITE])
    assert _run(tmp_path, workflow, "rerun").passed
    result = _run(tmp_path, workflow, "rerun")
    assert result.error.startswith("Workflow setup failed")


def test_unknown_backend_is_reported_not_raised(tmp_path):
    path = tmp_path / "backend.yaml"
    path.write_text(yaml.safe_dump({
        "name": "backend",
        "setup": {"backend": "nope", "statements": [CREATE]},
        "steps": [WRITE],
    }))
    result = _run(tmp_path, str(path), "backend")
    assert result.error == "Workflow setup failed: Unknown backend: nope"


def test_statements_keep_literal_dollars_and_fill_table_parts(tmp_path):
    step = {"id": "write", "operation": "sql", "config": {"backend": "pyspark", "statements": [
        "INSERT INTO ${table} SELECT id, CONCAT('n', id), 'x' FROM range(0, 20)",
        "DELETE FROM ${table} WHERE name RLIKE '^n1$'",
        "DELETE FROM ${table} WHERE note = '$$x'",
        "CALL local.system.rewrite_data_files(table => '${namespace}.${table_name}')"]}}
    result = _run(tmp_path, _workflow(tmp_path, "dollars", [step]), "dollars")
    assert result.passed, result


def test_point_queries_skip_sql_written_tables(tmp_path):
    step = {"id": "write", "operation": "sql", "config": {"backend": "pyspark", "statements": [
        "ALTER TABLE ${table} ADD COLUMN qty INT",
        "INSERT INTO ${table} SELECT id, 'n', 'x', 5 FROM range(0, 20)"]}}
    result = _run(tmp_path, _workflow(tmp_path, "qty", [step]), "qty", point_queries=True)
    assert result.passed, result
    assert not any(name.startswith("point_query") for name in _checks(result, "write"))


def test_compaction_keeps_a_delete_file_in_an_untouched_partition(tmp_path):
    path = tmp_path / "residual.yaml"
    path.write_text(yaml.safe_dump({
        "name": "residual",
        "setup": {"backend": "pyspark", "statements": [
            "CREATE TABLE ${table} (id BIGINT, region STRING) USING iceberg PARTITIONED BY (region) "
            "TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read')"]},
        "steps": [
            {"id": "write", "operation": "sql", "config": {"backend": "pyspark", "statements": [
                "INSERT INTO ${table} SELECT /*+ COALESCE(1) */ id, IF(id < 1000, 'us', 'eu') FROM range(0, 1010)",
                *[f"INSERT INTO ${{table}} SELECT /*+ COALESCE(1) */ id, 'eu' FROM range({start}, {start + 10})"
                  for start in range(1010, 1060, 10)],
                "DELETE FROM ${table} WHERE id = 7"]}},
            {"id": "compact", "operation": "replace", "config": {
                "command": "python3 scripts/example-pyspark-compact.py --warehouse ${warehouse} "
                           "--namespace ${namespace} --table-name ${table_name}",
                "expect": "replace", "timeout": 600}},
        ],
    }))
    result = _run(tmp_path, str(path), "residual", point_queries=True)
    assert result.passed, result
