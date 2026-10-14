"""Integration tests for workflows that build their own table with setup and sql steps.

These tests require a Spark session with Iceberg support (JVM).
"""

import yaml

from iceberg_fuzzer.workflow import run_workflow


def _workflow(tmp_path, expect):
    path = tmp_path / f"{expect}.yaml"
    path.write_text(yaml.safe_dump({
        "name": f"sql_{expect}",
        "setup": {"backend": "pyspark", "statements": [
            "CREATE TABLE ${table} (id BIGINT) USING iceberg "
            "TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read')"]},
        "steps": [
            {"id": "write", "operation": "sql", "config": {"backend": "pyspark", "statements": [
                "INSERT INTO ${table} SELECT id FROM range(0, 20)",
                "DELETE FROM ${table} WHERE id = 7"]}},
            {"id": "noop", "operation": "replace", "config": {"command": "true", "expect": expect}},
        ],
    }))
    return str(path)


def _run(tmp_path, workflow_file, table_name):
    return run_workflow(
        workflow_file=workflow_file,
        warehouse_path=str(tmp_path / "warehouse"),
        table_name=table_name,
        namespace="sql_it",
        output_dir=str(tmp_path / "logs"),
        point_queries=False,
    )


def test_noop_replace_passes_when_no_snapshot_expected(tmp_path):
    result = _run(tmp_path, _workflow(tmp_path, "none"), "expect_none")
    assert result.passed, result
    assert {a.name for a in result.step_assertions["write"]} >= {"duckdb.row_count", "pyspark.row_count"}
    assert {a.name: a.passed for a in result.step_assertions["noop"]}["snapshot.unchanged"]


def test_noop_replace_fails_when_replace_expected(tmp_path):
    result = _run(tmp_path, _workflow(tmp_path, "replace"), "expect_replace")
    assert not result.passed
    checks = {a.name: a.passed for a in result.step_assertions["noop"]}
    assert checks["snapshot.new"] is False
    assert checks["snapshot.operation"] is False
    assert checks["pyspark.content_hash"]


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
    result = _run(tmp_path, str(path), "residual")
    assert result.passed, result
