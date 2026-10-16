"""Unit tests for the seeded random SQL workflow generator."""

from iceberg_fuzzer.workflow.loader import load_workflow
from iceberg_fuzzer.workflow.random_sql import generate_workflow


def test_same_seed_gives_same_workflow():
    assert generate_workflow(7, "compact").to_yaml() == generate_workflow(7, "compact").to_yaml()
    assert generate_workflow(7, "compact").to_yaml() != generate_workflow(8, "compact").to_yaml()


def test_generated_workflows_load_and_end_with_a_compaction(tmp_path):
    for seed in range(200):
        path = tmp_path / f"{seed}.yaml"
        path.write_text(generate_workflow(seed, "compact", "spark").to_yaml())
        workflow = load_workflow(str(path))
        assert workflow.setup["statements"][0].startswith("CREATE TABLE ${table}")
        last = workflow.steps[-1]
        assert last["operation"] == "replace" and last["config"]["expect"] == "replace_or_unchanged"
        statements = "".join(s for step in workflow.steps for s in step["config"].get("statements", []))
        for placeholder in ("${table}", "${catalog}", "${namespace}", "${table_name}"):
            statements = statements.replace(placeholder, "")
        assert "$" not in statements


def test_generated_workflows_cover_every_partition_kind_and_nesting():
    dimensions = [generate_workflow(seed, "compact").dimensions for seed in range(300)]
    partitions = {d["partition"].split("(")[1] if "(" in d["partition"] else d["partition"] for d in dimensions}
    assert partitions >= {"none", "p_key)", "bucket", "truncate", "years", "months", "days", "hours"}
    assert any("STRUCT<a: INT, b: STRUCT" in d["columns"] for d in dimensions)
    assert any("MAP<" in d["columns"] for d in dimensions)


def test_pyiceberg_share_writes_single_insert_steps():
    from iceberg_fuzzer.workflow.random_sql import GeneratorConfig
    config = GeneratorConfig(pyiceberg_writer_share=1.0)
    steps = [s for seed in range(20) for s in generate_workflow(seed, "compact", config=config).definition["steps"]]
    writes = [s for s in steps if s["operation"] == "sql" and s["config"]["backend"] == "pyiceberg"]
    assert writes and all(len(s["config"]["statements"]) == 1 and s["config"]["statements"][0].startswith("INSERT INTO") for s in writes)


def test_pairwise_seeds_cover_every_pair_of_candidates():
    import itertools
    from iceberg_fuzzer.workflow.random_sql import pairwise_seeds
    candidates = range(300)
    chosen = pairwise_seeds(candidates)
    pairs = lambda seed: set(itertools.combinations(sorted(generate_workflow(seed, "compact", "spark").features.items()), 2))
    assert set().union(*(pairs(s) for s in chosen)) == set().union(*(pairs(s) for s in candidates))
    assert len(chosen) < 300 and chosen == pairwise_seeds(candidates)


def test_filters_are_deterministic_and_leave_the_workflow_alone():
    first, again = generate_workflow(7, "rust", "spark"), generate_workflow(7, "rust", "spark")
    assert first.filters == again.filters and first.filters[0].startswith("id >= ")
    assert {len(generate_workflow(seed, "rust", "spark").filters) for seed in range(40)} <= {1, 2}


def test_compactions_feature_keeps_intermediate_rust_compactions():
    for seed in range(200):
        generated = generate_workflow(seed, "rust", "spark")
        replaces = [s for s in generated.definition["steps"] if s["operation"] == "replace"]
        if sum(s["config"]["command"] == "rust" for s in replaces[:-1]):
            assert "COMPACT" in generated.features["compactions"].split("+")
            return
    raise AssertionError("no seed in 0 to 199 has an intermediate Rust compaction")


def test_schema_changes_keep_inserts_aligned_with_the_schema():
    from iceberg_fuzzer.workflow.random_sql import _schema_change, _columns, GeneratorConfig
    import random

    rng = random.Random(1)
    columns = _columns(rng, GeneratorConfig())
    for commit in range(1, 30):
        statement, description, after = _schema_change(rng, columns, commit, GeneratorConfig())
        assert statement.startswith("ALTER TABLE ${table} ")
        assert after[0] == columns[0]
        if description.startswith("add"):
            assert len(after) == len(columns) + 1 and after[-1].nullable
        elif description.startswith("drop"):
            assert len(after) == len(columns) - 1 and not any(c.partition_source for c in columns if c not in after)
        else:
            assert len(after) == len(columns)
        columns = after


def test_some_seeds_change_the_schema_and_rewrite_manifests():
    features = [generate_workflow(seed, "rust", "spark").features for seed in range(1, 60)]
    kinds = {kind for f in features for kind in f["schema_changes"].split("+")}
    assert {"add", "rename", "promote", "drop", "none"} <= kinds
    assert any(f["manifests_rewritten"] == "True" for f in features)


def test_every_seed_generates():
    from iceberg_fuzzer.workflow.random_sql import GeneratorConfig
    config = GeneratorConfig(deep_nesting_share=0.0, pyiceberg_writer_share=0.3, duckdb_writer_share=0.2)
    for seed in range(3000):
        generate_workflow(seed, "rust", "spark", config)


def test_a_dimension_draws_from_its_own_stream():
    from dataclasses import replace
    from iceberg_fuzzer.workflow.random_sql import GeneratorConfig

    def without(events, kind):
        return [e for e in events.split(", ") if not e.startswith(kind)]

    base = GeneratorConfig(pyiceberg_writer_share=0.3, duckdb_writer_share=0.2)
    for seed in range(50):
        with_updates = generate_workflow(seed, "rust", "spark", base)
        no_updates = generate_workflow(seed, "rust", "spark", replace(base, update_share=0.0))
        assert with_updates.dimensions["columns"] == no_updates.dimensions["columns"]
        assert without(with_updates.dimensions["workflow"], "UPDATE") == without(no_updates.dimensions["workflow"], "UPDATE")
