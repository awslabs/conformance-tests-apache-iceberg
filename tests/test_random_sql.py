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
        assert "$" not in "".join(s for step in workflow.steps for s in step["config"].get("statements", [])).replace("${table}", "")


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
