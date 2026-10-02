import random
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

from iceberg_fuzzer.cli import main
from iceberg_fuzzer.generator import TableGenerator


class SelectionGenerator(TableGenerator):
    def supports_s3_tables(self):
        return False

    def supports_partitioned_tables(self):
        return False

    def supports_minimal_mode(self):
        return False

    def generate_simple_tables(self, *args, **kwargs):
        return True

    def generate_write_pattern_tables(self, *args, **kwargs):
        return True

    def generate_partitioned_tables(self, *args, **kwargs):
        return True

    def generate_minimal_tables(self, *args, **kwargs):
        return True

    def cleanup(self):
        pass


def test_seed_replays_random_selection():
    specs = list(range(20))
    first = SelectionGenerator("", "", seed=31)
    second = SelectionGenerator("", "", seed=31)
    third = SelectionGenerator("", "", seed=32)

    selected = first._random_select_tables(specs, 6)
    assert selected == second._random_select_tables(specs, 6)
    assert selected != third._random_select_tables(specs, 6)
    assert len(selected) == 6


@pytest.mark.parametrize(
    ("backend", "generator_path", "random_count"),
    [
        ("pyspark", "iceberg_fuzzer.backend.pyspark.generator.PySparkGenerator", 6),
        ("pyiceberg", "iceberg_fuzzer.backend.pyiceberg.generator.PyIcebergGenerator", 6),
        ("polars", "iceberg_fuzzer.backend.polars.generator.PolarsGenerator", 6),
        ("pyspark", "iceberg_fuzzer.backend.pyspark.generator.PySparkGenerator", None),
    ],
)
def test_generate_cli_passes_seed(
    monkeypatch, tmp_path, capsys, backend, generator_path, random_count
):
    calls = {}

    class FakeGenerator:
        def __init__(self, warehouse_path, specs_path, seed):
            calls["seed"] = seed

        def generate_simple_tables(self, num_parallel, minimal, random_count, delete_existing):
            calls["random_count"] = random_count
            return True

        def cleanup(self):
            pass

    monkeypatch.setattr(generator_path, FakeGenerator)
    argv = [
        "iceberg-fuzzer",
        "generate",
        "--backend", backend,
        "--warehouse-path", str(tmp_path),
        "--specs", "specs/simple.yaml",
        "--seed", "31",
    ]
    if random_count is not None:
        argv.extend(["--random", str(random_count)])
    monkeypatch.setattr(sys, "argv", argv)

    main()

    assert calls == {"seed": 31, "random_count": random_count}
    warning = "Warning: --seed affects table selection only when --random is set."
    assert (warning in capsys.readouterr().err) == (random_count is None)


def test_evolution_cli_selection_replays_seed(monkeypatch, tmp_path):
    from iceberg_fuzzer.backend.pyspark import generator as pyspark_generator

    created = []

    def record_table(spark, warehouse_path, table_name, table_spec, delete_existing):
        created.append(table_name)
        return True

    monkeypatch.setattr(pyspark_generator, "create_spark_session", lambda _: Mock())
    monkeypatch.setattr("backend.pyspark.builders.create_evolution_table", record_table)
    specs_path = Path(__file__).resolve().parents[1] / "specs" / "evolution.yaml"
    selections = []
    for global_seed in (7, 99):
        monkeypatch.setattr(random, "sample", random.Random(global_seed).sample)
        created.clear()
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "iceberg-fuzzer",
                "generate",
                "--backend", "pyspark",
                "--warehouse-path", str(tmp_path / str(global_seed)),
                "--specs", str(specs_path),
                "--random", "3",
                "--seed", "42",
            ],
        )
        main()
        selections.append(created.copy())

    assert len(selections[0]) == 3
    assert selections[0] == selections[1]
