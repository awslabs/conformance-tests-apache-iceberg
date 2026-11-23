"""Unit tests for shape sampling. No Spark."""

import subprocess
import sys
from collections import defaultdict

from iceberg_fuzzer.exhaustive.shape_config import file_commits
from iceberg_fuzzer.exhaustive.shape_config_generator import TableConfigGenerator
from iceberg_fuzzer.exhaustive.shape_sampling import (
    EARLY_RELATIONS, class_key, generate_selected, relations, select_configs, witnesses)

ROW_RELATIONS = {"entries_reach_row_count", "union_deletes_file", "last_row_deleted"}


def _rows(*row_ids):
    return [{'row_id': r, 'value': r, 'position': p} for p, r in enumerate(row_ids)]


def _delete(row_id, data_file, position):
    return {'row_id': row_id, 'data_file': data_file, 'data_file_index': int(data_file[-1]), 'position': position}


def _config(snapshots, deletes=(), sentinel=None, files=((0, 1), (2,), (3,), (4,))):
    """Data files data_f0.. holding the given row ids, and delete files del_f0.. each with the given entries."""
    return {
        'config_id': 1, 'row_count': sum(len(f) for f in files), 'delete_pattern': ['i'] * sum(len(d) for d in deletes),
        'value_strategy': 'row_numbers', 'sentinel_in_file': sentinel,
        'data_files': [{'file_id': f'data_f{i}', 'file_index': i, 'rows': _rows(*rows)} for i, rows in enumerate(files)],
        'delete_files': [{'file_id': f'del_f{i}', 'file_index': i, 'deletes': list(d)} for i, d in enumerate(deletes)],
        'snapshots': [{'snapshot_id': i, 'data_files': list(d), 'delete_files': list(x)} for i, (d, x) in enumerate(snapshots)],
    }


def _delete_before_first_data_commit(config):
    commits = file_commits(config)
    first = min(commits[f['file_id']] for f in config['data_files'])
    return any(commits[f['file_id']] < first for f in config['delete_files'])


ALL_DATA = ['data_f0', 'data_f1', 'data_f2', 'data_f3']


class TestSampling:
    def test_relations_of_a_layout(self):
        # data_f0 is the sentinel, which the builder pads past its two rows, so deleting both deletes
        # neither every row of it nor its last.
        config = _config([(['data_f0', 'data_f1'], ['del_f0']), (['data_f2', 'data_f3'], ['del_f1'])],
                         [[_delete(0, 'data_f0', 0)], [_delete(1, 'data_f0', 1)]], sentinel='data_f0')
        assert relations(config) == {"delete_with_target", "delete_after_target", "sentinel_with_rewritten_file"}
        config['sentinel_in_file'] = 'data_f1'
        assert {"union_deletes_file", "last_row_deleted"} <= relations(config)
        config['sentinel_in_file'] = 'data_f0'
        config['delete_files'][0]['deletes'] = [_delete(2, 'data_f1', 0)]
        assert "sentinel_delete_after_rewritten_delete" in relations(config)

    def test_a_fully_covered_padded_sentinel_sets_no_row_relation(self):
        # Two delete files cover the sentinel's two config rows, one of them twice, but a padding row
        # the config does not list stays live.
        config = _config([(ALL_DATA, ['del_f0', 'del_f1'])],
                         [[_delete(0, 'data_f0', 0), _delete(0, 'data_f0', 0)], [_delete(1, 'data_f0', 1)]],
                         sentinel='data_f0')
        assert "duplicate_entry" in relations(config) and not relations(config) & ROW_RELATIONS

    def test_entries_reaching_a_row_count_need_a_row_live_after_every_delete(self):
        # Pattern i, i, j on four rows in two files of two, split into {i, i} and {j}: both i entries
        # on row 0 and j on row 1 of data_f0, which is not the sentinel.
        config = _config([(['data_f0', 'data_f1'], ['del_f0', 'del_f1'])],
                         [[_delete(0, 'data_f0', 0), _delete(0, 'data_f0', 0)], [_delete(1, 'data_f0', 1)]],
                         sentinel='data_f1', files=((0, 1), (2, 3)))
        assert "entries_reach_row_count" not in relations(config)
        assert {"union_deletes_file", "last_row_deleted"} <= relations(config)

    def test_a_delete_that_does_not_apply_sets_no_row_relation(self):
        config = _config([(['data_f1', 'data_f2', 'data_f3'], ['del_f0', 'del_f1']), (['data_f0'], [])],
                         [[_delete(0, 'data_f0', 0), _delete(0, 'data_f0', 0)], [_delete(1, 'data_f0', 1)]])
        assert "delete_before_target" in relations(config) and not relations(config) & ROW_RELATIONS

    def test_a_delete_committed_before_the_file_it_names(self):
        config = _config([(['data_f0', 'data_f1'], ['del_f0']), (['data_f2', 'data_f3'], [])], [[_delete(3, 'data_f2', 0)]])
        assert relations(config) & {"delete_with_target", "delete_after_target", "delete_before_target"} == {"delete_before_target"}

    def test_duplicate_entries_that_reach_a_files_row_count(self):
        config = _config([(ALL_DATA, ['del_f0'])], [[_delete(0, 'data_f0', 0), _delete(0, 'data_f0', 0)]])
        assert {"duplicate_entry", "entries_reach_row_count"} <= relations(config)
        assert "union_deletes_file" not in relations(config) and "last_row_deleted" not in relations(config)

    def test_class_keys_tell_configs_without_a_sentinel_apart(self):
        config = _config([(ALL_DATA, [])], sentinel='data_f0')
        without = {**config, 'sentinel_in_file': None}
        assert class_key(config) != class_key(without)

    def test_each_class_gets_configs_until_every_relation_it_has_is_set(self):
        deletes = [[_delete(0, 'data_f0', 0)]]
        same = {**_config([(ALL_DATA, ['del_f0'])], deletes), 'config_id': 1}
        later = {**_config([(ALL_DATA, []), ([], ['del_f0'])], deletes), 'config_id': 2}
        also_later = {**_config([(ALL_DATA, []), ([], ['del_f0'])], deletes), 'config_id': 3}
        assert select_configs([same, later, also_later]) == [1, 2]

    def test_the_sample_takes_every_class_of_small_tables(self):
        configs = list(TableConfigGenerator(max_row_count=3, seed=42).generate_all())
        chosen = set(select_configs(configs))
        assert {class_key(c) for c in configs} == {class_key(c) for c in configs if c['config_id'] in chosen}
        for key in {class_key(c) for c in configs}:
            members = [c for c in configs if class_key(c) == key]
            assert set().union(*(relations(c) for c in members)) == set().union(
                *(relations(c) for c in members if c['config_id'] in chosen))

    def test_reading_a_sample_imports_no_spark(self):
        imports = "import sys, iceberg_fuzzer.exhaustive.shape_sampling; print('pyspark' in sys.modules)"
        read = subprocess.run([sys.executable, "-c", imports], capture_output=True, text=True, check=True)
        assert read.stdout.split()[-1] == "False"


class TestWitnesses:
    def test_the_generator_commits_a_delete_before_a_data_file_it_names_only_under_deletes_before_data(self):
        assert not any("delete_before_target" in relations(c) for c in TableConfigGenerator(max_row_count=3, seed=42).generate_all())
        assert any("delete_before_target" in relations(c)
                   for c in TableConfigGenerator(max_row_count=3, seed=42, deletes_before_data=True).generate_all())

    def test_the_sample_is_its_picks_then_its_witnesses_numbered_past_the_generator_s_last_config(self):
        generator = TableConfigGenerator(max_row_count=3, seed=42)
        picks = select_configs(generator.generate_all())
        sample = generate_selected(3, 42)
        assert [c['config_id'] for c in sample[:len(picks)]] == picks
        found, first = sample[len(picks):], generator.config_count + 1
        assert found and [c['config_id'] for c in found] == list(range(first, first + len(found)))

    def test_each_witness_holds_the_most_early_relations_of_its_class_and_no_delete_before_the_first_data_commit(self):
        most = defaultdict(int)
        for config in TableConfigGenerator(max_row_count=3, seed=42, deletes_before_data=True).generate_all():
            held = relations(config) & set(EARLY_RELATIONS)
            if "delete_before_target" in held and not _delete_before_first_data_commit(config):
                most[class_key(config)] = max(most[class_key(config)], len(held))
        found = witnesses(3, 42, 1)
        assert len(found) == len({class_key(w) for w in found}) == len(most) and {class_key(w) for w in found} == set(most)
        for witness in found:
            held = relations(witness) & set(EARLY_RELATIONS)
            assert "delete_before_target" in held and not _delete_before_first_data_commit(witness)
            assert len(held) == most[class_key(witness)] and witness['sentinel_in_file']
        assert set().union(*(relations(w) for w in found)) >= set(EARLY_RELATIONS)
