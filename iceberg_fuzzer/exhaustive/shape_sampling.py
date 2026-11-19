"""Choosing which generated shape configs to build. Nothing builds the sample yet: `build-shapes`
still builds every config the generator yields.

A full run of the generator repeats each dependency structure in many configs. The sample keeps one
class per distinct dependency structure, the generator's own equivalence key: each data file's size
class and sentinel, which delete files name it and how much of it they delete. Within a class,
configs differ in their snapshot order, their value strategy, their manifest strategy and which of a
file's rows the deletes name. The sample takes, for each class, configs until every relation some
config of the class has is set. Most concern sequence numbers, by which a delete applies to a data
file committed in its own snapshot or an earlier one, the spec's rule for position deletes,
https://iceberg.apache.org/spec/#scan-planning:

  - delete_with_target: a delete file committed with a data file it names, so their sequence
    numbers are equal and the delete applies.
  - delete_after_target: a delete file committed after a data file it names.
  - delete_before_target: a delete file committed before a data file it names, which must not apply
    to it; only a witness holds one.
  - file_mixed_sequence: a data file one delete applies to and another delete naming it does not.
  - delete_spans_sequence: a delete file that applies to one data file it names and not to another.
  - sentinel_delete_after_rewritten_delete: a delete on the sentinel, which a compaction skips,
    committed after a delete on a file it rewrites.
  - sentinel_with_rewritten_file: the sentinel committed with a file a compaction rewrites, so they
    share a manifest the compaction rewrites.
  - duplicate_entry: a delete file deleting the same row twice.

and three relations of the rows a built file holds, read only on files the builder writes with the
config's rows alone, so never on the sentinel, which it pads with rows the config does not list,
and only from deletes that apply:

  - entries_reach_row_count: a delete file with as many entries for a data file as the file has
    rows, some of them repeated, while a row of the file stays live after every delete, which a
    planner counting entries takes for a fully deleted file.
  - union_deletes_file: a data file every row of which is deleted, though no single delete file
    deletes them all.
  - last_row_deleted: a delete on the last row of a data file of two or more.

The sample stops taking a class's configs by relations alone. Value strategies enter only through
the tie break below, by the pairs of ordering features a config adds, so a class need not hold each
of its value strategies. Manifest strategies do not enter at all: a config with the maximal one ties
its minimal twin on everything the sample reads and has the higher config id.

Ties go to the config whose ordering features pair with the most values the sample has not yet
paired, then to the lowest config id, so the sample is deterministic for a seed.

The generator commits every delete file at or after the data files it names, so no generated
config holds delete_before_target. The sample then takes its witnesses from a second pass of the
generator whose orderings may commit a delete file before a data file it names, see witnesses.
"""

from collections import defaultdict
from itertools import combinations
from typing import Any, Dict, FrozenSet, Iterable, List, Tuple

from .shape_config import committed_snapshots, file_commits, padded_file_ids
from .shape_config_generator import TableConfigGenerator, equivalence_key

# Features of a config's snapshot order and values, paired across the sample.
ORDERING_FEATURES = ("value_strategy", "snapshots", "data_commits", "sentinel_commit", "delete_commits")
# The relations of a delete committed before a data file it names, the most of which a witness holds.
EARLY_RELATIONS = ("delete_before_target", "file_mixed_sequence", "delete_spans_sequence")


def class_key(config: Dict[str, Any]) -> Tuple:
    """The generator's equivalence key, with whether the config has a sentinel,
    since its configs without one share keys with those that have one."""
    sentinel = config.get('sentinel_in_file') is not None
    return (sentinel,) + equivalence_key(config)


def relations(config: Dict[str, Any]) -> FrozenSet[str]:
    snap = file_commits(config)
    sentinel = config.get('sentinel_in_file')
    found = set()
    sentinel_delete_snaps, rewritten_delete_snaps = [], []
    applying: Dict[str, set] = defaultdict(set)
    for delete_file in config['delete_files']:
        applies = {}
        for d in delete_file['deletes']:
            if snap[delete_file['file_id']] == snap[d['data_file']]:
                found.add("delete_with_target")
            elif snap[delete_file['file_id']] > snap[d['data_file']]:
                found.add("delete_after_target")
            else:
                found.add("delete_before_target")
            applies[d['data_file']] = snap[delete_file['file_id']] >= snap[d['data_file']]
            (sentinel_delete_snaps if d['data_file'] == sentinel else rewritten_delete_snaps).append(
                snap[delete_file['file_id']])
        row_ids = [d['row_id'] for d in delete_file['deletes']]
        if len(row_ids) != len(set(row_ids)):
            found.add("duplicate_entry")
        if len(set(applies.values())) == 2:
            found.add("delete_spans_sequence")
        for data_file, applied in applies.items():
            applying[data_file].add(applied)
    if any(len(applied) == 2 for applied in applying.values()):
        found.add("file_mixed_sequence")
    if sentinel_delete_snaps and rewritten_delete_snaps and max(sentinel_delete_snaps) > min(rewritten_delete_snaps):
        found.add("sentinel_delete_after_rewritten_delete")
    if sentinel and any(snap[f['file_id']] == snap[sentinel] for f in config['data_files'] if f['file_id'] != sentinel):
        found.add("sentinel_with_rewritten_file")
    return frozenset(found | _row_relations(config, snap))


def _row_relations(config: Dict[str, Any], snap: Dict[str, int]) -> set:
    """entries_reach_row_count, union_deletes_file and last_row_deleted, see the module docstring."""
    padded = set(padded_file_ids(config))
    rows_of = {f['file_id']: {r['row_id'] for r in f['rows']} for f in config['data_files'] if f['file_id'] not in padded}
    last_row = {f['file_id']: max(f['rows'], key=lambda r: r['position'])['row_id']
                for f in config['data_files'] if f['file_id'] in rows_of and len(f['rows']) > 1}
    found = set()
    deleted: Dict[str, set] = defaultdict(set)
    deleted_by_one, reaching = set(), set()
    for delete_file in config['delete_files']:
        entries: Dict[str, List[int]] = defaultdict(list)
        for d in delete_file['deletes']:
            if d['data_file'] in rows_of and snap[delete_file['file_id']] >= snap[d['data_file']]:
                entries[d['data_file']].append(d['row_id'])
        for data_file, row_ids in entries.items():
            deleted[data_file].update(row_ids)
            if last_row.get(data_file) in row_ids:
                found.add("last_row_deleted")
            if set(row_ids) == rows_of[data_file]:
                deleted_by_one.add(data_file)
            if len(row_ids) >= len(rows_of[data_file]) > len(set(row_ids)):
                reaching.add(data_file)
    if any(deleted[f] != rows_of[f] for f in reaching):
        found.add("entries_reach_row_count")
    if any(row_ids == rows_of[f] and f not in deleted_by_one for f, row_ids in deleted.items()):
        found.add("union_deletes_file")
    return found


def _bucket(count: int) -> str:
    return str(count) if count < 3 else "3+"


def _ordering_features(config: Dict[str, Any]) -> Dict[str, Any]:
    """The config's value of each of ORDERING_FEATURES."""
    snap = file_commits(config)
    sentinel = config.get('sentinel_in_file')
    targets = [{d['data_file'] for d in f['deletes']} for f in config['delete_files']]
    data_snaps = sorted({snap[f['file_id']] for f in config['data_files']})
    delete_snaps = sorted({snap[f['file_id']] for f in config['delete_files']})
    commits = len(committed_snapshots(config))
    if not sentinel:
        sentinel_commit = "none"
    elif commits == 1:
        sentinel_commit = "only"
    else:
        position = snap[sentinel]
        sentinel_commit = "first" if position == 0 else "last" if position == commits - 1 else "middle"
    if not config['delete_files']:
        delete_commits = "none"
    elif all(snap[f['file_id']] == max(snap[t] for t in ts) for f, ts in zip(config['delete_files'], targets)):
        delete_commits = "with_last_target"
    elif min(delete_snaps) > max(data_snaps):
        delete_commits = "after_all_data"
    else:
        delete_commits = "mixed"
    return {
        "value_strategy": config['value_strategy'],
        "snapshots": _bucket(commits),
        "data_commits": "one" if len(data_snaps) == 1 else "several",
        "sentinel_commit": sentinel_commit,
        "delete_commits": delete_commits,
    }


def _ordering_pairs(features: Dict[str, Any]) -> FrozenSet[Tuple]:
    values = [(name, features[name]) for name in ORDERING_FEATURES]
    return frozenset(combinations(values, 2))


def select_configs(configs: Iterable[Dict[str, Any]]) -> List[int]:
    """Config ids of the sample: for each class, in the order its first config
    is generated, configs until every relation the class has is set."""
    classes: Dict[Tuple, List[Tuple[int, FrozenSet[str], FrozenSet[Tuple]]]] = {}
    for config in configs:
        classes.setdefault(class_key(config), []).append(
            (config['config_id'], relations(config), _ordering_pairs(_ordering_features(config))))
    chosen: List[int] = []
    paired: set = set()
    for members in classes.values():
        uncovered = set().union(*(r for _, r, _ in members))
        picked_any = False
        while uncovered or not picked_any:
            config_id, rels, pairs = min(
                members, key=lambda m: (-len(m[1] & uncovered), -len(m[2] - paired), m[0]))
            chosen.append(config_id)
            uncovered -= rels
            paired |= pairs
            picked_any = True
    return chosen


def witnesses(max_row_count: int, seed: int, first_id: int) -> List[Dict[str, Any]]:
    """For each dependency class, see class_key, whose config in the generator's pass under
    deletes_before_data is eligible, holding delete_before_target and committing every delete file
    at or after its first data commit, the class's eligible config holding the most of
    EARLY_RELATIONS, the first generated on a tie, renumbered from first_id, in the order the classes
    first hold one. Holds one config a class while the pass streams.

    A delete file committed before every data file would not last: at a later commit Iceberg Java
    drops delete files older than every live data file, MergingSnapshotProducer.apply,
    https://github.com/apache/iceberg/blob/apache-iceberg-1.10.0/core/src/main/java/org/apache/iceberg/MergingSnapshotProducer.java#L928-L937.
    Only the main pipeline's orderings read deletes_before_data, so only classes with a sentinel
    can get a witness, and of those only the ones whose pass holds an eligible config: none with no
    delete file or with one data file can."""
    found: Dict[Tuple, Tuple[int, Dict[str, Any]]] = {}
    for config in TableConfigGenerator(max_row_count=max_row_count, seed=seed, deletes_before_data=True).generate_all():
        commits = file_commits(config)
        first = min(commits[f['file_id']] for f in config['data_files'])
        held = relations(config) & set(EARLY_RELATIONS)
        if "delete_before_target" not in held or any(commits[f['file_id']] < first for f in config['delete_files']):
            continue
        key = class_key(config)
        if key not in found or len(held) > found[key][0]:
            found[key] = (len(held), config)
    return [{**config, 'config_id': first_id + position} for position, (_, config) in enumerate(found.values())]


def generate_selected(max_row_count: int = 6, seed: int = 42) -> List[Dict[str, Any]]:
    """The sampled configs, in the order select_configs picks them, then their witnesses, see
    witnesses, numbered past the generator's last config id. Generates the configs twice rather than
    holding every one in memory."""
    generator = TableConfigGenerator(max_row_count=max_row_count, seed=seed)
    ids = select_configs(generator.generate_all())
    wanted = set(ids)
    found = {c['config_id']: c for c in TableConfigGenerator(max_row_count=max_row_count, seed=seed).generate_all()
             if c['config_id'] in wanted}
    return [found[i] for i in ids] + witnesses(max_row_count, seed, generator.config_count + 1)
