#!/usr/bin/env python3
"""
Table Configuration Generator - Main Implementation

Implements 6-level generation pipeline:
L1: Row count (1-6)
L2: Delete patterns (7 data-independent patterns)
L3: File partitioning (Bell number partitions)
L4: Value assignment (3 variants)
L5: Snapshot ordering with constraints
L6: Manifest structure (minimal/maximal)
"""

from typing import Dict, List, Iterator, Set, Optional, Any
import copy
import logging
import random
from enum import Enum
from .combinatorics import (
    generate_set_partitions,
    generate_ordered_partitions,
    compute_file_sizes
)

logger = logging.getLogger(__name__)


class CoverageType(Enum):
    """How much of a data file is deleted by a delete file."""
    NONE = 0
    PARTIAL = 1
    FULL = 2


def compute_data_partition_signature(partition: List[List[int]], row_count: int) -> tuple:
    """
    Compute equivalence class signature for a data partition.

    Two partitions are equivalent if:
    1. Same multiset of file sizes
    2. Sentinel row is in a file of the same size in both

    Args:
        partition: List of row subsets (e.g., [[0,1,2], [3,4,5]])
        row_count: Total number of rows

    Returns:
        Tuple: (sorted_file_sizes_tuple, sentinel_file_size)
    """
    sentinel_row = row_count // 2

    # Compute file sizes
    file_sizes = [len(subset) for subset in partition]

    # Find which file contains the sentinel
    sentinel_file_size = None
    for subset in partition:
        if sentinel_row in subset:
            sentinel_file_size = len(subset)
            break

    # Sort file sizes for canonical form
    sorted_sizes = tuple(sorted(file_sizes))

    return (sorted_sizes, sentinel_file_size)


def group_data_partitions_by_equivalence(partitions: List[List[List[int]]],
                                         row_count: int) -> Dict[tuple, List[List[List[int]]]]:
    """
    Group data partitions into equivalence classes.

    Args:
        partitions: All Bell number partitions for row_count
        row_count: Number of rows

    Returns:
        Dictionary mapping signature -> list of partitions in that class
    """
    equivalence_classes = {}

    for partition in partitions:
        sig = compute_data_partition_signature(partition, row_count)
        if sig not in equivalence_classes:
            equivalence_classes[sig] = []
        equivalence_classes[sig].append(partition)

    return equivalence_classes


def pick_representative(partitions: List[List[List[int]]]) -> List[List[int]]:
    """
    Pick one representative from an equivalence class.

    Strategy: Choose lexicographically smallest partition.

    Args:
        partitions: List of partitions in same equivalence class

    Returns:
        The representative partition
    """
    # Convert to tuples for comparison, then back
    as_tuples = [tuple(tuple(subset) for subset in p) for p in partitions]
    representative = min(as_tuples)
    return [list(subset) for subset in representative]


class EquivalenceTracker:
    """
    Tracks equivalence classes for L1+L2+L3+L4 configs.

    Determines which configs are representatives (get exhaustive L5)
    vs non-representatives (get sampled L5).

    Equivalence relation includes:
    - Dependency structure (delete-data relationships)
    - Coverage types (NONE, PARTIAL, FULL)
    - File sizes (SMALL vs LARGE based on sentinel row)

    Sentinel row strategy:
    - One sentinel per row_count: sentinel = row_count // 2
    - File containing sentinel is marked LARGE (conceptually target_size rows)
    - Other files are SMALL (original size)
    - This tests compaction handling of non-compacted (large) files
    """

    def __init__(self):
        self.seen_classes = {}  # class_key → representative_config_id
        self.representative_count = 0
        self.non_representative_count = 0

    def is_representative(self, config: Dict[str, Any]) -> bool:
        """
        Check if this config is the first in its equivalence class.

        Returns True if representative (should get exhaustive L5).
        """
        key = equivalence_key(config)

        if key not in self.seen_classes:
            self.seen_classes[key] = config.get('config_id', 'unknown')
            self.representative_count += 1
            return True
        else:
            self.non_representative_count += 1
            return False


def equivalence_key(config: Dict[str, Any]) -> tuple:
    """
    Compute equivalence class key based on dependency structure,
    scoped within the L3 equivalence class.

    Key includes:
    - L3 equivalence class (file size signature) to prevent cross-L3 collapsing
    - num_data, num_delete
    - For each data file: (referenced_by, coverage, is_large)
    - For each delete file: frozenset[(data_idx, coverage, is_large)]

    The is_large field tracks whether a file contains the sentinel row.
    The L3 scoping ensures that configs from different L3 representatives
    each independently select their own content representatives.
    """
    num_data = len(config['data_files'])
    num_delete = len(config['delete_files'])

    # Include L3 equivalence class to scope within L3
    l3_equiv_class = config.get('data_partition_equiv_class', '')

    # Compute sentinel row
    sentinel_row = config['row_count'] // 2

    # Compute is_large for each data file
    is_large_map = {}
    for data_idx, data_file in enumerate(config['data_files']):
        has_sentinel = any(row['row_id'] == sentinel_row for row in data_file['rows'])
        is_large_map[data_idx] = has_sentinel

    # Build data → delete mapping
    data_to_delete = []
    for data_idx, data_file in enumerate(config['data_files']):
        referenced_by = set()
        max_coverage = CoverageType.NONE

        # Check each delete file
        for del_idx, del_file in enumerate(config['delete_files']):
            cov = _coverage(del_file, data_file)
            if cov != CoverageType.NONE:
                referenced_by.add(del_idx)
                if cov.value > max_coverage.value:
                    max_coverage = cov

        is_large = is_large_map[data_idx]
        data_to_delete.append((frozenset(referenced_by), max_coverage, is_large))

    # Build delete → data mapping
    delete_to_data = []
    for del_idx, del_file in enumerate(config['delete_files']):
        referenced_files = []

        for data_idx, data_file in enumerate(config['data_files']):
            cov = _coverage(del_file, data_file)
            if cov != CoverageType.NONE:
                is_large = is_large_map[data_idx]
                referenced_files.append((data_idx, cov, is_large))

        delete_to_data.append(frozenset(referenced_files))

    return (l3_equiv_class, num_data, num_delete, tuple(data_to_delete), tuple(delete_to_data))


def _coverage(delete_file: Dict[str, Any], data_file: Dict[str, Any]) -> CoverageType:
    """
    Compute how much of data_file is deleted by delete_file.

    Returns: NONE, PARTIAL, or FULL
    """
    data_rows = {row['row_id'] for row in data_file['rows']}
    deleted_rows = {delete['row_id'] for delete in delete_file['deletes']
                    if delete['data_file'] == data_file['file_id']}

    if not deleted_rows:
        return CoverageType.NONE
    elif deleted_rows == data_rows:
        return CoverageType.FULL
    else:
        return CoverageType.PARTIAL


class TableConfigGenerator:
    """
    Generator for table configurations with comprehensive test coverage.

    Yields complete configurations through a 6-level pipeline.
    """

    # L2: Data-independent delete patterns
    DELETE_PATTERNS = [
        [],                    # No deletes
        ['i'],                 # 1 delete
        ['i', 'i'],           # Same position twice
        ['i', 'j'],           # Two different positions
        ['i', 'i', 'i'],      # Same position three times
        ['i', 'i', 'j'],      # One position twice, another once
        ['i', 'j', 'k']       # Three different positions
    ]

    # Random sampling constants
    MAX_RANDOM_SNAPSHOTS = 4
    MAX_RANDOM_ORDERING_ATTEMPTS = 100

    def __init__(self, max_row_count: int = 6, seed: int = 42,
                 exhaustive_mode: bool = False, deletes_before_data: bool = False):
        """
        Initialize the generator.

        Args:
            max_row_count: Maximum number of data rows (1-6)
            seed: Random seed for reproducible L4b generation
            exhaustive_mode: If True, use exhaustive L5 for all configs.
                           If False (default), use equivalence-based approach.
            deletes_before_data: If True, the main pipeline's L5 lifts its
                           delete-after-data constraint, so an ordering may
                           commit a delete file before a data file it names.
                           The no-sentinel and single-row orderings do not
                           read it.
        """
        self.max_row_count = max_row_count
        self.config_count = 0
        self.random = random.Random(seed)
        self.exhaustive_mode = exhaustive_mode
        self.deletes_before_data = deletes_before_data
        self.equiv_tracker = EquivalenceTracker() if not exhaustive_mode else None

    def generate_all(self) -> Iterator[Dict[str, Any]]:
        """
        Main entry point: generate all configurations.

        Yields:
            Complete configuration dictionaries (with-sentinel + no-sentinel)
        """
        # With-sentinel configs (exhaustive)
        for config in self._level1_row_count():
            yield config

        # No-sentinel configs (sampled: one representative per equivalence class,
        # 3 snapshot orderings each)
        for config in self._generate_no_sentinel_configs():
            yield config

    def _no_sentinel_equiv_key(self, config: Dict[str, Any]) -> tuple:
        """Equivalence key for no-sentinel configs: only the delete-coverage
        pattern matters (not file sizes, since all files are small)."""
        num_data = len(config['data_files'])
        num_delete = len(config['delete_files'])

        data_to_delete = []
        for data_idx, data_file in enumerate(config['data_files']):
            referenced_by = set()
            max_coverage = 0
            for del_idx, del_file in enumerate(config['delete_files']):
                data_rows = {row['row_id'] for row in data_file['rows']}
                deleted_rows = {d['row_id'] for d in del_file['deletes']
                                if d['data_file'] == data_file['file_id']}
                if not deleted_rows:
                    cov = 0
                elif deleted_rows == data_rows:
                    cov = 2  # FULL
                else:
                    cov = 1  # PARTIAL
                if cov > 0:
                    referenced_by.add(del_idx)
                    max_coverage = max(max_coverage, cov)
            data_to_delete.append((frozenset(referenced_by), max_coverage))

        delete_to_data = []
        for del_idx, del_file in enumerate(config['delete_files']):
            referenced_files = []
            for data_idx, data_file in enumerate(config['data_files']):
                data_rows = {row['row_id'] for row in data_file['rows']}
                deleted_rows = {d['row_id'] for d in del_file['deletes']
                                if d['data_file'] == data_file['file_id']}
                if not deleted_rows:
                    cov = 0
                elif deleted_rows == data_rows:
                    cov = 2
                else:
                    cov = 1
                if cov > 0:
                    referenced_files.append((data_idx, cov))
            delete_to_data.append(frozenset(referenced_files))

        return (num_data, num_delete, tuple(data_to_delete), tuple(delete_to_data))

    def _generate_no_sentinel_configs(self) -> Iterator[Dict[str, Any]]:
        """
        Generate no-sentinel configs: one representative per equivalence class,
        3 snapshot orderings each.

        No-sentinel means all files are small (no padding). Tests compaction
        when it takes ALL files (nothing skipped).
        """
        # Collect one representative per equivalence class from the with-sentinel
        # configs (rerun the generator without yielding, just to find representatives)
        equiv_representatives = {}

        # Use a temporary generator to find representatives
        tmp_gen = TableConfigGenerator(
            max_row_count=self.max_row_count,
            seed=42,
            exhaustive_mode=self.exhaustive_mode
        )

        for config in tmp_gen._level1_row_count():
            if config['row_count'] == 1:
                continue  # row_count=1 always has sentinel = only file, skip
            key = self._no_sentinel_equiv_key(config)
            if key not in equiv_representatives:
                equiv_representatives[key] = config

        # For each representative, generate 3 snapshot orderings without sentinel
        # Dedup by physical signature (different equiv classes can collapse
        # to same physical table when sentinel is removed)
        seen = set()
        for key, config in equiv_representatives.items():
            orderings = self._generate_no_sentinel_orderings(config)
            for ordering in orderings:
                for value_strategy in ['row_numbers', 'nulls']:
                    config_copy = copy.deepcopy(config)
                    config_copy['snapshots'] = ordering
                    config_copy['sentinel_in_file'] = None
                    config_copy['value_strategy'] = value_strategy

                    # Assign values
                    if value_strategy == 'row_numbers':
                        for data_file in config_copy['data_files']:
                            for row in data_file['rows']:
                                row['value'] = row['row_id']
                    else:
                        row_count = config_copy['row_count']
                        all_row_ids = list(range(row_count))
                        num_nulls = max(1, self.random.randint(1, max(1, row_count // 2)))
                        null_rows = set(self.random.sample(all_row_ids, min(num_nulls, row_count)))
                        config_copy['null_rows'] = sorted(null_rows)
                        for data_file in config_copy['data_files']:
                            for row in data_file['rows']:
                                if row['row_id'] in null_rows:
                                    row['value'] = None
                                else:
                                    row['value'] = row['row_id']

                    for manifest_strategy in ['minimal', 'maximal']:
                        config_copy2 = copy.deepcopy(config_copy)
                        config_copy2['manifest_strategy'] = manifest_strategy

                        sig = self._physical_delete_signature(config_copy2)
                        if sig in seen:
                            continue
                        seen.add(sig)

                        self.config_count += 1
                        config_copy2['config_id'] = self.config_count
                        config_copy2['is_no_sentinel'] = True
                        yield config_copy2

    def _generate_no_sentinel_orderings(self, config: Dict[str, Any]) -> List[List[Dict[str, Any]]]:
        """
        Generate 3 snapshot orderings for a no-sentinel config:
        1. All files in one snapshot
        2. Max spread (each file in its own snapshot, deletes after their data)
        3. All data files in one snapshot, all delete files in a second
        """
        all_data_ids = [df['file_id'] for df in config['data_files']]
        all_del_ids = [df['file_id'] for df in config['delete_files']]

        orderings = []

        # 1. All in one snapshot
        orderings.append([{
            'snapshot_id': 0,
            'data_files': list(all_data_ids),
            'delete_files': list(all_del_ids)
        }])

        # 2. Max spread: each data file in its own snapshot, then each delete
        #    file in a snapshot of its own after them
        if len(all_data_ids) + len(all_del_ids) > 1:
            snapshots = []

            # Each data file gets its own snapshot
            for i, data_id in enumerate(all_data_ids):
                snapshots.append({
                    'snapshot_id': i,
                    'data_files': [data_id],
                    'delete_files': []
                })

            # Each delete file goes in a new snapshot after all its dependencies
            for del_id in all_del_ids:
                snapshots.append({
                    'snapshot_id': len(snapshots),
                    'data_files': [],
                    'delete_files': [del_id]
                })

            orderings.append(snapshots)

        # 3. Data in one snapshot, deletes in a second snapshot
        if all_del_ids:
            orderings.append([
                {'snapshot_id': 0, 'data_files': list(all_data_ids), 'delete_files': []},
                {'snapshot_id': 1, 'data_files': [], 'delete_files': list(all_del_ids)}
            ])

        return orderings

    def _enumerate_instantiations(self, pattern: List[str]) -> List[Dict[str, Optional[str]]]:
        """
        Enumerate all instantiation variants for a delete pattern.

        Each instantiation specifies which variable (if any) should be assigned
        the sentinel row.

        Args:
            pattern: Delete pattern like ['i', 'j', 'k']

        Returns:
            List of instantiation specs, each a dict mapping variable -> 'sentinel' or None
            Example for ['i', 'j']:
            [
                {'i': None, 'j': None},           # Both non-sentinel
                {'i': None, 'j': 'sentinel'},     # j is sentinel
                {'i': 'sentinel', 'j': None}      # i is sentinel
            ]
        """
        if not pattern:
            # No deletes, only one instantiation
            return [{}]

        # Get distinct variables
        distinct_vars = sorted(set(pattern))
        num_vars = len(distinct_vars)

        instantiations = []

        # Instantiation 0: All non-sentinel
        base_inst = {var: None for var in distinct_vars}
        instantiations.append(base_inst)

        # For patterns with ['i', 'j', 'k'], we only generate 3 instantiations:
        # - All non-sentinel
        # - k = sentinel (last variable)
        # - i = sentinel (first variable)
        # We SKIP j = sentinel (middle)

        if num_vars == 1:
            # Pattern like ['i'] or ['i', 'i'] -> 2 instantiations
            var = distinct_vars[0]
            instantiations.append({var: 'sentinel'})

        elif num_vars == 2:
            # Pattern like ['i', 'j'] or ['i', 'i', 'j'] -> 3 instantiations
            for var in distinct_vars:
                inst = {v: None for v in distinct_vars}
                inst[var] = 'sentinel'
                instantiations.append(inst)

        elif num_vars == 3:
            # Pattern ['i', 'j', 'k'] -> 3 instantiations (skip middle)
            # Add k=sentinel
            inst_k = {v: None for v in distinct_vars}
            inst_k[distinct_vars[2]] = 'sentinel'  # k
            instantiations.append(inst_k)

            # Add i=sentinel
            inst_i = {v: None for v in distinct_vars}
            inst_i[distinct_vars[0]] = 'sentinel'  # i
            instantiations.append(inst_i)

        return instantiations

    # ========================================================================
    # Level 1: Row Count
    # ========================================================================

    def _level1_row_count(self) -> Iterator[Dict[str, Any]]:
        """
        L1: Choose row count from 1 to max_row_count.

        Yields configurations with row_count field set.
        """
        for row_count in range(1, self.max_row_count + 1):
            if row_count == 1:
                for config in self._generate_single_row_configs():
                    yield config
            else:
                for config in self._level2_delete_patterns(row_count):
                    yield config

    def _generate_single_row_configs(self) -> Iterator[Dict[str, Any]]:
        """
        Generate configs for row_count=1.

        With 1 row: 1 data file, 1 row, that row is the sentinel.
        Simplifications vs general pipeline:
        - Only 1 data file (1 row can't be split)
        - Only single-variable patterns (all variables map to same row)
        - No instantiation variants (only one row to target)
        - L4c (repetition) skipped (can't repeat with 1 row)

        Still varies:
        - Delete file partitioning (Bell numbers on delete entries)
        - Snapshot orderings (data + delete files placed in valid orders)
        - Manifest structure (minimal/maximal)
        - Value strategy (sequential / null)
        """
        single_row_patterns = [[], ['i'], ['i', 'i'], ['i', 'i', 'i']]

        for pattern in single_row_patterns:
            delete_count = len(pattern)

            # 1 data file, 1 row
            data_files = [{
                'file_id': 'data_f0',
                'file_index': 0,
                'rows': [{'row_id': 0, 'value': None, 'position': 0}]
            }]

            # Generate delete file partitions (Bell partitions on delete entries)
            if delete_count == 0:
                delete_file_variants = [[]]
            else:
                # Since all delete entries are identical (same row, same position),
                # Bell partitions that produce the same multiset of file sizes are
                # equivalent. Only keep one partition per unique size multiset.
                seen_size_multisets = set()
                delete_file_variants = []
                for partition in generate_set_partitions(delete_count):
                    size_multiset = tuple(sorted(len(group) for group in partition))
                    if size_multiset in seen_size_multisets:
                        continue
                    seen_size_multisets.add(size_multiset)

                    del_files = []
                    for file_idx, entry_indices in enumerate(partition):
                        del_files.append({
                            'file_id': f'del_f{file_idx}',
                            'file_index': file_idx,
                            'deletes': [
                                {'row_id': 0, 'data_file': 'data_f0',
                                 'data_file_index': 0, 'position': 0}
                                for _ in entry_indices
                            ]
                        })
                    delete_file_variants.append(del_files)

            for delete_files in delete_file_variants:
                # Generate valid snapshot orderings
                # Files: 1 data file + N delete files
                # Constraint: each delete file must be in same or later snapshot as data_f0
                snapshot_orderings = self._generate_single_row_snapshot_orderings(delete_files)

                for snapshots in snapshot_orderings:
                    # Value strategies: sequential and null (skip repetition)
                    for value_strategy in ['row_numbers', 'nulls']:
                        data_files_copy = copy.deepcopy(data_files)
                        if value_strategy == 'row_numbers':
                            data_files_copy[0]['rows'][0]['value'] = 0
                        else:
                            data_files_copy[0]['rows'][0]['value'] = None

                        # Manifest strategies
                        for manifest_strategy in ['minimal', 'maximal']:
                            self.config_count += 1
                            config = {
                                'config_id': self.config_count,
                                'row_count': 1,
                                'delete_pattern': pattern,
                                'delete_count': delete_count,
                                'value_strategy': value_strategy,
                                'manifest_strategy': manifest_strategy,
                                'sentinel_row_id': 0,
                                'sentinel_in_file': 'data_f0',
                                'delete_instantiation_id': 0,
                                'is_data_partition_representative': True,
                                'data_partition_equiv_class': str(((1,), 1)),
                                'data_files': data_files_copy,
                                'delete_files': copy.deepcopy(delete_files),
                                'snapshots': snapshots,
                            }
                            # The single row is the null row; record it in
                            # null_rows for the nulls strategy only, matching the
                            # multi-row path (which leaves the key absent
                            # otherwise).
                            if value_strategy == 'nulls':
                                config['null_rows'] = [0]
                            yield config

    def _generate_single_row_snapshot_orderings(self, delete_files: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
        """
        Generate all valid snapshot orderings for row_count=1 configs.

        Constraint: delete files must be in same or later snapshot as data_f0.
        With 1 data file, valid options are:
        - All together in 1 snapshot
        - Data in snapshot 0, deletes spread across later snapshots
        - Max spread: data alone, each delete file in its own snapshot
        """
        if not delete_files:
            return [[{'snapshot_id': 0, 'data_files': ['data_f0'], 'delete_files': []}]]

        del_file_ids = [df['file_id'] for df in delete_files]
        orderings = []

        # Option 1: all in one snapshot
        orderings.append([{
            'snapshot_id': 0,
            'data_files': ['data_f0'],
            'delete_files': list(del_file_ids)
        }])

        # Option 2: data first, all deletes in second snapshot
        if len(del_file_ids) >= 1:
            orderings.append([
                {'snapshot_id': 0, 'data_files': ['data_f0'], 'delete_files': []},
                {'snapshot_id': 1, 'data_files': [], 'delete_files': list(del_file_ids)}
            ])

        # Option 3: max spread — data alone, each delete in its own snapshot
        if len(del_file_ids) >= 2:
            snapshots = [{'snapshot_id': 0, 'data_files': ['data_f0'], 'delete_files': []}]
            for i, del_id in enumerate(del_file_ids):
                snapshots.append({'snapshot_id': i + 1, 'data_files': [], 'delete_files': [del_id]})
            orderings.append(snapshots)

        return orderings

    # ========================================================================
    # Level 2: Delete Patterns
    # ========================================================================

    def _level2_delete_patterns(self, row_count: int) -> Iterator[Dict[str, Any]]:
        """
        L2: Choose delete pattern (data-independent).

        Yields configurations with delete pattern information.
        """
        for pattern in self.DELETE_PATTERNS:
            config = {
                'row_count': row_count,
                'delete_pattern': pattern,
                'delete_count': len(pattern)
            }
            for result in self._level3_file_partitioning(config):
                yield result

    def _physical_delete_signature(self, config: Dict[str, Any]) -> tuple:
        """Signature of the physical delete structure for dedup.
        Two configs with the same signature produce identical tables."""
        data_file_rows = tuple(
            tuple((row['row_id'], row['value']) for row in df['rows'])
            for df in config['data_files']
        )
        snapshot_sig = tuple(
            (tuple(sorted(s['data_files'])), tuple(sorted(s['delete_files'])))
            for s in config['snapshots']
        )
        delete_sig = tuple(
            tuple(sorted((d['row_id'], d['data_file'], d['position'])
                         for d in df['deletes']))
            for df in sorted(config['delete_files'], key=lambda f: f['file_id'])
        )
        return (
            config['value_strategy'],
            config.get('manifest_strategy', ''),
            data_file_rows,
            snapshot_sig,
            delete_sig,
            config.get('sentinel_in_file'),
        )

    # ========================================================================
    # Level 3: File Partitioning
    # ========================================================================

    def _level3_file_partitioning(self, config: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
        """
        L3: Partition data rows into files and deletes into delete files.

        Collects all skeletons, splits by is_data_partition_representative,
        pushes reps through L4→L5→L6 first, then nonreps.
        """
        skeletons = self._generate_l3_skeletons(config)

        reps_bag = [s for s in skeletons if s.get('is_data_partition_representative', True)]
        nonreps_bag = [s for s in skeletons if not s.get('is_data_partition_representative', True)]

        for skeleton in reps_bag:
            for result in self._level4_value_assignment(skeleton):
                yield result

        for skeleton in nonreps_bag:
            for result in self._level4_value_assignment(skeleton):
                if result.get('is_representative'):
                    logger.debug(
                        f"L3-nonrep became L5-rep: config_id={result.get('config_id')}, "
                        f"row_count={result.get('row_count')}, pattern={result.get('delete_pattern')}"
                    )
                yield result

    def _generate_l3_skeletons(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Generate all L3 structural skeletons (pre-L4) for a given pattern.

        Uses Bell number partitions for both data and deletes separately.
        All partitions get all instantiations (deterministic).
        """
        row_count = config['row_count']
        delete_count = config['delete_count']
        pattern = config['delete_pattern']

        sentinel_row = row_count // 2

        data_partitions = generate_set_partitions(row_count)

        equivalence_classes = group_data_partitions_by_equivalence(data_partitions, row_count)
        representatives = {}
        for sig, partitions in equivalence_classes.items():
            representatives[sig] = pick_representative(partitions)

        if delete_count == 0:
            delete_partitions = [[]]
        else:
            delete_partitions = generate_set_partitions(delete_count)

        all_instantiations = self._enumerate_instantiations(pattern)

        skeletons = []
        for data_partition in data_partitions:
            sig = compute_data_partition_signature(data_partition, row_count)
            is_representative = (data_partition == representatives[sig])

            sentinel_in_file = None
            for file_idx, subset in enumerate(data_partition):
                if sentinel_row in subset:
                    sentinel_in_file = f'data_f{file_idx}'
                    break

            for delete_partition in delete_partitions:
                data_files = self._create_data_files(data_partition)

                for inst_id, sentinel_constraint in enumerate(all_instantiations):
                    delete_files = self._create_delete_files(
                        delete_partition,
                        data_files,
                        pattern,
                        row_count,
                        sentinel_constraint
                    )

                    skeletons.append({
                        **config,
                        'data_partition': data_partition,
                        'delete_partition': delete_partition,
                        'sentinel_row_id': sentinel_row,
                        'sentinel_in_file': sentinel_in_file,
                        'delete_instantiation_id': inst_id,
                        'is_data_partition_representative': is_representative,
                        'data_partition_equiv_class': str(sig),
                        'data_files': data_files,
                        'delete_files': delete_files
                    })

        return skeletons

    def _create_data_files(self, partition: List[List[int]]) -> List[Dict[str, Any]]:
        """
        Create data file objects from a partition.

        Args:
            partition: Partition of row indices

        Returns:
            List of data file dictionaries
        """
        data_files = []
        for file_idx, row_indices in enumerate(partition):
            file_obj = {
                'file_id': f'data_f{file_idx}',
                'file_index': file_idx,
                'rows': [
                    {
                        'row_id': row_id,
                        'value': None,  # Will be assigned in L4
                        'position': pos
                    }
                    for pos, row_id in enumerate(row_indices)
                ]
            }
            data_files.append(file_obj)
        return data_files

    def _create_delete_files(self,
                            partition: List[List[int]],
                            data_files: List[Dict[str, Any]],
                            pattern: List[str],
                            row_count: int,
                            sentinel_constraint: Optional[Dict[str, Optional[str]]] = None) -> List[Dict[str, Any]]:
        """
        Create delete file objects from a partition and assign them to data files.

        The delete pattern is abstract (e.g., ['i', 'i', 'j']).
        We need to assign concrete row positions based on the data file structure.

        Strategy:
        - Group deletes by their variable (i, j, k)
        - Assign each group to row positions in data files
        - Respect sentinel constraints if provided

        Args:
            partition: Partition of delete indices (0 to delete_count-1)
            data_files: List of data file objects
            pattern: Abstract delete pattern like ['i', 'i', 'j']
            row_count: Total number of rows
            sentinel_constraint: Dict mapping variable -> 'sentinel' or None

        Returns:
            List of delete file dictionaries
        """
        if not pattern:
            return []

        # Map abstract variables to concrete row assignments
        row_assignments = self._assign_delete_positions(
            pattern, data_files, row_count, sentinel_constraint
        )

        if not row_assignments:
            return []

        # Build delete files based on partition
        delete_files = []
        for file_idx, delete_indices in enumerate(partition):
            deletes = []
            for del_idx in delete_indices:
                row_id, data_file_idx, position = row_assignments[del_idx]
                deletes.append({
                    'row_id': row_id,
                    'data_file': data_files[data_file_idx]['file_id'],
                    'data_file_index': data_file_idx,
                    'position': position
                })

            delete_file = {
                'file_id': f'del_f{file_idx}',
                'file_index': file_idx,
                'deletes': deletes
            }
            delete_files.append(delete_file)

        return delete_files

    def _assign_delete_positions(self,
                                 pattern: List[str],
                                 data_files: List[Dict[str, Any]],
                                 row_count: int,
                                 sentinel_constraint: Optional[Dict[str, Optional[str]]] = None) -> List[tuple]:
        """
        Assign abstract delete pattern to concrete row positions.

        Strategy:
        - Group deletes by variable (i, j, k)
        - Respect sentinel constraints if provided
        - For multiple same: pick same row multiple times
        - For different: spread across different files if possible

        Args:
            pattern: Abstract pattern like ['i', 'i', 'j']
            data_files: Available data files with rows
            row_count: Total number of rows (to compute sentinel)
            sentinel_constraint: Dict mapping variable -> 'sentinel' or None
                                Example: {'i': None, 'j': 'sentinel'}

        Returns:
            List of (row_id, data_file_index, position) tuples,
            one per delete in pattern
        """
        # Group deletes by variable
        var_groups = {}
        for idx, var in enumerate(pattern):
            if var not in var_groups:
                var_groups[var] = []
            var_groups[var].append(idx)

        # Compute sentinel row
        sentinel_row = row_count // 2

        # Collect all available (row_id, file_idx, position) tuples
        all_positions = []
        sentinel_positions = []
        non_sentinel_positions = []

        for file_idx, data_file in enumerate(data_files):
            for row in data_file['rows']:
                pos_tuple = (row['row_id'], file_idx, row['position'])
                all_positions.append(pos_tuple)

                if row['row_id'] == sentinel_row:
                    sentinel_positions.append(pos_tuple)
                else:
                    non_sentinel_positions.append(pos_tuple)

        if not all_positions:
            return []

        # Assign positions to variables
        assignments = [None] * len(pattern)
        non_sentinel_idx = 0

        for var in sorted(var_groups.keys()):
            indices = var_groups[var]

            # Determine which position to use based on sentinel constraint:
            # Variables constrained to 'sentinel' target the sentinel row specifically;
            # unconstrained variables round-robin through non-sentinel positions.
            if sentinel_constraint and sentinel_constraint.get(var) == 'sentinel':
                pos = sentinel_positions[0]
            else:
                if non_sentinel_positions:
                    pos = non_sentinel_positions[non_sentinel_idx % len(non_sentinel_positions)]
                    non_sentinel_idx += 1
                else:
                    # Only sentinel row available (edge case: row_count=1)
                    pos = all_positions[0]

            # All deletes with same variable get same position
            for idx in indices:
                assignments[idx] = pos

        return assignments

    # ========================================================================
    # Level 4: Value Assignment
    # ========================================================================

    def _level4_value_assignment(self, config: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
        """
        L4: Assign values to data rows (3 variants).

        - L4a: Values = row numbers (sequential, deterministic)
        - L4b: Nulls (some rows NULL, rest sequential)
        - L4c: Repetition (deleted row's value shared with others, if feasible)

        Args:
            config: Configuration with data_files

        Yields configurations with values assigned to all rows.
        """
        # L4a: Row numbers
        config_a = self._assign_values_row_numbers(config)
        for result in self._level5_snapshot_ordering(config_a):
            yield result

        # L4b: Nulls
        config_b = self._assign_values_nulls(config)
        for result in self._level5_snapshot_ordering(config_b):
            yield result

        # L4c: Repetition (if feasible)
        config_c = self._assign_values_repetition(config)
        if config_c is not None:
            for result in self._level5_snapshot_ordering(config_c):
                yield result

    def _assign_values_row_numbers(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        L4a: Assign row_id as value for each row.

        Simple 1:1 mapping.
        """
        config_copy = self._deep_copy_config(config)
        config_copy['value_strategy'] = 'row_numbers'

        for data_file in config_copy['data_files']:
            for row in data_file['rows']:
                row['value'] = row['row_id']

        return config_copy

    def _assign_values_nulls(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        L4b: Assign sequential values with some rows set to NULL.

        Uses seeded RNG to pick which rows get NULL. Remaining rows get
        sequential values (like L4a). The config stores which rows are null
        so the builder knows what to do (NULL if schema allows, otherwise
        treated as regular value).
        """
        config_copy = self._deep_copy_config(config)
        config_copy['value_strategy'] = 'nulls'

        row_count = config['row_count']

        # Pick which rows are null (seeded, reproducible)
        all_row_ids = list(range(row_count))
        num_nulls = max(1, self.random.randint(1, max(1, row_count // 2)))
        null_rows = set(self.random.sample(all_row_ids, min(num_nulls, row_count)))

        config_copy['null_rows'] = sorted(null_rows)

        # Assign values: NULL for selected rows, sequential for others
        for data_file in config_copy['data_files']:
            for row in data_file['rows']:
                if row['row_id'] in null_rows:
                    row['value'] = None
                else:
                    row['value'] = row['row_id']

        return config_copy

    def _assign_values_repetition(self, config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        L4c: Assign values with repetition based on deleted positions.

        Try both same-file and cross-file repetition from one deleted row.
        If that fails, do whichever one is feasible.
        """
        deleted_rows = self._get_deleted_row_numbers(config)
        if not deleted_rows:
            return None

        # Try both: same-file AND cross-file from one deleted row
        for row_id in sorted(deleted_rows):
            if self._can_satisfy_both_requirements(row_id, config):
                return self._create_repetition_config(row_id, config, same_file=True, cross_file=True)

        return None

    def _get_deleted_row_numbers(self, config: Dict[str, Any]) -> Set[int]:
        """Extract all row_ids that are deleted."""
        deleted = set()
        for delete_file in config['delete_files']:
            for delete in delete_file['deletes']:
                deleted.add(delete['row_id'])
        return deleted

    def _find_row_file(self, row_id: int, config: Dict[str, Any]) -> tuple:
        """Return (file_idx, file_size) for the file containing row_id."""
        for file_idx, data_file in enumerate(config['data_files']):
            for row in data_file['rows']:
                if row['row_id'] == row_id:
                    return file_idx, len(data_file['rows'])
        return None, 0

    def _can_same_file(self, row_id: int, config: Dict[str, Any]) -> bool:
        """Deleted row's file has ≥2 rows."""
        _, file_size = self._find_row_file(row_id, config)
        return file_size >= 2

    def _can_cross_file(self, config: Dict[str, Any]) -> bool:
        """There are ≥2 data files."""
        return len(config['data_files']) >= 2

    def _can_satisfy_both_requirements(self, row_id: int, config: Dict[str, Any]) -> bool:
        return self._can_same_file(row_id, config) and self._can_cross_file(config)

    def _create_repetition_config(self, row_id: int, config: Dict[str, Any],
                                  same_file: bool, cross_file: bool) -> Dict[str, Any]:
        """
        Create L4c config. Repeats the deleted row's value in same file
        and/or a different file depending on flags.
        """
        config_copy = self._deep_copy_config(config)
        config_copy['value_strategy'] = 'repetition'

        target_file_idx, _ = self._find_row_file(row_id, config)

        # Unique values start above max row_id to avoid collision with target_value
        max_row_id = max(
            row['row_id']
            for df in config_copy['data_files']
            for row in df['rows']
        )
        value_counter = max_row_id + 1
        target_value = row_id

        repeated_in_same_file = False
        repeated_cross_file = False

        for file_idx, data_file in enumerate(config_copy['data_files']):
            for row in data_file['rows']:
                if row['row_id'] == row_id:
                    row['value'] = target_value
                elif same_file and file_idx == target_file_idx and not repeated_in_same_file:
                    row['value'] = target_value
                    repeated_in_same_file = True
                elif cross_file and file_idx != target_file_idx and not repeated_cross_file:
                    row['value'] = target_value
                    repeated_cross_file = True
                else:
                    row['value'] = value_counter
                    value_counter += 1

        return config_copy

    # ========================================================================
    # Level 5: Snapshot Ordering
    # ========================================================================

    def _level5_snapshot_ordering(self, config: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
        """
        L5: Order files into snapshots with constraints.

        Two modes:
        1. Exhaustive mode (--exhaustive flag):
           - All configs get full exhaustive orderings
           - ≤5 files: All ordered partitions
           - ≥6 files: The same orderings as for five or fewer files, collapsed

        2. Equivalence-based mode (default):
           - Representatives: Full exhaustive orderings
           - Non-representatives: Random sample (1 ordering)

        Constraint: Delete files must be in same or later snapshot than their data files,
        unless deletes_before_data lifts it.
        """
        if self.exhaustive_mode:
            # Exhaustive mode: always generate all orderings
            orderings = self._generate_orderings_exhaustive(config)
            is_representative = None  # Not tracked in exhaustive mode
        else:
            # Equivalence-based mode
            is_representative = self.equiv_tracker.is_representative(config)

            if is_representative:
                # Representative: full exhaustive
                orderings = self._generate_orderings_exhaustive(config)
            else:
                # Non-representative: random sample
                orderings = self._generate_orderings_random_sample(config)

        for ordering in orderings:
            config_copy = self._deep_copy_config(config)
            config_copy['snapshots'] = ordering
            if is_representative is not None:
                config_copy['is_representative'] = is_representative
            for result in self._level6_manifest_structure(config_copy):
                yield result

    def _generate_orderings_exhaustive(self, config: Dict[str, Any]) -> List[List[Dict[str, Any]]]:
        """
        Generate all valid orderings using hybrid strategy.

        - ≤5 files: Full exhaustive (all ordered partitions)
        - ≥6 files: Full exhaustive with equivalence collapsing
        """
        num_data = len(config['data_files'])
        num_delete = len(config['delete_files'])
        num_total = num_data + num_delete

        if num_total <= 5:
            # Case 1: Full exhaustive (no collapsing)
            return self._generate_exhaustive_orderings(config)
        else:
            # Case 2: Full exhaustive with equivalence collapsing
            return self._generate_exhaustive_orderings_with_collapsing(config)

    def _generate_orderings_random_sample(self, config: Dict[str, Any]) -> List[List[Dict[str, Any]]]:
        """
        Generate a random sample of valid orderings for non-representatives.

        Strategy:
        1. Pick a random snapshot count (1 to num_files)
        2. Generate one random valid ordering with that snapshot count
        """
        all_files = config['data_files'] + config['delete_files']
        num_files = len(all_files)

        if num_files == 0:
            return [[]]

        # Pick random snapshot count (weighted toward fewer snapshots)
        max_snapshots = min(num_files, self.MAX_RANDOM_SNAPSHOTS)
        snapshot_count = self.random.randint(1, max_snapshots)

        # Try to generate a valid random ordering
        for _ in range(self.MAX_RANDOM_ORDERING_ATTEMPTS):
            ordering = self._generate_random_ordering(config, snapshot_count)
            if ordering is not None:
                return [ordering]

        # Fallback: return single snapshot (always valid)
        logger.debug(
            "Random ordering fallback triggered for config with %d files after %d attempts",
            num_files, self.MAX_RANDOM_ORDERING_ATTEMPTS
        )
        return [self._generate_single_snapshot_ordering(config)]

    def _generate_random_ordering(self, config: Dict[str, Any],
                                  snapshot_count: int) -> Optional[List[Dict[str, Any]]]:
        """
        Generate one random ordering with the specified snapshot count.

        Returns None if constraints cannot be satisfied.
        """
        all_files = config['data_files'] + config['delete_files']
        num_files = len(all_files)
        num_data = len(config['data_files'])

        # Build dependency map
        dependencies = self._build_file_dependencies(config)

        # Randomly assign files to snapshots
        file_to_snapshot = {}
        for file_idx in range(num_files):
            file_to_snapshot[file_idx] = self.random.randint(0, snapshot_count - 1)

        # Check if assignment satisfies constraints
        for del_idx, data_indices in dependencies.items():
            del_snap = file_to_snapshot[del_idx]
            for data_idx in data_indices:
                data_snap = file_to_snapshot[data_idx]
                if del_snap < data_snap:
                    # Constraint violated
                    return None

        # Convert to snapshot format
        snapshots = [{'snapshot_id': i, 'data_files': [], 'delete_files': []}
                     for i in range(snapshot_count)]

        for file_idx, snap_idx in file_to_snapshot.items():
            if file_idx < num_data:
                # Data file
                snapshots[snap_idx]['data_files'].append(all_files[file_idx]['file_id'])
            else:
                # Delete file
                snapshots[snap_idx]['delete_files'].append(all_files[file_idx]['file_id'])

        return snapshots

    def _generate_single_snapshot_ordering(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate trivial ordering with all files in one snapshot (always valid)."""
        snapshot = {
            'snapshot_id': 0,
            'data_files': [f['file_id'] for f in config['data_files']],
            'delete_files': [f['file_id'] for f in config['delete_files']]
        }
        return [snapshot]

    def _generate_exhaustive_orderings(self, config: Dict[str, Any]) -> List[List[Dict[str, Any]]]:
        """
        Generate all ordered partitions of files, filter for delete constraints.

        For ≤5 files total (max Ordered Bell(5) = 541).
        """
        all_files = config['data_files'] + config['delete_files']
        num_files = len(all_files)

        if num_files == 0:
            return [[]]  # Empty ordering

        # Build dependency map: delete_file_index -> set of data_file_indices
        dependencies = self._build_file_dependencies(config)

        # Generate all ordered partitions
        all_orderings = list(generate_ordered_partitions(num_files, num_files))

        # Filter valid orderings and convert to snapshots
        valid_orderings = []
        seen_signatures = set()

        for ordering in all_orderings:
            if self._is_valid_file_ordering(ordering, dependencies):
                # Convert to snapshot format (filters empty parts)
                snapshots = self._convert_ordering_to_snapshots(ordering, all_files)

                # Deduplicate: multiple orderings may become identical after filtering empty parts
                sig = self._compute_snapshot_signature(snapshots)
                if sig not in seen_signatures:
                    seen_signatures.add(sig)
                    valid_orderings.append(snapshots)

        return valid_orderings

    def _generate_exhaustive_orderings_with_collapsing(self, config: Dict[str, Any]) -> List[List[Dict[str, Any]]]:
        """
        Generate all ordered partitions with equivalence collapsing.

        For ≥6 files: Generate all valid orderings, then keep only one representative
        per canonical label sequence.

        Collapsing rules:
        - Consecutive snapshots of unreferenced data (no sentinel) → collapse
        - Consecutive snapshots of unreferenced data (with sentinel) → collapse
        - Consecutive delete-only snapshots → collapse
        """
        all_files = config['data_files'] + config['delete_files']
        num_files = len(all_files)

        if num_files == 0:
            return [[]]

        # Build dependency map
        dependencies = self._build_file_dependencies(config)

        # Identify which data files are referenced and which contain sentinel
        sentinel_row = config['row_count'] // 2
        data_file_info = []
        for data_idx, data_file in enumerate(config['data_files']):
            # Check if referenced
            is_referenced = False
            for del_file in config['delete_files']:
                for delete in del_file['deletes']:
                    if delete['data_file_index'] == data_idx:
                        is_referenced = True
                        break
                if is_referenced:
                    break

            # Check if contains sentinel
            has_sentinel = any(row['row_id'] == sentinel_row for row in data_file['rows'])

            data_file_info.append({
                'is_referenced': is_referenced,
                'has_sentinel': has_sentinel
            })

        # Generate all ordered partitions and filter for validity
        all_orderings = list(generate_ordered_partitions(num_files, num_files))

        # Track canonical sequences to avoid duplicates
        seen_canonical = {}
        seen_snapshot_sigs = set()  # For deduplicating after empty part filtering

        for ordering in all_orderings:
            if self._is_valid_file_ordering(ordering, dependencies):
                # Convert to snapshot format (filters empty parts)
                snapshots = self._convert_ordering_to_snapshots(ordering, all_files)

                # Deduplicate: multiple orderings may become identical after filtering
                snapshot_sig = self._compute_snapshot_signature(snapshots)
                if snapshot_sig in seen_snapshot_sigs:
                    continue
                seen_snapshot_sigs.add(snapshot_sig)

                # Compute canonical label sequence
                canonical = self._compute_canonical_label_sequence(
                    snapshots, data_file_info, len(config['data_files'])
                )

                # Keep only first representative of each canonical sequence
                if canonical not in seen_canonical:
                    seen_canonical[canonical] = snapshots

        return list(seen_canonical.values())

    def _compute_canonical_label_sequence(self, snapshots: List[Dict[str, Any]],
                                          data_file_info: List[Dict[str, bool]],
                                          num_data: int) -> tuple:
        """
        Compute canonical label sequence for an ordering.

        Labels:
        - UnrefNonSentinel: ONLY unreferenced data, NO sentinel
        - UnrefWithSentinel: ONLY unreferenced data, HAS sentinel
        - DeleteOnly: ONLY delete files
        - Other: everything else (referenced data, mixed)

        Collapsing: Consecutive identical collapsible labels reduce to one.
        """
        labels = []

        for snapshot in snapshots:
            # Extract file indices
            data_indices = [int(f.split('_f')[1]) for f in snapshot['data_files']]
            has_deletes = len(snapshot['delete_files']) > 0

            if not data_indices and has_deletes:
                # DeleteOnly
                labels.append('DeleteOnly')
            elif data_indices and not has_deletes:
                # Check if all data files are unreferenced
                all_unreferenced = all(
                    not data_file_info[idx]['is_referenced']
                    for idx in data_indices
                )

                if all_unreferenced:
                    # Check sentinel presence
                    any_sentinel = any(
                        data_file_info[idx]['has_sentinel']
                        for idx in data_indices
                    )

                    if any_sentinel:
                        labels.append('UnrefWithSentinel')
                    else:
                        labels.append('UnrefNonSentinel')
                else:
                    # Has referenced data
                    labels.append('Other')
            else:
                # Mixed or other
                labels.append('Other')

        # Collapse consecutive identical collapsible labels
        collapsed = []
        prev_label = None

        for label in labels:
            if label in ('UnrefNonSentinel', 'UnrefWithSentinel', 'DeleteOnly'):
                # Collapsible label
                if label != prev_label:
                    collapsed.append(label)
                    prev_label = label
                # else: skip, it's a consecutive duplicate
            else:
                # Non-collapsible label (Other)
                collapsed.append(label)
                prev_label = label

        return tuple(collapsed)

    def _build_file_dependencies(self, config: Dict[str, Any]) -> Dict[int, Set[int]]:
        """
        Build dependency map for all files (data + delete combined), which
        orders each delete file at or after the data files it names; none
        under deletes_before_data.

        Returns:
            Dict mapping delete_file_global_index -> set of data_file_global_indices
        """
        num_data = len(config['data_files'])
        dependencies = {}
        if self.deletes_before_data:
            return dependencies

        for del_idx, delete_file in enumerate(config['delete_files']):
            global_del_idx = num_data + del_idx  # Delete files come after data files
            referenced_data = set()

            for delete in delete_file['deletes']:
                data_file_idx = delete['data_file_index']
                referenced_data.add(data_file_idx)

            if referenced_data:
                dependencies[global_del_idx] = referenced_data

        return dependencies

    def _is_valid_file_ordering(self, ordering: List[List[int]],
                                dependencies: Dict[int, Set[int]]) -> bool:
        """
        Check if file ordering satisfies delete-after-data constraints.

        Args:
            ordering: Ordered partition of file indices
            dependencies: Dict mapping delete_file_index -> set of data_file_indices

        Returns:
            True if valid
        """
        # Build file_index -> snapshot_index map
        file_to_snapshot = {}
        for snap_idx, snapshot in enumerate(ordering):
            for file_idx in snapshot:
                file_to_snapshot[file_idx] = snap_idx

        # Check each dependency
        for delete_idx, data_indices in dependencies.items():
            delete_snap = file_to_snapshot[delete_idx]
            for data_idx in data_indices:
                data_snap = file_to_snapshot[data_idx]
                # Delete must be in same or later snapshot
                if delete_snap < data_snap:
                    return False

        return True

    def _convert_ordering_to_snapshots(self, ordering: List[List[int]],
                                       all_files: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Convert ordered partition of file indices to snapshot format.

        Args:
            ordering: List of snapshots, each containing file indices
            all_files: Combined list of data_files + delete_files

        Returns:
            List of snapshot dicts with data_files and delete_files separated

        Note:
            Empty parts in the ordering are filtered out (not converted to snapshots).
        """
        num_data = len([f for f in all_files if f['file_id'].startswith('data_')])

        snapshots = []
        snapshot_id = 0

        for file_indices in ordering:
            # Skip empty parts (generated by ordered partitions algorithm)
            if not file_indices:
                continue

            data_file_ids = []
            delete_file_ids = []

            for file_idx in file_indices:
                file_id = all_files[file_idx]['file_id']
                if file_idx < num_data:
                    data_file_ids.append(file_id)
                else:
                    delete_file_ids.append(file_id)

            snapshot = {
                'snapshot_id': snapshot_id,
                'data_files': data_file_ids,
                'delete_files': delete_file_ids
            }
            snapshots.append(snapshot)
            snapshot_id += 1

        return snapshots

    # ========================================================================
    # Level 6: Manifest Structure
    # ========================================================================

    def _level6_manifest_structure(self, config: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
        """
        L6: Define manifest structure (minimal vs maximal).

        Creates 2 variants:
        - All snapshots use minimal manifests
        - All snapshots use maximal manifests
        """
        # Variant 1: Minimal
        config_min = self._create_minimal_manifests(config)
        self.config_count += 1
        config_min['config_id'] = self.config_count
        yield config_min

        # Variant 2: Maximal
        config_max = self._create_maximal_manifests(config)
        self.config_count += 1
        config_max['config_id'] = self.config_count
        yield config_max

    def _create_minimal_manifests(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create config with all files in one manifest per type per snapshot.
        """
        config_copy = self._deep_copy_config(config)
        config_copy['manifest_strategy'] = 'minimal'

        for snapshot in config_copy['snapshots']:
            # All data files in one manifest
            if snapshot['data_files']:
                snapshot['data_manifests'] = [snapshot['data_files']]
            else:
                snapshot['data_manifests'] = []

            # All delete files in one manifest
            if snapshot['delete_files']:
                snapshot['delete_manifests'] = [snapshot['delete_files']]
            else:
                snapshot['delete_manifests'] = []

        return config_copy

    def _create_maximal_manifests(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create config with each file in its own manifest.
        """
        config_copy = self._deep_copy_config(config)
        config_copy['manifest_strategy'] = 'maximal'

        for snapshot in config_copy['snapshots']:
            # Each data file in its own manifest
            snapshot['data_manifests'] = [[f] for f in snapshot['data_files']]

            # Each delete file in its own manifest
            snapshot['delete_manifests'] = [[f] for f in snapshot['delete_files']]

        return config_copy

    # ========================================================================
    # Helper Methods
    # ========================================================================

    def _compute_snapshot_signature(self, snapshots: List[Dict[str, Any]]) -> tuple:
        """
        Compute a hashable signature for a snapshot ordering.

        Used for deduplication after filtering empty snapshots.
        """
        sig = []
        for snapshot in snapshots:
            data_files = tuple(sorted(snapshot['data_files']))
            delete_files = tuple(sorted(snapshot['delete_files']))
            sig.append((data_files, delete_files))
        return tuple(sig)

    def _deep_copy_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Deep copy a configuration dictionary."""
        return copy.deepcopy(config)
