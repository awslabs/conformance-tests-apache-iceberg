#!/usr/bin/env python3
"""
Combinatorial utility functions for table configuration generation.

Provides functions for:
- Set partitions (Bell numbers)
- Ordered partitions (Fubini/Ordered Bell numbers)
- Snapshot ordering constraint filtering
"""

from typing import List, Iterator, Dict, Set


def generate_set_partitions(n: int) -> List[List[List[int]]]:
    """
    Generate all ways to partition a set {0, 1, ..., n-1} into non-empty subsets.

    This generates all set partitions (Bell number B(n) count).

    Args:
        n: Size of the set to partition

    Returns:
        List of partitions, where each partition is a list of subsets

    Example:
        For n=3: [[[0,1,2]], [[0,1],[2]], [[0,2],[1]], [[1,2],[0]], [[0],[1],[2]]]
    """
    if n == 0:
        return [[]]
    if n == 1:
        return [[[0]]]

    result = []

    # Get all partitions of {0, 1, ..., n-2}
    smaller_partitions = generate_set_partitions(n - 1)

    for partition in smaller_partitions:
        # Add element (n-1) to each existing subset
        for i in range(len(partition)):
            new_partition = [subset[:] for subset in partition]
            new_partition[i].append(n - 1)
            result.append(new_partition)

        # Add element (n-1) as a new singleton subset
        new_partition = [subset[:] for subset in partition]
        new_partition.append([n - 1])
        result.append(new_partition)

    return result


def generate_ordered_partitions(n: int, max_parts: int = None) -> Iterator[List[List[int]]]:
    """
    Generate ordered partitions of n items into k non-empty ordered bins.

    This generates Fubini numbers (Ordered Bell numbers).
    Unlike set partitions, the order of bins matters: [[0],[1]] != [[1],[0]]

    Args:
        n: Number of items to partition (items are 0, 1, ..., n-1)
        max_parts: Maximum number of parts (bins). If None, uses n.

    Yields:
        Ordered partitions as lists of lists

    Example:
        For n=2:
            [[0, 1]]      - both in one bin
            [[0], [1]]    - 0 first, then 1
            [[1], [0]]    - 1 first, then 0

        For n=3, max_parts=2:
            [[0,1,2]]
            [[0,1], [2]]
            [[0,2], [1]]
            [[1,2], [0]]
            [[0], [1,2]]
            [[1], [0,2]]
            [[2], [0,1]]
    """
    if max_parts is None:
        max_parts = n

    if n == 0:
        yield []
        return

    # Generate partitions for k = 1 to max_parts bins
    for k in range(1, min(n, max_parts) + 1):
        # Generate all canonical assignments of n items to k ordered bins
        for assignment in _assign_items_to_ordered_bins(n, k):
            # Convert assignment tuple to list of lists
            bins = [[] for _ in range(k)]
            for item_idx, bin_idx in enumerate(assignment):
                bins[bin_idx].append(item_idx)
            yield bins


def _assign_items_to_ordered_bins(num_items: int, num_bins: int) -> Iterator[tuple]:
    """
    Generate all ways to assign items to ordered bins (canonical form).

    Canonical form: bins are used in order, no gaps.
    For example, with 3 items and 2 bins:
        - (0,0,0) - all in bin 0
        - (0,0,1) - items 0,1 in bin 0, item 2 in bin 1
        - (0,1,0) - items 0,2 in bin 0, item 1 in bin 1
        - etc.

    Args:
        num_items: Number of items to assign
        num_bins: Number of bins

    Yields:
        Tuples of length num_items, where tuple[i] is the bin for item i
    """
    if num_items == 0:
        yield ()
        return

    if num_bins == 1:
        yield (0,) * num_items
        return

    # First item must go in bin 0 (canonical form)
    for rest in _assign_items_recursive(num_items - 1, num_bins, 0):
        yield (0,) + rest


def _assign_items_recursive(remaining_items: int, num_bins: int, max_bin_used: int) -> Iterator[tuple]:
    """
    Recursively generate canonical bin assignments.

    Args:
        remaining_items: Number of items left to assign
        num_bins: Total number of bins available
        max_bin_used: Highest bin index used so far

    Yields:
        Tuples representing bin assignments for remaining items
    """
    if remaining_items == 0:
        yield ()
        return

    # Can assign to any bin from 0 to min(max_bin_used + 1, num_bins - 1)
    # This maintains canonical form (no gaps in bin usage)
    for bin_idx in range(min(max_bin_used + 2, num_bins)):
        new_max_bin = max(max_bin_used, bin_idx)
        for rest in _assign_items_recursive(remaining_items - 1, num_bins, new_max_bin):
            yield (bin_idx,) + rest


def filter_snapshot_orderings(orderings: List[List[List[int]]],
                               dependencies: Dict[int, Set[int]]) -> List[List[List[int]]]:
    """
    Filter snapshot orderings to keep only those where deletes come after their data.

    Args:
        orderings: List of snapshot orderings, where each ordering is a list of snapshots,
                  and each snapshot is a list of file indices
        dependencies: Dict mapping delete file index -> set of data file indices it depends on

    Returns:
        Filtered list containing only valid orderings

    Example:
        orderings = [
            [[0, 1, 2]],        # All in snapshot 0
            [[0, 1], [2]],      # 0,1 in snap 0, 2 in snap 1
            [[2], [0, 1]]       # 2 in snap 0, 0,1 in snap 1
        ]
        dependencies = {2: {0}}  # Delete file 2 depends on data file 0

        Result: First two orderings valid, third invalid (delete 2 before data 0)
    """
    if not dependencies:
        # No dependencies, all orderings valid
        return orderings

    valid_orderings = []

    for ordering in orderings:
        if _is_valid_ordering(ordering, dependencies):
            valid_orderings.append(ordering)

    return valid_orderings


def _is_valid_ordering(ordering: List[List[int]], dependencies: Dict[int, Set[int]]) -> bool:
    """
    Check if an ordering satisfies all delete-after-data constraints.

    Args:
        ordering: A snapshot ordering (list of snapshots)
        dependencies: Dict mapping delete file index -> set of data file indices

    Returns:
        True if valid, False if any delete comes before its data
    """
    # Build a map: file_index -> snapshot_index
    file_to_snapshot = {}
    for snapshot_idx, snapshot in enumerate(ordering):
        for file_idx in snapshot:
            file_to_snapshot[file_idx] = snapshot_idx

    # Check each dependency
    for delete_file, data_files in dependencies.items():
        delete_snapshot = file_to_snapshot.get(delete_file)
        if delete_snapshot is None:
            continue

        for data_file in data_files:
            data_snapshot = file_to_snapshot.get(data_file)
            if data_snapshot is None:
                continue

            # Delete must be in same or later snapshot than data
            if delete_snapshot < data_snapshot:
                return False

    return True


def compute_file_sizes(partition: List[List[int]]) -> List[int]:
    """
    Compute the size of each subset in a partition.

    Args:
        partition: A partition (list of subsets)

    Returns:
        List of sizes, one per subset

    Example:
        partition = [[0, 1, 2], [3, 4], [5]]
        returns: [3, 2, 1]
    """
    return [len(subset) for subset in partition]


# Pre-computed Bell numbers for quick reference
BELL_NUMBERS = {
    0: 1,
    1: 1,
    2: 2,
    3: 5,
    4: 15,
    5: 52,
    6: 203,
}

# Pre-computed Ordered Bell (Fubini) numbers
ORDERED_BELL_NUMBERS = {
    0: 1,
    1: 1,
    2: 3,
    3: 13,
    4: 75,
    5: 541,
    6: 4683,
    7: 47293,
    8: 545835,
    9: 7087261,
}
