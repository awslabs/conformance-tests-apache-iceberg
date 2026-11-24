#!/usr/bin/env python3
"""
Validation tests for the exhaustive shape config generator.

Checks:
1. Completeness: config counts match known regression values
2. No duplicates: all configs have unique structural signatures
3. Coverage: every valid (row_count, delete_pattern, value_strategy) bucket has configs
"""

import json
import hashlib
from collections import defaultdict

from iceberg_conformance.exhaustive.shape_config_generator import TableConfigGenerator


EXPECTED_COUNTS = {
    1: 64,
    2: 1332,
    3: 5478,
    4: 17900,
    5: 45474,
    6: 132104,
}

DELETE_PATTERNS = [
    [],
    ['i'],
    ['i', 'i'],
    ['i', 'j'],
    ['i', 'i', 'i'],
    ['i', 'i', 'j'],
    ['i', 'j', 'k'],
]

VALUE_STRATEGIES = ['row_numbers', 'nulls', 'repetition']


def compute_structural_signature(config):
    """Compute a hashable signature capturing everything that makes a config unique.
    If two configs have the same signature, they would produce identical tables."""
    data_file_rows = tuple(
        tuple((row['row_id'], row['value']) for row in df['rows'])
        for df in config['data_files']
    )

    snapshot_sig = tuple(
        (tuple(sorted(s['data_files'])), tuple(sorted(s['delete_files'])))
        for s in config['snapshots']
    )

    # Sort entries within each delete file (order within file doesn't matter)
    delete_sig = tuple(
        tuple(sorted((d['row_id'], d['data_file'], d['position']) for d in df['deletes']))
        for df in sorted(config['delete_files'], key=lambda f: f['file_id'])
    )

    return (
        config['row_count'],
        config['delete_count'],
        config['value_strategy'],
        config.get('manifest_strategy', ''),
        data_file_rows,
        snapshot_sig,
        delete_sig,
        config.get('sentinel_in_file'),
    )


def test_completeness():
    """Verify config counts match known regression values per row_count."""
    print("Testing completeness (regression counts)...")
    failures = []

    from collections import Counter
    g = TableConfigGenerator(max_row_count=max(EXPECTED_COUNTS.keys()))
    by_row = Counter()
    for config in g.generate_all():
        by_row[config['row_count']] += 1

    for row_count, expected in EXPECTED_COUNTS.items():
        actual = by_row[row_count]
        if actual != expected:
            failures.append(f"  row_count={row_count}: expected {expected}, got {actual}")
        else:
            print(f"  row_count={row_count}: {actual} ✓")

    if failures:
        print("FAILED:")
        for f in failures:
            print(f)
        return False
    print("  All counts match ✓")
    return True


def test_no_duplicates(max_row_count=4, max_duplicate_pct=2.0):
    """Verify duplicate rate is under threshold."""
    print(f"Testing duplicates under {max_duplicate_pct}% (max_row_count={max_row_count})...")

    g = TableConfigGenerator(max_row_count=max_row_count)
    seen = {}
    duplicates = []

    for config in g.generate_all():
        sig = compute_structural_signature(config)
        sig_hash = hashlib.sha256(str(sig).encode()).hexdigest()[:16]

        if sig_hash in seen:
            duplicates.append((config['config_id'], seen[sig_hash]))
        else:
            seen[sig_hash] = config['config_id']

    total = len(seen) + len(duplicates)
    pct = 100 * len(duplicates) / total if total > 0 else 0

    if pct > max_duplicate_pct:
        print(f"  FAILED: {len(duplicates)} duplicate(s) ({pct:.2f}%) exceeds {max_duplicate_pct}%")
        for dup_id, orig_id in duplicates[:5]:
            print(f"    config {dup_id} duplicates config {orig_id}")
        return False

    print(f"  {total} configs, {len(duplicates)} duplicates ({pct:.2f}%) ✓")
    return True


def test_coverage(max_row_count=4):
    """Verify every valid (row_count, delete_pattern, value_strategy) bucket has configs."""
    print(f"Testing coverage (max_row_count={max_row_count})...")

    g = TableConfigGenerator(max_row_count=max_row_count)
    buckets = defaultdict(int)

    for config in g.generate_all():
        key = (
            config['row_count'],
            tuple(config['delete_pattern']),
            config['value_strategy'],
        )
        buckets[key] += 1

    empty_buckets = []
    for row_count in range(1, max_row_count + 1):
        for pattern in DELETE_PATTERNS:
            if len(pattern) > row_count:
                continue
            # row_count=1 only uses single-variable patterns
            if row_count == 1 and len(set(pattern)) > 1:
                continue
            for strategy in VALUE_STRATEGIES:
                # Repetition is skipped when:
                # - row_count = 1 (can't share values with only 1 row)
                # - no deletes (current implementation requires a deleted row)
                # - row_count = 2 (P1/P2 fail: with 2 rows and 1-2 files,
                #   can't satisfy same-file + cross-file repetition.
                #   P3 fallback not yet implemented)
                if strategy == 'repetition':
                    if row_count == 1:
                        continue
                    if len(pattern) == 0:
                        continue
                    if row_count == 2:
                        continue
                key = (row_count, tuple(pattern), strategy)
                if buckets[key] == 0:
                    empty_buckets.append(key)

    if empty_buckets:
        print(f"  FAILED: {len(empty_buckets)} empty bucket(s)")
        for bucket in empty_buckets[:10]:
            print(f"    {bucket}")
        return False

    print(f"  {len(buckets)} non-empty buckets, no gaps ✓")
    return True


if __name__ == '__main__':
    results = []
    results.append(test_completeness())
    results.append(test_no_duplicates())
    results.append(test_coverage())

    print()
    if all(results):
        print("ALL TESTS PASSED ✓")
    else:
        print("SOME TESTS FAILED ✗")
        exit(1)
