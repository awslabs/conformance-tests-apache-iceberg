"""Tests pinning the partitioned table matrix and the names derived from it.

`generate` and `validate` both read `partitioned_table_specs`, so a rename, an
omission or a duplicate in it would be mirrored on both sides and the two would
still agree with each other. These tests hold the contract independently: the
counts, the derived names, and the uniqueness the matrix depends on.
"""

import pytest

from iceberg_fuzzer.common.partition_transforms import (
    DELETE_MODES,
    FULL_TRANSFORMS,
    FULL_WRITE_PATTERNS,
    MINIMAL_TRANSFORMS,
    MINIMAL_WRITE_PATTERNS,
    partitioned_table_specs,
    transform_name,
)

EXPECTED_MINIMAL_NAMES = [
    'part_identity_category_mixed_chaos_cow',
    'part_identity_category_mixed_chaos_posdelete',
    'part_bucket4_id_mixed_chaos_cow',
    'part_bucket4_id_mixed_chaos_posdelete',
    'part_month_timestamp_mixed_chaos_cow',
    'part_month_timestamp_mixed_chaos_posdelete',
    'part_identity_category_bucket4_id_mixed_chaos_cow',
    'part_identity_category_bucket4_id_mixed_chaos_posdelete',
]


class TestCounts:
    def test_minimal_is_eight_tables(self):
        assert len(partitioned_table_specs(minimal=True)) == 8

    def test_full_is_two_hundred_and_eighty_eight(self):
        assert len(partitioned_table_specs()) == 288

    def test_the_counts_are_the_product_of_the_dimensions(self):
        # Guards against a table being dropped or doubled by a change to any one
        # dimension rather than to the count itself.
        assert len(MINIMAL_TRANSFORMS) * len(MINIMAL_WRITE_PATTERNS) * len(DELETE_MODES) == 8
        assert len(FULL_TRANSFORMS) * len(FULL_WRITE_PATTERNS) * len(DELETE_MODES) == 288

    def test_minimal_is_a_subset_of_full(self):
        full = {spec['name'] for spec in partitioned_table_specs()}
        minimal = {spec['name'] for spec in partitioned_table_specs(minimal=True)}
        assert minimal < full


class TestNames:
    def test_minimal_names_are_exactly_these(self):
        assert [s['name'] for s in partitioned_table_specs(minimal=True)] == EXPECTED_MINIMAL_NAMES

    def test_names_are_unique(self):
        # Two transforms deriving the same name would silently halve the coverage:
        # one table would be generated twice and the other not at all.
        for minimal in (True, False):
            names = [spec['name'] for spec in partitioned_table_specs(minimal=minimal)]
            duplicates = {name for name in names if names.count(name) > 1}
            assert not duplicates, duplicates

    def test_order_is_stable(self):
        # Callers index into this to pick a random subset with a seed, so the order
        # has to be the same on every run.
        assert [s['name'] for s in partitioned_table_specs()] == \
            [s['name'] for s in partitioned_table_specs()]

    @pytest.mark.parametrize('transform,expected', [
        (['identity(category)'], 'identity_category'),
        (['bucket[4](id)'], 'bucket4_id'),
        (['month(timestamp)'], 'month_timestamp'),
        (['truncate[2](name)'], 'truncate2_name'),
        (['identity(category)', 'bucket[4](id)'], 'identity_category_bucket4_id'),
    ])
    def test_name_derivation(self, transform, expected):
        assert transform_name(transform) == expected

    def test_a_transform_the_parser_rejects_has_no_name(self):
        # The parser rejects `date`, so the name derivation has to as well.
        with pytest.raises(ValueError, match='unrecognized partition transform'):
            transform_name(['date(timestamp)'])

    def test_every_name_is_made_of_its_parts(self):
        for spec in partitioned_table_specs():
            assert spec['name'] == \
                f"part_{transform_name(spec['transform'])}_{spec['pattern']}_{spec['delete_mode']}"


class TestTransforms:
    def test_minimal_transforms_are_in_the_full_set(self):
        assert all(transform in FULL_TRANSFORMS for transform in MINIMAL_TRANSFORMS)

    def test_no_transform_is_listed_twice(self):
        listed = [tuple(transform) for transform in FULL_TRANSFORMS]
        duplicates = {t for t in listed if listed.count(t) > 1}
        assert not duplicates, duplicates


class TestParsing:
    """Every transform in the matrix has to be one the PySpark backend can parse."""

    def test_every_transform_parses(self):
        from iceberg_fuzzer.backend.pyspark.builders import parse_partition_spec
        for transform in FULL_TRANSFORMS:
            parsed = parse_partition_spec(",".join(transform))
            assert len(parsed) == len(transform), transform

    def test_an_unrecognised_transform_is_rejected(self):
        # Silently dropping it would create the table unpartitioned under a name
        # saying it was partitioned, which no check downstream would catch.
        from iceberg_fuzzer.backend.pyspark.builders import parse_partition_spec
        with pytest.raises(ValueError, match='Unrecognised partition transform'):
            parse_partition_spec('date(timestamp)')

    def test_a_typo_is_rejected(self):
        from iceberg_fuzzer.backend.pyspark.builders import parse_partition_spec
        with pytest.raises(ValueError, match='Unrecognised partition transform'):
            parse_partition_spec('buckets[4](id)')
