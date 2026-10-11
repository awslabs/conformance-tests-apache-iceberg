"""Tests for comparing a row's position against the id it carries.

Content validation derives a row's expected columns from the id on that row, so
a row that should not be in the table matches itself. Only the position-to-id
comparison objects, and only if it predicts the right id for the right table.
"""

import importlib.util
from pathlib import Path

import pytest

from iceberg_fuzzer.common.mixed_chaos import (
    MIXED_CHAOS_TOTAL_ROWS_APPENDED,
    MIXED_CHAOS_TOTAL_ROWS_FINAL,
    posdelete_removes_row,
    posdelete_surviving_row_id,
)
from iceberg_fuzzer.common.partition_transforms import (
    FULL_WRITE_PATTERNS,
    partitioned_row_id,
    partitioned_table_specs,
)

PATTERNS_THAT_SCATTER = {('mixed_chaos', 'posdelete')}


def _load_duckdb_validator():
    """Load the DuckDB validator by path.

    `iceberg_fuzzer/validator.py` takes the `iceberg_fuzzer.validator` name, so the
    `validator/local_duckdb/` directory beneath it is not importable; the CLI runs
    the script as a subprocess instead.
    """
    path = (
        Path(__file__).resolve().parent.parent
        / 'iceberg_fuzzer' / 'validator' / 'local_duckdb' / 'validator_duckdb.py'
    )
    spec = importlib.util.spec_from_file_location('validator_duckdb', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestTheDeleteRuleItself:
    def test_the_deletes_land_on_the_declared_row_count(self):
        # If they did not, the row-count check would fail for a reason that has
        # nothing to do with the defect this comparison is here to catch.
        removed = sum(
            1 for row_id in range(1, MIXED_CHAOS_TOTAL_ROWS_APPENDED + 1)
            if posdelete_removes_row(row_id)
        )
        assert MIXED_CHAOS_TOTAL_ROWS_APPENDED - removed == MIXED_CHAOS_TOTAL_ROWS_FINAL

    def test_surviving_ids_are_the_ones_not_deleted(self):
        survivors = [
            row_id for row_id in range(1, MIXED_CHAOS_TOTAL_ROWS_APPENDED + 1)
            if not posdelete_removes_row(row_id)
        ]
        assert [posdelete_surviving_row_id(i) for i in range(len(survivors))] == survivors

    def test_the_last_surviving_id_is_past_the_final_row_count(self):
        # The ids left are scattered over the whole appended range, so the
        # highest is well above the row count -- which is exactly what a check
        # assuming a contiguous run would get wrong.
        last = posdelete_surviving_row_id(MIXED_CHAOS_TOTAL_ROWS_FINAL - 1)
        assert last > MIXED_CHAOS_TOTAL_ROWS_FINAL
        assert last == MIXED_CHAOS_TOTAL_ROWS_APPENDED - 1


class TestWhichIdBelongsAtAPosition:
    def test_mixed_chaos_posdelete_skips_the_deleted_ids(self):
        assert [
            partitioned_row_id(i, 'mixed_chaos', 'posdelete') for i in range(6)
        ] == [1, 2, 4, 5, 7, 8]

    def test_mixed_chaos_cow_is_contiguous(self):
        # A cow table is rewritten whole, so nothing is skipped.
        assert [
            partitioned_row_id(i, 'mixed_chaos', 'cow') for i in range(6)
        ] == [1, 2, 3, 4, 5, 6]

    @pytest.mark.parametrize('delete_mode', ['cow', 'posdelete'])
    @pytest.mark.parametrize('pattern', sorted(set(FULL_WRITE_PATTERNS)))
    def test_every_other_combination_is_contiguous(self, pattern, delete_mode):
        # Applying the scattered rule to a pattern that deletes a contiguous
        # range, or none at all, reports a mismatch on almost every row.
        if (pattern, delete_mode) in PATTERNS_THAT_SCATTER:
            pytest.skip('covered above')
        assert [
            partitioned_row_id(i, pattern, delete_mode) for i in range(6)
        ] == [1, 2, 3, 4, 5, 6]

    def test_only_one_table_in_the_set_scatters(self):
        scattering = {
            spec['name'] for spec in partitioned_table_specs()
            if partitioned_row_id(2, spec['pattern'], spec['delete_mode']) != 3
        }
        assert scattering == {
            spec['name'] for spec in partitioned_table_specs()
            if (spec['pattern'], spec['delete_mode']) in PATTERNS_THAT_SCATTER
        }
        assert scattering, 'the scattered rule should reach some table'

    def test_an_unknown_pattern_is_assumed_contiguous(self):
        # The generator's `else` arm appends without deleting, so a pattern it
        # does not recognise holds a contiguous run.
        assert partitioned_row_id(4, 'something_new', 'posdelete') == 5


class TestTheCallSiteReadsTheKeysTheSpecsCarry:
    """The expectation is keyed on two spec fields, read with `.get`.

    A rename of either leaves `.get` returning None, which routes every table to
    the contiguous rule. `mixed_chaos` posdelete would then report a mismatch on
    almost every row, and nothing above would notice -- those tests call
    `partitioned_row_id` directly with the arguments spelled out.
    """

    def test_every_partitioned_spec_carries_both_keys(self):
        validator = _load_duckdb_validator()
        specs = validator._extract_expected_partitioned_tables({}, minimal=False)
        assert specs, 'no partitioned specs were produced'
        for spec in specs:
            assert spec.get('pattern'), spec['name']
            assert spec.get('delete_mode'), spec['name']

    def test_the_scattered_rule_is_reached_through_a_real_spec(self):
        validator = _load_duckdb_validator()
        specs = validator._extract_expected_partitioned_tables({}, minimal=True)
        scattered = [
            spec for spec in specs
            if partitioned_row_id(2, spec.get('pattern'), spec.get('delete_mode')) != 3
        ]
        assert scattered, 'no spec in the minimal set reaches the scattered rule'
        for spec in scattered:
            assert spec['pattern'] == 'mixed_chaos'
            assert spec['delete_mode'] == 'posdelete'

    def test_a_spec_missing_its_keys_would_be_read_as_contiguous(self):
        # Pins the consequence, so the two tests above are not just restating the
        # happy path: this is what a rename silently produces.
        assert partitioned_row_id(2, None, None) == 3
