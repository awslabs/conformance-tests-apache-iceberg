"""Tests for the delete-mode check in the DuckDB validator.

The check reads a snapshot summary out of the table metadata, so these build the
metadata directly rather than generating a table, which takes about twenty minutes
each.
"""

import importlib.util
import json
from pathlib import Path

import pytest


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


validator = _load_duckdb_validator()


@pytest.fixture
def table_with_summary(tmp_path):
    """A table directory holding just the snapshot summary the check reads."""
    def build(summary, version=1):
        metadata_dir = tmp_path / 'metadata'
        metadata_dir.mkdir(exist_ok=True)
        (metadata_dir / f'v{version}.metadata.json').write_text(json.dumps({
            'format-version': 2,
            'current-snapshot-id': 1,
            'snapshots': [{'snapshot-id': 1, 'summary': summary}],
        }))
        return tmp_path
    return build


NO_DELETES = {'total-delete-files': '0', 'total-position-deletes': '0'}
POSITION_DELETES = {'total-delete-files': '25', 'total-position-deletes': '50000'}


class TestPosdeleteRequirement:
    def test_no_position_deletes_fails_when_required(self, table_with_summary):
        valid, info = validator._validate_delete_file_types(
            table_with_summary(NO_DELETES), 'posdelete', require_position_deletes=True
        )
        assert not valid
        assert 'position deletes' in info

    def test_position_deletes_pass_when_required(self, table_with_summary):
        assert validator._validate_delete_file_types(
            table_with_summary(POSITION_DELETES), 'posdelete',
            require_position_deletes=True,
        )[0]

    def test_nothing_is_required_by_default(self, table_with_summary):
        # The patterns that never issue a DELETE hold no delete file and are not
        # expected to, so the requirement is opted into rather than out of.
        assert validator._validate_delete_file_types(
            table_with_summary(NO_DELETES), 'posdelete'
        )[0]

    def test_a_cow_table_is_still_rejected_for_holding_deletes(self, table_with_summary):
        # Guards against the requirement being read as "deletes are fine here".
        assert not validator._validate_delete_file_types(
            table_with_summary(POSITION_DELETES), 'cow', require_position_deletes=True
        )[0]


class TestWhichTablesItAppliesTo:
    """Which specs ask for the requirement, through the call site that decides.

    Scoping it wider than mixed_chaos fails tables for a reason unrelated to the
    delete mode: of the patterns the partitioned and write-pattern sets share,
    four never issue a DELETE at all.
    """

    @pytest.mark.parametrize('table_type', ['partitioned', 'write_pattern'])
    def test_mixed_chaos_without_position_deletes_is_invalid(
        self, table_with_summary, table_type
    ):
        result = validator._validate_delete_files(
            table_with_summary(NO_DELETES),
            {'type': table_type, 'pattern': 'mixed_chaos'},
            'posdelete',
        )
        assert not result['valid']

    @pytest.mark.parametrize('pattern', [
        'insert_heavy', 'gradual_growth', 'batch_replace', 'burst_ops',
        'delete_heavy', 'update_heavy', 'truncate_cycles',
    ])
    @pytest.mark.parametrize('table_type', ['partitioned', 'write_pattern'])
    def test_every_other_pattern_is_valid_without_them(
        self, table_with_summary, table_type, pattern
    ):
        result = validator._validate_delete_files(
            table_with_summary(NO_DELETES),
            {'type': table_type, 'pattern': pattern},
            'posdelete',
        )
        assert result['valid'], result['delete_info']

    def test_a_spec_with_no_pattern_is_valid_without_them(self, table_with_summary):
        # The single-type tables carry no pattern.
        result = validator._validate_delete_files(
            table_with_summary(NO_DELETES),
            {'type': 'single_type', 'data_type': 'int'},
            'posdelete',
        )
        assert result['valid'], result['delete_info']

    def test_mixed_chaos_with_position_deletes_is_valid(self, table_with_summary):
        result = validator._validate_delete_files(
            table_with_summary(POSITION_DELETES),
            {'type': 'partitioned', 'pattern': 'mixed_chaos'},
            'posdelete',
        )
        assert result['valid'], result['delete_info']
