"""End-to-end tests that `validate` holds a table to what its spec declared.

CI runs `validate` over the generated corpora, which proves the happy path but not
that the checks would fail if a table were wrong. These tests generate a small
corpus, confirm it validates, then break one thing at a time and confirm each break
is reported.

Polars is the backend throughout: it needs no JVM, so a whole generate/validate
cycle is seconds.
"""

import copy
import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

# Two types, one codec, one encoding, one format version: two tables, enough to
# exercise the type check on an integer and on a string.
SPEC = {
    'metadata': {
        'description': 'Subset for spec-fidelity tests',
        'rows_per_table': 1000,
        'rows_per_commit': 100,
        'total_commits': 10,
    },
    'iceberg_versions': [{'version': 2, 'namespace': 'v2'}],
    'compression_configs': [{'codec': 'uncompressed', 'levels': [None]}],
    'encoding_types': ['PLAIN'],
    'format_versions': [2],
    'primitive_types': [
        {'name': 'int', 'iceberg_type': 'int'},
        {'name': 'string_small', 'iceberg_type': 'string', 'max_length': 1},
    ],
}

REPO_ROOT = Path(__file__).resolve().parent.parent


def _load_duckdb_validator():
    """Load the DuckDB validator by path.

    `iceberg_fuzzer/validator.py` takes the `iceberg_fuzzer.validator` name, so the
    `validator/local_duckdb/` directory beneath it is not importable; the CLI runs
    the script as a subprocess instead.
    """
    path = (
        REPO_ROOT / 'iceberg_fuzzer' / 'validator' / 'local_duckdb' / 'validator_duckdb.py'
    )
    spec = importlib.util.spec_from_file_location('validator_duckdb', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


validator = _load_duckdb_validator()


def _duckdb_connection():
    import duckdb
    conn = duckdb.connect()
    conn.execute('INSTALL iceberg')
    conn.execute('LOAD iceberg')
    return conn


def _cli(*args):
    """Run the CLI as the user would, and hand back the completed process."""
    return subprocess.run(
        [sys.executable, '-m', 'iceberg_fuzzer.cli', *args],
        cwd=REPO_ROOT, capture_output=True, text=True, timeout=1800,
    )


def _write_spec(directory, spec):
    # The CLI routes on the specs filename, so it has to contain 'simple' to be
    # treated as a single-type spec.
    path = directory / 'simple_subset.yaml'
    path.write_text(yaml.safe_dump(spec, sort_keys=False))
    return path


def _validate(warehouse, spec_path):
    return _cli('validate', '--warehouse-path', str(warehouse), '--specs', str(spec_path))


@pytest.fixture(scope='module')
def corpus(tmp_path_factory):
    """A generated warehouse plus the spec that produced it."""
    directory = tmp_path_factory.mktemp('fidelity')
    warehouse = directory / 'warehouse'
    spec_path = _write_spec(directory, SPEC)
    result = _cli(
        'generate', '--backend', 'polars',
        '--warehouse-path', str(warehouse), '--specs', str(spec_path),
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return warehouse, spec_path, directory


class TestAsGenerated:
    def test_validates(self, corpus):
        warehouse, spec_path, _ = corpus
        result = _validate(warehouse, spec_path)
        assert result.returncode == 0, result.stdout[-4000:]
        assert 'Invalid tables: 0' in result.stdout
        assert 'Missing tables: 0' in result.stdout


class TestDeclaredTypeIsChecked:
    def test_a_different_declared_type_fails(self, corpus):
        """The tables are unchanged; only what the spec claims they hold changes.

        This is the check that caught the backends writing a type the spec did not
        name, so it has to fail rather than compare the values and shrug.
        """
        warehouse, _, directory = corpus
        mutated = copy.deepcopy(SPEC)
        mutated['primitive_types'] = [
            {'name': 'int', 'iceberg_type': 'long'},  # written as int
            {'name': 'string_small', 'iceberg_type': 'string', 'max_length': 1},
        ]
        target_dir = directory / 'wrong_type'
        target_dir.mkdir(exist_ok=True)
        spec_path = _write_spec(target_dir, mutated)
        result = _validate(warehouse, spec_path)
        assert result.returncode != 0, result.stdout[-4000:]
        # Invalid, not missing: a rename would otherwise turn this into a
        # missing-table test that still passes and checks nothing.
        assert 'Missing tables: 0' in result.stdout, result.stdout[-4000:]
        assert 'Invalid tables: 0' not in result.stdout


class TestContentIsChecked:
    """Every row's value is compared, which the other checks cannot see."""

    def test_wrong_expected_values_fail(self, corpus, tmp_path):
        # `name` drives the expected values and the table name; `iceberg_type`
        # drives the column the backend writes and the declared-type check. A spec
        # naming the type `long` while declaring it `int` therefore generates a
        # table of row_id values and expects row_id * 1000 ones, with the declared
        # type agreeing, so only the content comparison can fail.
        spec = copy.deepcopy(SPEC)
        spec['primitive_types'] = [{'name': 'long', 'iceberg_type': 'int'}]
        spec_path = _write_spec(tmp_path, spec)
        warehouse = tmp_path / 'warehouse'
        generated = _cli(
            'generate', '--backend', 'polars',
            '--warehouse-path', str(warehouse), '--specs', str(spec_path),
        )
        assert generated.returncode == 0, generated.stdout + generated.stderr

        result = _validate(warehouse, spec_path)
        assert result.returncode != 0, result.stdout[-4000:]
        assert 'Missing tables: 0' in result.stdout, result.stdout[-4000:]
        assert 'Content errors' in result.stdout, result.stdout[-4000:]
        assert '✓ Validating column type' in result.stdout, 'the type check should pass'

    def test_matching_values_pass(self, corpus):
        warehouse, spec_path, _ = corpus
        result = _validate(warehouse, spec_path)
        assert result.returncode == 0, result.stdout[-4000:]


class TestFormatVersionIsChecked:
    """The declared format version is compared against the table metadata.

    Driven directly rather than through the CLI because the version string builds
    the expected table name as well as the expectation, so a spec claiming v1
    against v2 tables makes validate report them MISSING and the version check
    never runs. Unlike the type and value checks, this one cannot be isolated
    through a spec file.
    """

    def test_a_v1_claim_against_v2_tables_fails(self, corpus):
        warehouse, _, _ = corpus
        table = warehouse / 'simple' / 'unpart_int_uncompressed_plain_v2'
        spec = {
            'name': 'unpart_int_uncompressed_plain_v2',
            'type': 'single_type',
            'data_type': 'int',
            'iceberg_type': 'int',
            'compression': 'uncompressed',
            'encoding': 'plain',
            'version': 'v1',          # the table on disk is v2
            'expected_rows': 1000,
            'expected_commits': 10,
            'schema_columns': 2,
        }
        conn = _duckdb_connection()
        try:
            result = validator._validate_iceberg_table(conn, str(table), spec)
        finally:
            conn.close()
        metadata = result['metadata_validation']
        assert not metadata['version_valid'], metadata
        assert metadata['expected_version'] == 1
        assert metadata['actual_version'] == 2
        assert not result['valid']

    def test_a_v2_claim_against_v2_tables_passes(self, corpus):
        # The control: same table, same call, only the claimed version differs.
        warehouse, _, _ = corpus
        table = warehouse / 'simple' / 'unpart_int_uncompressed_plain_v2'
        spec = {
            'name': 'unpart_int_uncompressed_plain_v2',
            'type': 'single_type',
            'data_type': 'int',
            'iceberg_type': 'int',
            'compression': 'uncompressed',
            'encoding': 'plain',
            'version': 'v2',
            'expected_rows': 1000,
            'expected_commits': 10,
            'schema_columns': 2,
        }
        conn = _duckdb_connection()
        try:
            result = validator._validate_iceberg_table(conn, str(table), spec)
        finally:
            conn.close()
        assert result['metadata_validation']['version_valid'], result['metadata_validation']
        assert result['valid'], result
