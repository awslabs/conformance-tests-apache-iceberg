"""Tests that the compression level a spec asks for is checked, not just the codec.

The Parquet footer records the codec but not the level it was given, so the level
is read back from the Iceberg table metadata, where every backend writes it as the
`write.parquet.compression-level` property.
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
def table_with_properties(tmp_path):
    """Build a table directory holding just the metadata the check reads."""
    def build(properties, filename='00007-abc.metadata.json'):
        metadata_dir = tmp_path / 'metadata'
        metadata_dir.mkdir(exist_ok=True)
        (metadata_dir / filename).write_text(
            json.dumps({'format-version': 2, 'properties': properties})
        )
        return tmp_path
    return build


class TestCompressionLevel:
    def test_matching_level_is_valid(self, table_with_properties):
        path = table_with_properties({'write.parquet.compression-level': '1'})
        result = validator._validate_compression_level(path, {'compression_level': 1})
        assert result['valid']
        assert result['actual_compression_level'] == '1'

    def test_wrong_level_is_invalid(self, table_with_properties):
        # The failure the codec check cannot see: gzip either way, level 1 not 3.
        path = table_with_properties({
            'write.parquet.compression-codec': 'gzip',
            'write.parquet.compression-level': '1',
        })
        result = validator._validate_compression_level(path, {'compression_level': 3})
        assert not result['valid']
        assert result['expected_compression_level'] == 3
        assert result['actual_compression_level'] == '1'

    def test_missing_level_is_invalid_when_one_is_expected(self, table_with_properties):
        path = table_with_properties({'write.parquet.compression-codec': 'gzip'})
        result = validator._validate_compression_level(path, {'compression_level': 2})
        assert not result['valid']
        assert result['actual_compression_level'] is None

    def test_codecs_without_a_level_expect_the_property_absent(self, table_with_properties):
        # snappy, lz4 and uncompressed take no level, so the property should not
        # be there; one appearing means a level leaked in from somewhere.
        path = table_with_properties({'write.parquet.compression-codec': 'snappy'})
        assert validator._validate_compression_level(path, {'compression_level': None})['valid']

    def test_unexpected_level_is_invalid(self, table_with_properties):
        path = table_with_properties({
            'write.parquet.compression-codec': 'snappy',
            'write.parquet.compression-level': '5',
        })
        assert not validator._validate_compression_level(path, {'compression_level': None})['valid']

    def test_newest_metadata_file_wins(self, table_with_properties):
        # Properties can be reset between commits, so the level in force is the
        # one in the newest metadata file, not the first.
        table_with_properties({'write.parquet.compression-level': '1'}, '00001-a.metadata.json')
        path = table_with_properties({'write.parquet.compression-level': '9'}, '00002-b.metadata.json')
        assert validator._validate_compression_level(path, {'compression_level': 9})['valid']

    def test_newest_wins_for_hadoop_named_metadata(self, table_with_properties):
        # The names PySpark's catalog writes are not zero padded, so v9 sorts
        # after v20 and these tables reach v24. Sorting by name reads a level
        # from partway through the run.
        for version in range(1, 24):
            table_with_properties(
                {'write.parquet.compression-level': '1'}, f'v{version}.metadata.json'
            )
        path = table_with_properties(
            {'write.parquet.compression-level': '3'}, 'v24.metadata.json'
        )
        result = validator._validate_compression_level(path, {'compression_level': 3})
        assert result['valid'], result
        assert result['actual_compression_level'] == '3'

    def test_absent_metadata_is_reported(self, tmp_path):
        result = validator._validate_compression_level(tmp_path, {'compression_level': 1})
        assert not result['valid']
        assert 'error' in result


class TestExpectedSpec:
    def test_level_is_carried_on_the_spec(self):
        """The level has to reach the check as a field, not only in the table name."""
        specs = {
            'primitive_types': [{'name': 'int', 'iceberg_type': 'int'}],
            'compression_configs': [
                {'codec': 'gzip', 'levels': [1, 3]},
                {'codec': 'snappy', 'levels': [None]},
            ],
            'encoding_types': ['PLAIN'],
            'iceberg_versions': [{'version': 2, 'namespace': 'v2'}],
        }
        tables = validator._extract_expected_single_type_tables(specs)
        by_name = {t['name']: t for t in tables}
        assert by_name['unpart_int_gzip_lvl1_plain_v2']['compression_level'] == 1
        assert by_name['unpart_int_gzip_lvl3_plain_v2']['compression_level'] == 3
        assert by_name['unpart_int_snappy_plain_v2']['compression_level'] is None
