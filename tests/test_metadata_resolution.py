"""Tests for finding a table's current metadata file and current snapshot.

Both are decided from the file contents rather than from a filename or a list
position, because neither of those orders reliably: two catalogs name metadata
files two ways, and `snapshots` is the set retained rather than a history ending
on the current one.
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
def table(tmp_path):
    """Writes a metadata directory one commit at a time.

    `commit` appends a file named however the caller asks, with the metadata-log
    every real writer builds: the files committed before it, in order.
    """
    metadata_dir = tmp_path / 'metadata'
    metadata_dir.mkdir()
    history = []

    def commit(name, last_updated_ms=None, **extra):
        if last_updated_ms is None:
            last_updated_ms = 1_700_000_000_000 + len(history) * 1000
        document = {
            'format-version': 2,
            'last-updated-ms': last_updated_ms,
            'metadata-log': [
                {'timestamp-ms': entry[1], 'metadata-file': str(entry[0])}
                for entry in history
            ],
            'snapshots': [],
            'current-snapshot-id': None,
        }
        document.update(extra)
        path = metadata_dir / name
        path.write_text(json.dumps(document))
        history.append((path, last_updated_ms))
        return path

    commit.path = tmp_path
    commit.metadata_dir = metadata_dir
    return commit


class TestWhichFileIsCurrent:
    def test_an_unpadded_hadoop_counter_is_ordered_by_number(self, table):
        # The defect this started from: v9 sorts after v20 by name, and the
        # PySpark tables reach the twenties.
        for version in range(1, 25):
            table(f'v{version}.metadata.json')
        assert validator._latest_metadata_file(table.path).name == 'v24.metadata.json'

    def test_a_padded_pyiceberg_counter_resolves_too(self, table):
        for counter in range(11):
            table(f'{counter:05d}-0000ffff-0000-0000-0000-000000000000.metadata.json')
        assert validator._latest_metadata_file(table.path).name.startswith('00010-')

    def test_a_gzipped_hadoop_file_is_not_missed(self, table):
        table('v1.metadata.json')
        latest = table('v2.gz.metadata.json')
        assert validator._latest_metadata_file(table.path) == latest

    def test_the_two_naming_schemes_in_one_directory(self, table):
        # What makes a name rule unfixable rather than just awkward: a table
        # written by Spark and then by PyIceberg, which the workflows allow. No
        # ordering over `v4` and `00000-<uuid>` can say which came last, and a
        # rule preferring the `vN` files picks the stale one.
        for version in range(1, 5):
            table(f'v{version}.metadata.json')
        current = table('00000-0000ffff-0000-0000-0000-000000000000.metadata.json')
        assert validator._latest_metadata_file(table.path) == current

    def test_and_in_the_other_order(self, table):
        table('00000-0000ffff-0000-0000-0000-000000000000.metadata.json')
        current = table('v1.metadata.json')
        assert validator._latest_metadata_file(table.path) == current

    def test_the_current_file_is_the_one_no_log_supersedes(self, table):
        # The rule itself, with the counters deliberately out of step with the
        # order the files were committed in.
        table('v7.metadata.json')
        table('v3.metadata.json')
        current = table('v5.metadata.json')
        assert validator._latest_metadata_file(table.path) == current

    def test_a_truncated_log_still_resolves(self, table):
        # `write.metadata.previous-versions-max` caps the log, so it does not
        # name every earlier file -- only the immediate successor has to.
        paths = [table(f'v{version}.metadata.json') for version in range(1, 8)]
        for path in paths:
            document = json.loads(path.read_text())
            document['metadata-log'] = document['metadata-log'][-1:]
            path.write_text(json.dumps(document))
        assert validator._latest_metadata_file(table.path) == paths[-1]

    def test_an_orphan_from_an_interrupted_commit_loses_the_tie(self, table):
        # Two files no log supersedes, so the timestamp decides.
        table('v1.metadata.json')
        current = table('v2.metadata.json', last_updated_ms=1_700_000_500_000)
        orphan = table.metadata_dir / 'v99.metadata.json'
        orphan.write_text(json.dumps({
            'format-version': 2,
            'last-updated-ms': 1_700_000_100_000,
            'metadata-log': [],
            'snapshots': [],
        }))
        assert validator._latest_metadata_file(table.path) == current

    def test_a_half_written_file_is_reported_not_skipped(self, table):
        # A filesystem catalog is not safe to write to concurrently, so a file
        # that will not parse means something is wrong with the table rather than
        # with the read. Skipping it would answer from an older file and report
        # the table as valid.
        table('v1.metadata.json')
        (table.metadata_dir / 'v2.metadata.json').write_text('{"format-version": 2,')
        with pytest.raises(json.JSONDecodeError):
            validator._latest_metadata_file(table.path)

    def test_no_metadata_directory_is_none(self, tmp_path):
        assert validator._latest_metadata_file(tmp_path) is None

    def test_an_empty_metadata_directory_is_none(self, table):
        assert validator._latest_metadata_file(table.path) is None

    def test_it_agrees_with_the_hadoop_version_hint(self, table):
        # `version-hint.text` is what the Hadoop catalog itself records as
        # current, so it is independent evidence. The resolver does not read it --
        # PyIceberg writes none, and a table PyIceberg wrote to after Spark would
        # have a stale one -- so it serves as the oracle here instead.
        for version in range(1, 15):
            table(f'v{version}.metadata.json')
        (table.metadata_dir / 'version-hint.text').write_text('14\n')
        hint = (table.metadata_dir / 'version-hint.text').read_text().strip()
        assert validator._latest_metadata_file(table.path).name == \
            f'v{hint}.metadata.json'

    def test_a_version_hint_does_not_override_the_contents(self, table):
        # The case it would get wrong: Spark left a hint saying v4, then PyIceberg
        # committed and did not update it.
        for version in range(1, 5):
            table(f'v{version}.metadata.json')
        current = table('00000-0000ffff-0000-0000-0000-000000000000.metadata.json')
        (table.metadata_dir / 'version-hint.text').write_text('4\n')
        assert validator._latest_metadata_file(table.path) == current

    def test_metadata_log_is_optional(self, tmp_path):
        # Optional in the spec, and written by both engines here -- measured, 0 of
        # 42 real tables omit it. A writer that did omit it must still resolve.
        metadata_dir = tmp_path / 'metadata'
        metadata_dir.mkdir()
        for version, updated in ((1, 1000), (9, 1001), (20, 1002)):
            (metadata_dir / f'v{version}.metadata.json').write_text(
                json.dumps({'format-version': 2, 'last-updated-ms': updated})
            )
        assert validator._latest_metadata_file(tmp_path).name == 'v20.metadata.json'

    def test_an_empty_metadata_log_falls_back_to_the_timestamp(self, tmp_path):
        metadata_dir = tmp_path / 'metadata'
        metadata_dir.mkdir()
        for version, updated in ((1, 1000), (9, 1001), (20, 1002)):
            (metadata_dir / f'v{version}.metadata.json').write_text(json.dumps(
                {'format-version': 2, 'last-updated-ms': updated, 'metadata-log': []}
            ))
        assert validator._latest_metadata_file(tmp_path).name == 'v20.metadata.json'

    def test_with_neither_field_the_counter_still_orders(self, tmp_path):
        # Both are optional, so there is a directory with nothing to order by.
        # Falling back to a name sort is what put v9 after v20, so the counter is
        # read as a number instead.
        metadata_dir = tmp_path / 'metadata'
        metadata_dir.mkdir()
        for version in (1, 9, 20):
            (metadata_dir / f'v{version}.metadata.json').write_text(
                json.dumps({'format-version': 2})
            )
        assert validator._latest_metadata_file(tmp_path).name == 'v20.metadata.json'

    def test_the_counter_never_beats_the_log(self, tmp_path):
        # The counter is a tie-break, so it must not bring back the stale answer
        # in the mixed-scheme case even when no file carries a timestamp.
        metadata_dir = tmp_path / 'metadata'
        metadata_dir.mkdir()
        (metadata_dir / 'v4.metadata.json').write_text(json.dumps({'format-version': 2}))
        current = metadata_dir / '00000-0000ffff-0000-0000-0000-000000000000.metadata.json'
        current.write_text(json.dumps({
            'format-version': 2,
            'metadata-log': [{'timestamp-ms': 1, 'metadata-file': 'v4.metadata.json'}],
        }))
        assert validator._latest_metadata_file(tmp_path) == current

    def test_reading_returns_the_resolved_document(self, table):
        table('v1.metadata.json')
        table('v2.metadata.json', format_version_marker='second')
        document = validator._read_table_metadata(table.path)
        assert document['format_version_marker'] == 'second'


def _snapshot(snapshot_id, total_records):
    return {
        'snapshot-id': snapshot_id,
        'summary': {'total-records': str(total_records)},
    }


class TestWhichSnapshotIsCurrent:
    def test_it_is_the_one_the_id_names(self):
        metadata = {
            'snapshots': [_snapshot(11, 100), _snapshot(22, 200)],
            'current-snapshot-id': 22,
        }
        assert validator._current_snapshot(metadata)['snapshot-id'] == 22

    def test_a_rollback_leaves_later_snapshots_in_the_list(self):
        # Measured on a table the shipped rollback workflow had run over: 12
        # snapshots retained, current-snapshot-id 5102367805288580501 holding
        # 1005 records, and snapshots[-1] a different one holding 1003. Taking
        # the last entry reports the table two records smaller than it is.
        metadata = {
            'snapshots': [
                _snapshot(5102367805288580501, 1005),
                _snapshot(3649507814674824603, 1003),
            ],
            'current-snapshot-id': 5102367805288580501,
        }
        current = validator._current_snapshot(metadata)
        assert current['snapshot-id'] == 5102367805288580501
        assert current['summary']['total-records'] == '1005'
        assert current is not metadata['snapshots'][-1]

    def test_no_snapshots_is_none(self):
        assert validator._current_snapshot({'snapshots': []}) is None

    def test_a_table_never_written_to_is_none(self):
        # Created, so it has metadata, but nothing has been committed to it.
        metadata = {'snapshots': [_snapshot(11, 0)], 'current-snapshot-id': None}
        assert validator._current_snapshot(metadata) is None

    def test_an_absent_id_with_no_snapshots_is_none(self):
        # `current-snapshot-id` is optional in the spec and PyIceberg does leave
        # it out -- 2 of 22 metadata files on a freshly generated table, both the
        # create-table file with no snapshots yet. The empty `snapshots` check
        # has to come first for that reason, or reading the id raises.
        assert validator._current_snapshot({'snapshots': []}) is None
        assert validator._current_snapshot({}) is None

    def test_an_id_naming_no_retained_snapshot_is_none(self):
        # Expiry can remove the snapshot an id names. Reporting nothing is right;
        # reporting a different snapshot's counts is not.
        metadata = {'snapshots': [_snapshot(11, 100)], 'current-snapshot-id': 99}
        assert validator._current_snapshot(metadata) is None


class TestTheHadoopCounter:
    def test_it_reads_the_number(self):
        assert validator._hadoop_metadata_version('v20.metadata.json') == 20
        assert validator._hadoop_metadata_version('v9.gz.metadata.json') == 9

    def test_another_scheme_has_none(self):
        # Not a sentinel number, because there is no version to report.
        assert validator._hadoop_metadata_version(
            '00010-0000ffff-0000-0000-0000-000000000000.metadata.json'
        ) is None
        assert validator._hadoop_metadata_version('version-hint.text') is None
