"""Tests that every engine's canonical cast renders a value the same way.

Cross-engine content hashing compares the strings these expressions produce, so
two engines that render the same value differently report a hash mismatch that
looks like a data defect. The timestamp types are where that happens: an instant
has no single wall clock, and each engine picks one.

The PySpark side runs in a subprocess (tests/spark_canonical_probe.py). Spark
allows one SparkContext per process and `getOrCreate` returns whatever exists
ignoring the config asked for, so a session created here would be handed to the
modules that need an Iceberg catalog and they would fail -- measured: 279 passed
became 91 failed, every one of them a Spark test that passes on its own.
"""

import hashlib
import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pytest

from iceberg_fuzzer.backend.duckdb.canonical import DuckDBCanonicalCast

# Zones either side of UTC, one at a non-whole-hour offset, so an expression that
# followed the session zone would shift by a different amount in each.
SESSION_ZONES = ('UTC', 'Asia/Tokyo', 'America/Los_Angeles', 'Pacific/Chatham')

# One per offset the generated values use, including the quarter hour and the two
# past twelve hours, where the UTC date is not the local date.
INSTANTS = [
    datetime(2023, 1, 1, 1, 0, tzinfo=timezone(timedelta(hours=9))),
    datetime(2023, 1, 1, 7, 0, tzinfo=timezone(timedelta(hours=-8))),
    datetime(2023, 1, 1, 2, 0, tzinfo=timezone(timedelta(hours=5, minutes=30))),
    datetime(2023, 1, 1, 4, 0, tzinfo=timezone(timedelta(hours=-9, minutes=-30))),
    datetime(2023, 1, 1, 3, 0, tzinfo=timezone(timedelta(hours=12, minutes=45))),
    datetime(2023, 1, 1, 5, 0, tzinfo=timezone(timedelta(hours=14))),
    datetime(2023, 1, 1, 0, 0, tzinfo=timezone.utc),
]

WALL_CLOCKS = [datetime(2023, 1, 1, hour, 0) for hour in (0, 1, 13, 23)]


def _in_utc(values):
    return [
        value.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')
        for value in values
    ]


def _duckdb_canonical(values, duckdb_type):
    expr = DuckDBCanonicalCast().cast('v', duckdb_type)
    conn = duckdb.connect()
    try:
        rows = ', '.join(f"(CAST(? AS {duckdb_type}))" for _ in values)
        return [
            row[0] for row in conn.execute(
                f"SELECT {expr} FROM (VALUES {rows}) AS t(v)", list(values)
            ).fetchall()
        ]
    finally:
        conn.close()


@pytest.fixture(scope='module')
def spark_rendered():
    """Every PySpark rendering the tests need, from one subprocess."""
    probe = Path(__file__).resolve().parent / 'spark_canonical_probe.py'
    request = json.dumps({
        'zones': list(SESSION_ZONES),
        'instants': [value.isoformat() for value in INSTANTS],
        'wall_clocks': [value.isoformat() for value in WALL_CLOCKS],
    })
    finished = subprocess.run(
        [sys.executable, str(probe)], input=request,
        capture_output=True, text=True, timeout=600,
    )
    marker = [
        line for line in finished.stdout.splitlines()
        if line.startswith('CANONICAL_PROBE ')
    ]
    if not marker:
        pytest.fail(
            f'the Spark probe produced no result\nstdout:\n{finished.stdout[-2000:]}'
            f'\nstderr:\n{finished.stderr[-2000:]}'
        )
    return json.loads(marker[-1][len('CANONICAL_PROBE '):])


class TestTimestamptz:
    """Spark's TimestampType is Iceberg's timestamptz: an instant."""

    def test_duckdb_renders_utc(self):
        # The form the base class documents, so it is the one to agree with.
        assert _duckdb_canonical(INSTANTS, 'TIMESTAMPTZ') == _in_utc(INSTANTS)

    @pytest.mark.parametrize('zone', SESSION_ZONES)
    def test_spark_does_not_follow_the_session_zone(self, spark_rendered, zone):
        assert spark_rendered['timestamptz'][zone] == _in_utc(INSTANTS), zone

    def test_spark_agrees_with_duckdb_in_every_zone(self, spark_rendered):
        expected = _duckdb_canonical(INSTANTS, 'TIMESTAMPTZ')
        for zone in SESSION_ZONES:
            assert spark_rendered['timestamptz'][zone] == expected, zone

    def test_a_row_hash_agrees_across_engines(self, spark_rendered):
        # What the mismatch actually surfaced as: the per-row hash the workflow
        # compares, not the rendered string.
        def hashes(rendered):
            return [hashlib.sha256(value.encode()).hexdigest() for value in rendered]

        expected = hashes(_duckdb_canonical(INSTANTS, 'TIMESTAMPTZ'))
        for zone in SESSION_ZONES:
            assert hashes(spark_rendered['timestamptz'][zone]) == expected, zone


class TestTimestampWithoutZone:
    """Spark's TimestampNTZType is Iceberg's `timestamp`: a wall clock."""

    def test_the_wall_clock_is_rendered_as_written(self, spark_rendered):
        assert spark_rendered['timestamp_ntz'] == [
            value.strftime('%Y-%m-%d %H:%M:%S.%f') for value in WALL_CLOCKS
        ]

    def test_spark_agrees_with_duckdb(self, spark_rendered):
        assert spark_rendered['timestamp_ntz'] == \
            _duckdb_canonical(WALL_CLOCKS, 'TIMESTAMP')
