"""
Tests that `timestamptz` handling does not follow the host timezone.
"""

import importlib.util
import os
import time
from contextlib import contextmanager
from datetime import timezone
from math import gcd
from pathlib import Path

import pytest

from iceberg_fuzzer.backend.polars.generator import PolarsGenerator
from iceberg_fuzzer.backend.pyiceberg.builders import generate_sample_data
from iceberg_fuzzer.backend.pyspark.builders import (
    generate_partitioned_data,
    generate_spark_data,
)
from iceberg_fuzzer.backend.pyspark.data import generate_value as spark_value
from iceberg_fuzzer.common.timestamps import (
    TIMESTAMPTZ_OFFSETS,
    timestamp_value,
    timestamptz_value,
)
from iceberg_fuzzer.exhaustive.dimensions.data_types import generate_value


def _load_duckdb_validator():
    """Load the DuckDB validator by path.

    `iceberg_fuzzer/validator.py` takes the `iceberg_fuzzer.validator` name, so
    the `validator/local_duckdb/` directory beneath it is not importable; the CLI
    runs the script as a subprocess instead.
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

HOST_ZONES = ('UTC', 'America/Los_Angeles', 'Asia/Tokyo', 'Asia/Kolkata')

ROW_IDS = (1, 2, 3, 7, 17, 23, 24, 170)

requires_tzset = pytest.mark.skipif(
    not hasattr(time, 'tzset'), reason='setting the host zone needs time.tzset'
)


@contextmanager
def host_timezone(name):
    """Run the block as though the machine were set to `name`."""
    before = os.environ.get('TZ')
    os.environ['TZ'] = name
    time.tzset()
    try:
        yield
    finally:
        if before is None:
            os.environ.pop('TZ', None)
        else:
            os.environ['TZ'] = before
        time.tzset()


class _CapturingSpark:
    """Stands in for a session: the rows are built before Spark sees them."""

    def createDataFrame(self, data, schema=None):
        self.data = data


def _single_column_spec(iceberg_type):
    return {'schema': [{'name': 'value', 'type': iceberg_type}]}


def _pyiceberg(row_id):
    table = generate_sample_data(_single_column_spec('timestamptz'), 1, row_id - 1)
    return table.column('value').to_pylist()[0]


def _polars(row_id):
    generator = PolarsGenerator.__new__(PolarsGenerator)
    return generator._generate_polars_data('value', 'timestamptz', 1, row_id)[0]


def _pyspark(row_id):
    spark = _CapturingSpark()
    generate_spark_data(spark, _single_column_spec('timestamptz'), 1, row_id - 1)
    return spark.data[0]['value']


def _partitioned_rows(count, row_offset=0):
    """The rows the partitioned writer builds.

    It derives all 30 columns from the row id, and uses the spec only to build a
    Spark schema that the stub discards -- so the spec here is a placeholder, not
    a selection of which column to generate.
    """
    spark = _CapturingSpark()
    generate_partitioned_data(spark, _single_column_spec('int'), count, row_offset)
    return spark.data


def _partitioned(row_id):
    return _partitioned_rows(1, row_id - 1)[0]['timestamptz_col']


def _pyiceberg_naive(row_id):
    table = generate_sample_data(_single_column_spec('timestamp'), 1, row_id - 1)
    return table.column('value').to_pylist()[0]


def _polars_naive(row_id):
    generator = PolarsGenerator.__new__(PolarsGenerator)
    return generator._generate_polars_data('value', 'timestamp', 1, row_id)[0]


def _pyspark_naive(row_id):
    spark = _CapturingSpark()
    generate_spark_data(spark, _single_column_spec('timestamp'), 1, row_id - 1)
    return spark.data[0]['value']


def _partitioned_naive(row_id):
    return _partitioned_rows(1, row_id - 1)[0]['timestamp_col']


TIMESTAMPTZ_PRODUCERS = {
    # What `validate` compares a read value against.
    'validate': lambda row_id: validator._generate_primitive_data('timestamptz', row_id),
    'pyspark': _pyspark,
    'pyiceberg': _pyiceberg,
    'polars': _polars,
    'partitioned': _partitioned,
    # Reached through MutationEngine by a workflow `append` step
    'workflow append': lambda row_id: spark_value('timestamptz_col', 'timestamp', row_id),
    # build-shapes writes and reads its own corpus with its own formula, which adds
    # sub-second variation so a row can be identified by its value. Only the zone
    # has to agree.
    'build-shapes': lambda row_id: generate_value('timestamptz', row_id),
}

# The same producers for Iceberg's zone-free `timestamp`, which is a wall clock
# and has to stay naive. Spark names this one `timestamp_ntz`.
TIMESTAMP_PRODUCERS = {
    'validate': lambda row_id: validator._generate_primitive_data('timestamp', row_id),
    'pyspark': _pyspark_naive,
    'pyiceberg': _pyiceberg_naive,
    'polars': _polars_naive,
    'partitioned': _partitioned_naive,
    'workflow append': lambda row_id: spark_value('timestamp_col', 'timestamp_ntz', row_id),
    'build-shapes': lambda row_id: generate_value('timestamp', row_id),
}

# build-shapes advances seconds and microseconds from the epoch rather than the
# hour of one day, so a row is identifiable by its value alone. That is a
# different formula deliberately: its zone has to agree, and its instant has to
# be the zone-free value's (see test_build_shapes_keeps_the_instant_whatever_the_zone).
OWN_FORMULA = {'build-shapes'}


@pytest.mark.parametrize('name', sorted(TIMESTAMPTZ_PRODUCERS))
class TestEveryProducerAgrees:
    def test_the_value_carries_the_zone_for_its_row(self, name):
        produce = TIMESTAMPTZ_PRODUCERS[name]
        for row_id in ROW_IDS:
            value = produce(row_id)
            assert value.tzinfo is not None, (name, row_id)
            if name in OWN_FORMULA:
                assert value.utcoffset() == timestamptz_value(row_id).utcoffset()
            else:
                assert value == timestamptz_value(row_id), (name, row_id)

    @requires_tzset
    def test_same_instant_regardless_of_host_timezone(self, name):
        # The instant, not the wall clock, is what has to hold: a writer turns the
        # value into one and a reader turns it back, and either step reads the host
        # zone if the value does not carry one.
        produce = TIMESTAMPTZ_PRODUCERS[name]
        instants = {}
        for zone in HOST_ZONES:
            with host_timezone(zone):
                instants[zone] = produce(7).timestamp()
        assert len(set(instants.values())) == 1, (name, instants)


@pytest.mark.parametrize('name', sorted(TIMESTAMP_PRODUCERS))
class TestEveryProducerLeavesTheZoneFreeTypeAlone:
    def test_the_value_is_naive(self, name):
        for row_id in ROW_IDS:
            assert TIMESTAMP_PRODUCERS[name](row_id).tzinfo is None, (name, row_id)

    def test_the_value_matches_the_shared_formula(self, name):
        if name in OWN_FORMULA:
            pytest.skip('generates its own values, see OWN_FORMULA')
        for row_id in ROW_IDS:
            assert TIMESTAMP_PRODUCERS[name](row_id) == timestamp_value(row_id), (name, row_id)

    @requires_tzset
    def test_the_wall_clock_does_not_follow_the_host(self, name):
        # It carries no zone, so nothing should convert it -- but a writer that
        # went via an instant and back would shift it.
        wall_clocks = {}
        for zone in HOST_ZONES:
            with host_timezone(zone):
                wall_clocks[zone] = TIMESTAMP_PRODUCERS[name](7)
        assert len(set(wall_clocks.values())) == 1, (name, wall_clocks)


def test_build_shapes_keeps_the_instant_whatever_the_zone():
    # The stored value is the instant, so it must not depend on the zone. The
    # padding ids are negative and large; seven of them cover every offset.
    for row_id in ROW_IDS + tuple(-(i + 1_000_000) for i in range(7)):
        instant = generate_value('timestamptz', row_id).astimezone(timezone.utc)
        assert instant.replace(tzinfo=None) == generate_value('timestamp', row_id), row_id


def test_the_count_is_coprime_with_the_hour_cycle():
    # The hour of day cycles every 24 rows. A shared factor would advance the
    # hour and the offset in step, so most pairings would never be generated.
    assert gcd(len(TIMESTAMPTZ_OFFSETS), 24) == 1


def test_validate_expects_what_the_partitioned_writer_writes():
    for row in _partitioned_rows(200):
        expected = validator._generate_expected_partitioned_data(row['id'])
        for column in ('timestamp_col', 'timestamptz_col'):
            assert row[column] == expected[column], (column, row['id'])


def test_the_partitioned_values_vary_by_row():
    rows = _partitioned_rows(200)
    assert len({row['timestamp_col'] for row in rows}) > 1
    assert len({row['timestamptz_col'] for row in rows}) > 1
