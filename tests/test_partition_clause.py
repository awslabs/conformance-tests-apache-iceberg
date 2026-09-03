"""Tests for the `PARTITIONED BY` clause the partitioned tables are created with.

Covers the clause and the statement it goes into, neither of which needs a Spark
session or a generated table.
"""

import pytest

from iceberg_fuzzer.backend.pyspark.builders import (
    build_create_table_sql,
    build_partition_clause,
    parse_partition_spec,
)


def clause(spec_str):
    return build_partition_clause(parse_partition_spec(spec_str))


@pytest.mark.parametrize('spec_str,expected', [
    # Spark's transform names are not the spec's: `year` is written `years`, and
    # bucket and truncate take the parameter first.
    ('identity(category)', 'PARTITIONED BY (category)'),
    ('bucket[4](id)', 'PARTITIONED BY (bucket(4, id))'),
    ('bucket[32](id)', 'PARTITIONED BY (bucket(32, id))'),
    ('truncate[5](name)', 'PARTITIONED BY (truncate(5, name))'),
    ('year(timestamp)', 'PARTITIONED BY (years(timestamp))'),
    ('month(timestamp)', 'PARTITIONED BY (months(timestamp))'),
    ('day(timestamp)', 'PARTITIONED BY (days(timestamp))'),
    ('hour(timestamp)', 'PARTITIONED BY (hours(timestamp))'),
    ('void(category)', 'PARTITIONED BY (void(category))'),
])
def test_one_transform(spec_str, expected):
    assert clause(spec_str) == expected


@pytest.mark.parametrize('spec_str,expected', [
    (
        'identity(category), bucket[4](id)',
        'PARTITIONED BY (category, bucket(4, id))',
    ),
    (
        'identity(category), month(timestamp), bucket[4](id)',
        'PARTITIONED BY (category, months(timestamp), bucket(4, id))',
    ),
])
def test_several_transforms_keep_their_order(spec_str, expected):
    # The order of fields is part of what the set covers, so it has to survive
    # into the clause rather than being normalised.
    assert clause(spec_str) == expected


def test_an_empty_spec_has_no_clause():
    # The caller appends the clause only when it is non-empty, so an unpartitioned
    # table is still legal SQL.
    assert clause('') == ''
    assert build_partition_clause([]) == ''


def test_an_unrecognised_transform_raises():
    # Dropping it instead would create the table partitioned by less than its
    # name claims, which Spark accepts without complaint.
    with pytest.raises(ValueError, match='quarter'):
        build_partition_clause([('quarter', 'timestamp', None)])


class TestTheClauseReachesTheStatement:
    """Building the clause is not enough: it has to reach the CREATE TABLE.

    A clause left out of the statement creates the table unpartitioned, which
    Spark accepts without complaint.
    """

    SCHEMA = 'id int, category string'
    PROPS = "'write.parquet.compression-codec' = 'snappy'"

    def test_the_clause_is_in_the_statement(self):
        sql = build_create_table_sql(
            'local.partitioned.t', self.SCHEMA, clause('bucket[4](id)'), self.PROPS
        )
        assert 'PARTITIONED BY (bucket(4, id))' in sql

    def test_it_comes_before_the_properties(self):
        # Spark rejects the statement the other way round.
        sql = build_create_table_sql(
            'local.partitioned.t', self.SCHEMA, clause('bucket[4](id)'), self.PROPS
        )
        assert sql.index('PARTITIONED BY') < sql.index('TBLPROPERTIES')

    def test_it_comes_after_the_schema_and_using(self):
        sql = build_create_table_sql(
            'local.partitioned.t', self.SCHEMA, clause('bucket[4](id)'), self.PROPS
        )
        assert sql.index('USING iceberg') < sql.index('PARTITIONED BY')

    def test_every_declared_field_appears(self):
        sql = build_create_table_sql(
            'local.partitioned.t',
            self.SCHEMA,
            clause('identity(category), month(timestamp), bucket[4](id)'),
            self.PROPS,
        )
        for field in ('category', 'months(timestamp)', 'bucket(4, id)'):
            assert field in sql, field

    def test_an_empty_clause_leaves_a_legal_statement(self):
        sql = build_create_table_sql('local.simple.t', self.SCHEMA, '', self.PROPS)
        assert 'PARTITIONED BY' not in sql
        assert sql.index('USING iceberg') < sql.index('TBLPROPERTIES')
