"""create_partitioned_table has to create the table with the partitioning it is given.

The tests create each table through it against a real Spark session and read back
the partition spec Iceberg recorded. The session stops the function once its CREATE
TABLE has run, so no rows are written. They need a JVM, like
test_table_builder_integration.py.
"""

import json
from pathlib import Path

import pytest

from iceberg_fuzzer.backend.pyspark.builders import (
    create_partitioned_table,
    create_spark_session,
)


class TableCreated(BaseException):
    """Raised once the CREATE TABLE has run.

    A BaseException, so that the `except Exception` in create_partitioned_table
    lets it through instead of reporting a failed table.
    """


class StopAfterCreateTable:
    """A real Spark session that stops its caller once a CREATE TABLE has run."""

    def __init__(self, spark):
        self._spark = spark

    def __getattr__(self, name):
        return getattr(self._spark, name)

    def sql(self, statement, *args, **kwargs):
        result = self._spark.sql(statement, *args, **kwargs)
        if statement.lstrip().upper().startswith("CREATE TABLE"):
            raise TableCreated
        return result


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    session = create_spark_session(str(tmp_path_factory.mktemp("warehouse")))
    yield session
    session.stop()


def recorded_partitioning(spark, table_name):
    """(transform, source column) for each field of the table's current spec."""
    warehouse = Path(spark.conf.get("spark.sql.catalog.local.warehouse"))
    metadata_dir = warehouse / "partitioned" / table_name / "metadata"
    (metadata_file,) = metadata_dir.glob("v*.metadata.json")
    metadata = json.loads(metadata_file.read_text())
    columns = {
        field["id"]: field["name"]
        for schema in metadata["schemas"]
        for field in schema["fields"]
    }
    (spec,) = [
        spec
        for spec in metadata["partition-specs"]
        if spec["spec-id"] == metadata["default-spec-id"]
    ]
    return [
        (field["transform"], columns[field["source-id"]]) for field in spec["fields"]
    ]


@pytest.mark.parametrize(
    "table_name,partition_spec,expected",
    [
        (
            "part_identity_category_mixed_chaos_posdelete",
            "identity(category)",
            [("identity", "category")],
        ),
        (
            "part_bucket4_id_mixed_chaos_cow",
            "bucket[4](id)",
            [("bucket[4]", "id")],
        ),
        (
            "part_month_timestamp_mixed_chaos_posdelete",
            "month(timestamp)",
            [("month", "timestamp")],
        ),
        (
            "part_identity_category_bucket4_id_mixed_chaos_cow",
            "identity(category),bucket[4](id)",
            [("identity", "category"), ("bucket[4]", "id")],
        ),
    ],
)
def test_the_table_is_created_with_its_partitioning(
    spark, table_name, partition_spec, expected
):
    with pytest.raises(TableCreated):
        create_partitioned_table(
            StopAfterCreateTable(spark),
            spark.conf.get("spark.sql.catalog.local.warehouse"),
            table_name,
            "mixed_chaos",
            partition_spec,
            delete_existing=True,
        )
    assert recorded_partitioning(spark, table_name) == expected
