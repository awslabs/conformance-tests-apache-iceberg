"""Unit tests for the PyIceberg validation engine."""

import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog

from iceberg_fuzzer.backend import ColumnFilter
from iceberg_fuzzer.backend.canonical import hash_batches
from iceberg_fuzzer.backend.pyiceberg.validation import PyIcebergValidation

ROWS = pa.table({
    "id": pa.array([1, 2, 3, 4], pa.int64()),
    "region": pa.array(["us", "eu", "us", None], pa.string()),
})


@pytest.fixture
def table(tmp_path):
    catalog = SqlCatalog("test", uri=f"sqlite:///{tmp_path}/catalog.db", warehouse=f"file://{tmp_path}")
    catalog.create_namespace("ns")
    table = catalog.create_table("ns.t", schema=ROWS.schema)
    table.append(ROWS)
    return catalog


def _reader(catalog, *row_filters):
    return PyIcebergValidation(lambda ns, name: catalog.load_table(f"{ns}.{name}"), row_filters)


def test_hash_matches_the_reference(table):
    checkpoint = _reader(table).capture_checkpoint("", "ns", "t")
    assert (checkpoint.row_count, checkpoint.content_hash) == hash_batches(ROWS.to_batches())


def test_filtered_reads_are_captured_and_validated(table):
    reader = _reader(table, "region = 'us'")
    checkpoint = reader.capture_checkpoint("", "ns", "t")
    us = ROWS.filter(pa.compute.equal(ROWS["region"], "us"))
    assert checkpoint.filtered["region = 'us'"] == hash_batches(us.to_batches())
    assert all(a.passed for a in reader.validate(checkpoint, "", "ns", "t"))


def test_validate_fails_when_rows_change(table):
    reader = _reader(table, "region = 'us'")
    checkpoint = reader.capture_checkpoint("", "ns", "t")
    table.load_table("ns.t").delete("id = 1")
    failed = {a.name for a in reader.validate(checkpoint, "", "ns", "t") if not a.passed}
    assert failed == {"row_count", "content_hash", "row_count where region = 'us'", "content_hash where region = 'us'"}


def test_point_query_support(table):
    reader = _reader(table)
    assert reader.filtered_count("", "ns", "t", ColumnFilter.eq("region", "us")) == 2
    assert reader.filtered_count("", "ns", "t", ColumnFilter.null("region")) == 1
    assert reader.get_row_ids("", "ns", "t") == {1, 2, 3, 4}
    assert [name for name, _ in reader.get_columns("", "ns", "t")] == ["region"]
