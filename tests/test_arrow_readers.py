"""Readers built on ArrowValidation agree with each other on the same table."""

import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog

from iceberg_fuzzer.backend.polars.validation import PolarsValidation
from iceberg_fuzzer.backend.pyiceberg.validation import PyIcebergValidation

ROWS = pa.table({
    "id": pa.array([1, 2, 3], pa.int64()),
    "region": pa.array(["us", "eu", None]),
    "tags": pa.array([{"a": 1}, None, {"b": 2, "c": 3}], pa.map_(pa.string(), pa.int32())),
    "amount": pa.array([1.5, float("nan"), None]),
})


@pytest.fixture
def load(tmp_path):
    catalog = SqlCatalog("test", uri=f"sqlite:///{tmp_path}/catalog.db", warehouse=f"file://{tmp_path}")
    catalog.create_namespace("ns")
    catalog.create_table("ns.t", schema=ROWS.schema, properties={"format-version": "2"}).append(ROWS)
    return lambda ns, name: catalog.load_table(f"{ns}.{name}")


def test_polars_and_pyiceberg_agree(load):
    filters = ("region = 'us'", "region IS NULL")
    pyiceberg = PyIcebergValidation(load, filters).capture_checkpoint("", "ns", "t")
    polars = PolarsValidation(load, filters).capture_checkpoint("", "ns", "t")
    assert pyiceberg == polars
    assert pyiceberg.row_count == 3 and pyiceberg.filtered["region = 'us'"][0] == 1


def test_filtered_count_and_row_ids(load):
    from iceberg_fuzzer.backend import ColumnFilter
    reader = PolarsValidation(load)
    assert reader.filtered_count("", "ns", "t", ColumnFilter.null("region")) == 1
    assert reader.get_row_ids("", "ns", "t") == {1, 2, 3}


def test_iceberg_rust_agrees_with_pyiceberg(load):
    pytest.importorskip("datafusion")
    pytest.importorskip("pyiceberg_core")
    from iceberg_fuzzer.backend.iceberg_rust.validation import IcebergRustValidation
    filters = ("region = 'us'", "region IS NULL")
    assert IcebergRustValidation(load, filters).capture_checkpoint("", "ns", "t") == PyIcebergValidation(load, filters).capture_checkpoint("", "ns", "t")


def test_daft_agrees_with_pyiceberg(load):
    pytest.importorskip("daft")
    from iceberg_fuzzer.backend.daft.validation import DaftValidation
    filters = ("region = 'us'", "region IS NULL")
    assert DaftValidation(load, filters).capture_checkpoint("", "ns", "t") == PyIcebergValidation(load, filters).capture_checkpoint("", "ns", "t")
