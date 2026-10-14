"""Unit tests for table checks."""

import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog

from iceberg_fuzzer.checks import ManifestMinimumSequence, PropertiesUnchanged, RetainedSnapshots

ROWS = pa.table({"id": pa.array([1, 2, 3], pa.int64())})


@pytest.fixture
def catalog(tmp_path):
    catalog = SqlCatalog("test", uri=f"sqlite:///{tmp_path}/catalog.db", warehouse=f"file://{tmp_path}")
    catalog.create_namespace("ns")
    table = catalog.create_table("ns.t", schema=ROWS.schema, properties={"format-version": "2"})
    table.append(ROWS)
    table.append(ROWS)
    return catalog


def _load(catalog):
    return lambda ns, name: catalog.load_table(f"{ns}.{name}")


def test_manifest_minimum_holds_for_appends(catalog):
    assert all(a.passed for a in ManifestMinimumSequence(_load(catalog)).after(None, "ns", "t"))


def test_retained_snapshots_pass_until_history_changes(catalog):
    check = RetainedSnapshots(_load(catalog))
    state = check.before("ns", "t")
    assert len(state) == 2 and all(a.passed for a in check.after(state, "ns", "t"))
    first = min(state)
    state[first] = (0, 0)
    assert not all(a.passed for a in check.after(state, "ns", "t"))


def test_properties_unchanged(catalog):
    check = PropertiesUnchanged(_load(catalog))
    state = check.before("ns", "t")
    assert check.after(state, "ns", "t")[0].passed
    with catalog.load_table("ns.t").transaction() as tx:
        tx.set_properties({"owner": "someone"})
    assert not check.after(state, "ns", "t")[0].passed


def test_operations_scope():
    assert ManifestMinimumSequence(None).applies_to("sql")
    assert RetainedSnapshots(None).applies_to("replace") and not RetainedSnapshots(None).applies_to("sql")


def test_manifest_list_header_holds_for_pyiceberg_appends(catalog):
    from iceberg_fuzzer.checks import ManifestListHeader
    assert [a.passed for a in ManifestListHeader(_load(catalog)).after(None, "ns", "t")] == [True]


def _stats(**metrics):
    from types import SimpleNamespace
    keys = ("value_counts", "null_value_counts", "nan_value_counts", "lower_bounds", "upper_bounds")
    return SimpleNamespace(**{k: metrics.get(k, {}) for k in keys})


def _kinds(field, values, data_file, mode):
    from iceberg_fuzzer.checks.statistics import _column_problems
    return [problem.kind for problem in _column_problems("f", field, values, data_file, mode)]


def test_column_statistics_compare_bounds_with_values():
    from pyiceberg.conversions import to_bytes
    from pyiceberg.types import DoubleType, NestedField

    double = NestedField(1, "d", DoubleType())
    values = pa.array([1.0, float("nan"), None, -2.0])
    counts = {"value_counts": {1: 4}, "null_value_counts": {1: 1}, "nan_value_counts": {1: 1}}
    b = lambda v: to_bytes(DoubleType(), v)  # noqa: E731
    exact = _stats(**counts, lower_bounds={1: b(-2.0)}, upper_bounds={1: b(1.0)})
    assert _kinds(double, values, exact, "full") == []
    assert _kinds(double, values, _stats(**counts, lower_bounds={1: b(-2.0)}), "truncate(16)") == ["missing_upper_bound"]
    assert _kinds(double, values, exact, "counts") == ["bounds_in_counts_mode"]
    assert _kinds(double, values, _stats(**counts, lower_bounds={1: b(-1.0)}, upper_bounds={1: b(1.0)}), "truncate(16)") == ["lower_bound"]


def test_present_counts_are_checked_in_every_mode():
    from pyiceberg.types import LongType, NestedField
    field = NestedField(1, "n", LongType())
    values = pa.array([1, None])
    wrong = _stats(value_counts={1: 99}, null_value_counts={1: 0})
    assert sorted(_kinds(field, values, wrong, "none")) == ["counts_in_none_mode", "counts_in_none_mode", "null_count", "value_count"]
    assert _kinds(field, values, _stats(), "counts") == ["missing_value_count", "missing_null_count"]


def test_nan_and_signed_zero_bounds():
    from pyiceberg.conversions import to_bytes
    from pyiceberg.types import DoubleType, NestedField
    field = NestedField(1, "d", DoubleType())
    b = lambda v: to_bytes(DoubleType(), v)  # noqa: E731
    counts = {"value_counts": {1: 2}, "null_value_counts": {1: 0}, "nan_value_counts": {1: 0}}
    finite = pa.array([1.0, 2.0])
    assert "nan_bound" in _kinds(field, finite, _stats(**counts, lower_bounds={1: b(float("nan"))}, upper_bounds={1: b(2.0)}), "truncate(16)")
    zeros = pa.array([-0.0, 0.0])
    reversed_zeros = _stats(**counts, lower_bounds={1: b(0.0)}, upper_bounds={1: b(-0.0)})
    assert _kinds(field, zeros, reversed_zeros, "full") == ["lower_bound", "upper_bound"]
    assert _kinds(field, zeros, _stats(**counts, lower_bounds={1: b(-0.0)}, upper_bounds={1: b(0.0)}), "full") == []


def test_string_upper_bound_may_be_missing_only_when_truncation_cannot_form_it():
    from pyiceberg.conversions import to_bytes
    from pyiceberg.types import NestedField, StringType
    field = NestedField(2, "s", StringType())
    counts = {"value_counts": {2: 2}, "null_value_counts": {2: 0}}
    short = pa.array(["a", "b"])
    no_upper = _stats(**counts, lower_bounds={2: to_bytes(StringType(), "a")})
    assert _kinds(field, short, no_upper, "full") == ["missing_upper_bound"]
    assert _kinds(field, short, no_upper, "truncate(16)") == ["missing_upper_bound"]
    maximal = pa.array(["a", "\U0010ffff" * 20])
    assert _kinds(field, maximal, no_upper, "truncate(16)") == []


def test_long_string_bounds_under_truncation():
    from pyiceberg.conversions import to_bytes
    from pyiceberg.types import NestedField, StringType
    field = NestedField(2, "s", StringType())
    values = pa.array(["a" * 20, "b"])
    truncated = _stats(value_counts={2: 2}, null_value_counts={2: 0},
                       lower_bounds={2: to_bytes(StringType(), "a" * 16)}, upper_bounds={2: to_bytes(StringType(), "c")})
    assert _kinds(field, values, truncated, "truncate(16)") == []
    assert _kinds(field, values, truncated, "full") == ["lower_bound", "upper_bound"]


def test_schema_unchanged_sees_a_rename(catalog):
    from iceberg_fuzzer.checks import TableDefinitionUnchanged
    check = TableDefinitionUnchanged(_load(catalog))
    state = check.before("ns", "t")
    assert all(a.passed for a in check.after(state, "ns", "t"))
    with catalog.load_table("ns.t").update_schema() as update:
        update.rename_column("id", "renamed_id")
    assert not all(a.passed for a in check.after(state, "ns", "t"))


def test_new_files_are_found_by_path_not_by_entry_metadata(catalog):
    from iceberg_fuzzer.checks import NewFileEntries
    check = NewFileEntries(_load(catalog))
    state = check.before("ns", "t")
    catalog.load_table("ns.t").append(ROWS)
    assert all(a.passed for a in check.after(state, "ns", "t"))
    # Files from earlier snapshots that look new must have been committed during the step.
    _, snapshots = state
    assert check.after((set(), snapshots), "ns", "t")[0].passed is False


def test_new_files_from_every_snapshot_the_step_committed_count(catalog):
    from iceberg_fuzzer.checks import NewFileEntries
    check = NewFileEntries(_load(catalog))
    state = check.before("ns", "t")
    catalog.load_table("ns.t").append(ROWS)
    catalog.load_table("ns.t").append(ROWS)
    assert check.after(state, "ns", "t")[0].passed


def test_bounds_written_before_a_promotion_keep_their_width():
    from pyiceberg.conversions import to_bytes
    from pyiceberg.types import DoubleType, FloatType, IntegerType, LongType, NestedField

    counts = {"value_counts": {1: 2}, "null_value_counts": {1: 0}}
    old_ints = _stats(**counts, lower_bounds={1: to_bytes(IntegerType(), -3)}, upper_bounds={1: to_bytes(IntegerType(), 7)})
    assert _kinds(NestedField(1, "n", LongType()), pa.array([-3, 7], pa.int32()), old_ints, "full") == []
    old_floats = _stats(**counts, nan_value_counts={1: 0},
                        lower_bounds={1: to_bytes(FloatType(), -1.5)}, upper_bounds={1: to_bytes(FloatType(), 2.5)})
    assert _kinds(NestedField(1, "x", DoubleType()), pa.array([-1.5, 2.5], pa.float32()), old_floats, "full") == []


def test_columns_are_found_by_field_id_after_a_rename(catalog):
    from iceberg_fuzzer.checks import columns_by_field_id

    table = catalog.load_table("ns.t")
    with table.update_schema() as update:
        update.rename_column("id", "renamed")
    table = catalog.load_table("ns.t")
    with_ids = pa.table({"id": [1]}, schema=pa.schema([pa.field("id", pa.int64(), metadata={"PARQUET:field_id": "1"})]))
    assert list(columns_by_field_id(with_ids, table)) == [1]
    with table.transaction() as tx:
        tx.set_properties({"schema.name-mapping.default": '[{"field-id": 1, "names": ["id", "renamed"]}]'})
    assert list(columns_by_field_id(pa.table({"id": [1]}), catalog.load_table("ns.t"))) == [1]


def test_delete_file_order_holds_every_writer_to_the_spec():
    from iceberg_fuzzer.checks import DeleteFileOrder
    assert all(DeleteFileOrder(None).applies_to(op) for op in ("sql", "replace", "append", "delete"))


def test_delete_file_codec_falls_back_to_the_data_file_codec():
    from iceberg_fuzzer.checks import DeleteFileCodec
    assert DeleteFileCodec._codec({"write.parquet.compression-codec": "gzip"}) == "gzip"
    assert DeleteFileCodec._codec({"write.parquet.compression-codec": "gzip", "write.delete.parquet.compression-codec": "snappy"}) == "snappy"
    assert DeleteFileCodec._codec({}) == "zstd"


def test_data_file_codec_compares_new_files_with_the_table_codec(catalog):
    from iceberg_fuzzer.checks import DataFileCodec
    check = DataFileCodec(_load(catalog))
    state = check.before("ns", "t")
    table = catalog.load_table("ns.t")
    with table.transaction() as tx:
        tx.set_properties({"write.parquet.compression-codec": "gzip"})
    catalog.load_table("ns.t").append(ROWS)
    assert check.after(state, "ns", "t")[0].passed
    with catalog.load_table("ns.t").transaction() as tx:
        tx.set_properties({"write.parquet.compression-codec": "snappy"})
    result = check.after(state, "ns", "t")[0]
    assert not result.passed and "GZIP" in result.actual


def test_metadata_codec_matches_the_metadata_file_to_the_table_property(catalog, tmp_path):
    import gzip
    from types import SimpleNamespace

    from iceberg_fuzzer.checks import MetadataCodec
    assert MetadataCodec(_load(catalog)).after(None, "ns", "t")[0].passed
    io = catalog.load_table("ns.t").io
    compressed = tmp_path / "00001-a.gz.metadata.json"
    compressed.write_bytes(gzip.compress(b"{}"))

    def result(properties, path):
        table = SimpleNamespace(properties=properties, metadata_location=f"file://{path}", io=io)
        return MetadataCodec(lambda ns, name: table).after(None, "ns", "t")[0]

    assert result({"write.metadata.compression-codec": "gzip"}, compressed).passed
    assert not result({}, compressed).passed
    plain = tmp_path / "00002-b.metadata.json"
    plain.write_bytes(b"{}")
    assert not result({"write.metadata.compression-codec": "gzip"}, plain).passed


def test_data_path_location_holds_new_files_to_the_table_data_path(catalog):
    from iceberg_fuzzer.checks import DataPathLocation
    check = DataPathLocation(_load(catalog))
    state = check.before("ns", "t")
    catalog.load_table("ns.t").append(ROWS)
    assert check.after(state, "ns", "t")[0].passed
    location = catalog.load_table("ns.t").location()
    with catalog.load_table("ns.t").transaction() as tx:
        tx.set_properties({"write.data.path": f"{location}/data"})
    assert check.after(state, "ns", "t")[0].passed
    with catalog.load_table("ns.t").transaction() as tx:
        tx.set_properties({"write.data.path": f"{location}/elsewhere"})
    assert not check.after(state, "ns", "t")[0].passed


def test_partition_summaries_follow_the_limit():
    from types import SimpleNamespace

    from iceberg_fuzzer.checks import PartitionSummaries
    from pyiceberg.table.snapshots import Operation, Summary

    def passed(limit, changed, included):
        summary = {"changed-partition-count": str(changed), **({"partition-summaries-included": "true"} if included else {})}
        table = SimpleNamespace(properties={} if limit is None else {"write.summary.partition-limit": str(limit)},
                                current_snapshot=lambda: SimpleNamespace(summary=Summary(Operation.REPLACE, **summary)))
        return PartitionSummaries(None)._check_commit(table)[0].passed

    assert passed(None, 3, False) and not passed(None, 3, True)
    assert passed(10, 3, True) and not passed(10, 3, False)
    assert passed(2, 3, False) and not passed(2, 3, True)


def test_every_check_describes_itself():
    from iceberg_fuzzer.checks import TABLE_CHECKS, Basis
    for check in TABLE_CHECKS.values():
        info = check.info()
        assert info["summary"] and info["source"].startswith("https://") and isinstance(check.basis, Basis), check.name


def test_results_carry_their_check_problem_and_reader():
    from iceberg_fuzzer.backend import AssertionResult
    result = AssertionResult.of("row_count", False, 1, 2, reader="pyspark", row_filter="id < 5")
    assert result.name == "pyspark.row_count where id < 5"
    named = AssertionResult(name="column_statistics.lower_bound", passed=False)
    assert (named.check, named.problem) == ("column_statistics", "lower_bound")
    assert AssertionResult.content_hash(1, 1).for_reader("polars").name == "polars.content_hash"
    assert not AssertionResult.skipped("point_query.c1", "unsupported type").applicable


def test_checks_with_nothing_to_check_are_not_applicable(catalog):
    from iceberg_fuzzer.checks import DataFileCodec, DeleteFileOrder, ManifestListHeader
    for check in (DataFileCodec(_load(catalog)), DeleteFileOrder(_load(catalog)), ManifestListHeader(_load(catalog))):
        state = check.before("ns", "t")
        assert [(a.passed, a.applicable) for a in check.after(state, "ns", "t")] == [(True, False)], check.name
    check = ManifestListHeader(_load(catalog))
    state = check.before("ns", "t")
    catalog.load_table("ns.t").append(ROWS)
    assert all(a.applicable for a in check.after(state, "ns", "t"))
