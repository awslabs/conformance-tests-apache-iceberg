"""Checks on the table's state, history and metadata file around a step."""

from pyiceberg.table import Table

from iceberg_fuzzer.backend import AssertionResult
from iceberg_fuzzer.backend.canonical import hash_batches
from iceberg_fuzzer.checks.base import ICEBERG_JAVA, Basis, CommitCheck, TableCheck, file_name

REWRITE_FILES = ICEBERG_JAVA + "api/src/main/java/org/apache/iceberg/RewriteFiles.java#L26-L29"


class RetainedSnapshots(TableCheck):
    """Every snapshot that existed before the step still reads the same rows after it."""

    name = "retained_snapshot"
    summary = "Every snapshot that existed before a compaction reads the same rows after it"
    basis = Basis.COMPACTION
    source = REWRITE_FILES
    operations = ("replace",)

    def _hashes(self, table: Table, snapshot_ids) -> dict[int, tuple[int, int]]:
        return {
            sid: hash_batches(table.scan(snapshot_id=sid).to_arrow().to_batches())
            for sid in snapshot_ids
        }

    def before(self, namespace: str, table_name: str):
        table = self._load_table(namespace, table_name)
        return self._hashes(table, [s.snapshot_id for s in table.metadata.snapshots])

    def after(self, state, namespace: str, table_name: str) -> list[AssertionResult]:
        now = self._hashes(self._load_table(namespace, table_name), state)
        changed = [f"snapshot {sid}: rows and hash {state[sid]} before, {now[sid]} after" for sid in state if state[sid] != now[sid]]
        return [self._result(changed, f"{len(state)} snapshots read the same rows", ok="unchanged")]


class PropertiesUnchanged(TableCheck):
    """A compaction leaves every table property as it was."""

    name = "properties"
    summary = "A compaction leaves every table property as it was"
    basis = Basis.COMPACTION
    source = REWRITE_FILES
    operations = ("replace",)

    def before(self, namespace: str, table_name: str):
        return dict(self._load_table(namespace, table_name).properties)

    def after(self, state, namespace: str, table_name: str) -> list[AssertionResult]:
        now = dict(self._load_table(namespace, table_name).properties)
        return [AssertionResult.of(self.name, state == now, state, now)]


class TableDefinitionUnchanged(TableCheck):
    """A compaction leaves the table's schema and partition spec as they were: field IDs, names,
    types, nullability and the current schema and spec IDs."""

    name = "table_definition"
    summary = "A compaction leaves the schema, the partition spec and their IDs as they were"
    basis = Basis.COMPACTION
    source = REWRITE_FILES
    operations = ("replace",)

    @staticmethod
    def _state(table: Table) -> dict:
        return {
            "schema": (table.metadata.current_schema_id, table.schema().model_dump_json()),
            "partition_spec": (table.metadata.default_spec_id, table.spec().model_dump_json()),
        }

    def before(self, namespace: str, table_name: str):
        return self._state(self._load_table(namespace, table_name))

    def after(self, state, namespace: str, table_name: str) -> list[AssertionResult]:
        now = self._state(self._load_table(namespace, table_name))
        return [AssertionResult.of(self.name, state[part] == now[part], state[part], now[part], problem=part) for part in state]


class MetadataCodec(CommitCheck):
    """The metadata file a compaction commits follows write.metadata.compression-codec: gzip
    content under a .gz.metadata.json name for gzip, plain JSON otherwise, as Iceberg Java names
    and writes it."""

    name = "metadata_codec"
    summary = "The committed metadata file is gzip under a .gz.metadata.json name exactly when write.metadata.compression-codec is gzip"
    basis = Basis.TABLE_PROPERTY
    source = ICEBERG_JAVA + "core/src/main/java/org/apache/iceberg/TableMetadataParser.java#L128-L146"
    operations = ("replace",)

    def _check_commit(self, table: Table) -> list[AssertionResult]:
        gzip = table.properties.get("write.metadata.compression-codec", "none").strip().lower() == "gzip"
        with table.io.new_input(table.metadata_location).open() as stream:
            compressed = stream.read(2) == b"\x1f\x8b"
        named = table.metadata_location.endswith(".gz.metadata.json")
        expected = "gzip content in a .gz.metadata.json file" if gzip else "plain JSON in a .metadata.json file"
        actual = f"{'gzip' if compressed else 'plain'} content in {file_name(table.metadata_location)}"
        return [AssertionResult.of(self.name, compressed == named == gzip, expected, actual)]


class PartitionSummaries(CommitCheck):
    """A compaction's snapshot summary includes per-partition summaries exactly when its changed
    partitions number at most write.summary.partition-limit, as Iceberg Java's SnapshotSummary
    does. The limit defaults to 0, which leaves them out."""

    name = "partition_summaries"
    summary = "A compaction's snapshot summary has partition summaries exactly when its changed partitions are within write.summary.partition-limit"
    basis = Basis.TABLE_PROPERTY
    source = ICEBERG_JAVA + "core/src/main/java/org/apache/iceberg/SnapshotSummary.java#L202-L203"
    operations = ("replace",)

    def _check_commit(self, table: Table) -> list[AssertionResult]:
        snapshot = table.current_snapshot()
        summary = snapshot.summary.additional_properties if snapshot and snapshot.summary else {}
        if "changed-partition-count" not in summary:
            return [AssertionResult.of(self.name, False, "a changed-partition-count in the summary", "none", problem="changed_partition_count")]
        limit = int(table.properties.get("write.summary.partition-limit", "0"))
        changed = int(summary["changed-partition-count"])
        included = summary.get("partition-summaries-included") == "true"
        expected = 0 < changed <= limit
        return [AssertionResult.of(
            self.name, included == expected,
            f"partition summaries {'included' if expected else 'left out'} for {changed} changed partitions and limit {limit}",
            "included" if included else "left out",
        )]
