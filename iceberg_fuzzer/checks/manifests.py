"""Checks on manifest lists and manifest entries."""

import json

from pyiceberg.manifest import ManifestContent, ManifestEntryStatus
from pyiceberg.table import Table

from iceberg_fuzzer.backend import AssertionResult
from iceberg_fuzzer.checks.base import (
    ICEBERG_JAVA, SPEC, Basis, CommitCheck, NewFilesCheck, TableCheck, file_name, live_entries, live_paths,
)


class ManifestMinimumSequence(TableCheck):
    """Each manifest-list min_sequence_number equals the lowest data sequence number among the
    manifest's live entries, those ADDED or EXISTING. An overstated minimum lets readers that
    use it skip delete files that still apply."""

    name = "manifest_minimum"
    summary = "Each manifest's min_sequence_number equals its lowest live entry's sequence number"
    basis = Basis.SPEC
    source = SPEC + "#manifest-lists"

    def after(self, state, namespace: str, table_name: str) -> list[AssertionResult]:
        table = self._load_table(namespace, table_name)
        snapshot = table.current_snapshot()
        if snapshot is None:
            return [self._not_applicable("no snapshot")]
        wrong = []
        for manifest in snapshot.manifests(table.io):
            live = [
                e.sequence_number
                for e in manifest.fetch_manifest_entry(table.io, discard_deleted=False)
                if e.status in (ManifestEntryStatus.ADDED, ManifestEntryStatus.EXISTING)
            ]
            if live and manifest.min_sequence_number != min(live):
                kind = "delete" if manifest.content == ManifestContent.DELETES else "data"
                wrong.append(
                    f"{kind} manifest {file_name(manifest.manifest_path)} records "
                    f"{manifest.min_sequence_number}, live entries start at {min(live)}"
                )
        return [self._result(wrong, "each min_sequence_number equals its lowest live entry", ok="equal")]


class NewFileEntries(NewFilesCheck):
    """Every file a compaction introduces is recorded by a snapshot the compaction committed: ADDED
    by the last one, or ADDED or carried as EXISTING from an earlier one, since a compaction such as
    Spark's data rewrite followed by its delete rewrite can commit more than one. Only replace
    steps, whose snapshots are all the compaction's."""

    name = "new_file_entries"
    summary = "Every file a compaction adds is recorded as ADDED by a snapshot the compaction committed"
    basis = Basis.SPEC
    source = SPEC + "#manifests"
    operations = ("replace",)

    def before(self, namespace: str, table_name: str) -> tuple[set[str], set[int]]:
        table = self._load_table(namespace, table_name)
        return live_paths(table), {s.snapshot_id for s in table.snapshots()}

    def after(self, state, namespace: str, table_name: str) -> list[AssertionResult]:
        paths_before, snapshots_before = state
        table = self._load_table(namespace, table_name)
        snapshot = table.current_snapshot()
        current = snapshot.snapshot_id if snapshot else None
        committed = {s.snapshot_id for s in table.snapshots()} - snapshots_before

        def recorded(entry) -> bool:
            if entry.snapshot_id == current:
                return entry.status == ManifestEntryStatus.ADDED
            return entry.snapshot_id in committed and entry.status in (ManifestEntryStatus.ADDED, ManifestEntryStatus.EXISTING)

        new = [entry for entry in live_entries(table) if entry.data_file.file_path not in paths_before]
        if not new:
            return [self._not_applicable("no new files")]
        wrong = [
            f"{file_name(entry.data_file.file_path)} is {entry.status.name} with snapshot {entry.snapshot_id}"
            for entry in new if not recorded(entry)
        ]
        return [self._result(wrong, "new files recorded by the snapshots the step committed")]


def _read_long(data: bytes, offset: int) -> tuple[int, int]:
    """One Avro zigzag varint."""
    shift = result = 0
    while True:
        byte = data[offset]
        offset += 1
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return (result >> 1) ^ -(result & 1), offset
        shift += 7


def avro_header(data: bytes) -> dict[str, bytes]:
    """The metadata map at the start of an Avro object container file."""
    if data[:4] != b"Obj\x01":
        raise ValueError("not an Avro object container file")
    metadata, offset = {}, 4
    while True:
        count, offset = _read_long(data, offset)
        if count == 0:
            return metadata
        if count < 0:
            count = -count
            _, offset = _read_long(data, offset)
        for _ in range(count):
            key_length, offset = _read_long(data, offset)
            key = data[offset:offset + key_length].decode()
            offset += key_length
            value_length, offset = _read_long(data, offset)
            metadata[key] = data[offset:offset + value_length]
            offset += value_length


# Manifest list fields the v2 spec marks required, by field ID, with their Avro types.
REQUIRED_V2_MANIFEST_LIST_TYPES = {
    500: "string", 501: "long", 502: "int", 517: "int", 515: "long", 516: "long", 503: "long",
    504: "int", 505: "int", 506: "int", 512: "long", 513: "long", 514: "long",
}


def _manifest_list_header(table: Table) -> tuple[dict[str, bytes], dict] | None:
    snapshot = table.current_snapshot()
    if snapshot is None or table.metadata.format_version != 2:
        return None
    with table.io.new_input(snapshot.manifest_list).open() as stream:
        header = avro_header(stream.read(64 * 1024))
    return header, json.loads(header["avro.schema"])


class ManifestListHeader(CommitCheck):
    """The manifest list a compaction commits has every field the v2 spec requires, with its
    required Avro type, not a union with null."""

    name = "manifest_list_header"
    summary = "The committed manifest list has every field the v2 spec requires, with the required type"
    basis = Basis.SPEC
    source = SPEC + "#manifest-lists"
    operations = ("replace",)

    def _check_commit(self, table: Table) -> list[AssertionResult]:
        parsed = _manifest_list_header(table)
        if parsed is None:
            return [self._not_applicable("not a v2 table with a snapshot")]
        _, schema = parsed
        fields = {field.get("field-id"): field for field in schema["fields"]}
        problems = []
        for field_id, avro_type in REQUIRED_V2_MANIFEST_LIST_TYPES.items():
            field = fields.get(field_id)
            if field is None:
                problems.append(f"required field {field_id} is missing")
            elif field["type"] != avro_type:
                problems.append(f"{field['name']} is {field['type']}, the spec requires {avro_type}")
        return [self._result(problems, "v2 manifest list required fields", ok="as specified")]


class ManifestListConventions(CommitCheck):
    """The manifest list a compaction commits follows the conventions Iceberg Java writes and other
    readers such as iceberg-go rely on: a format-version header of 2 and a partition summary record
    named r508. These are not in the spec, so they are reported apart from it."""

    name = "manifest_list_conventions"
    summary = "The committed manifest list has Iceberg Java's format-version header and r508 record name"
    basis = Basis.ICEBERG_JAVA
    source = ICEBERG_JAVA + "core/src/main/java/org/apache/iceberg/ManifestListWriter.java#L196-L202"
    operations = ("replace",)

    def _check_commit(self, table: Table) -> list[AssertionResult]:
        parsed = _manifest_list_header(table)
        if parsed is None:
            return [self._not_applicable("not a v2 table with a snapshot")]
        header, schema = parsed
        problems = []
        if header.get("format-version") != b"2":
            problems.append(f"format-version header {header.get('format-version')!r}")
        for field in schema["fields"]:
            if field.get("field-id") == 507:
                element = next(t for t in (field["type"] if isinstance(field["type"], list) else [field["type"]]) if t != "null")
                if element["items"].get("name") != "r508":
                    problems.append(f"partition summary record named {element['items'].get('name')}")
        return [self._result(problems, "Iceberg Java's manifest list conventions", ok="as written by Iceberg Java")]
