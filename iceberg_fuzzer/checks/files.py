"""Checks on the data and delete files a step writes."""

import pyarrow.parquet as pq
from pyiceberg.manifest import DataFileContent

from iceberg_fuzzer.backend import AssertionResult
from iceberg_fuzzer.checks.base import (
    CONFIGURATION, ICEBERG_JAVA, SPEC, Basis, NewFilesCheck, file_name, live_entries, new_entries,
)


class DeleteFileOrder(NewFilesCheck):
    """Position delete files a step introduces are sorted by file_path, then pos, as the spec
    requires. Every step, so the delete files writers such as Spark produce are held to the same
    rule as a compactor's."""

    name = "delete_file_order"
    summary = "Every new position delete file is sorted by file_path, then pos"
    basis = Basis.SPEC
    source = SPEC + "#position-delete-files"

    def after(self, state, namespace: str, table_name: str) -> list[AssertionResult]:
        table = self._load_table(namespace, table_name)
        entries = new_entries(table, state, DataFileContent.POSITION_DELETES)
        if not entries:
            return [self._not_applicable("no new position delete files")]
        unsorted = []
        for entry in entries:
            rows = self._files.read(table, entry.data_file.file_path)
            keys = list(zip(rows.column("file_path").to_pylist(), rows.column("pos").to_pylist()))
            if keys != sorted(keys):
                unsorted.append(file_name(entry.data_file.file_path))
        return [self._result([f"{name} is unsorted" for name in unsorted], "sorted by file_path, pos", ok="sorted")]


# Parquet codec names by the values write.parquet.compression-codec takes. Iceberg's default is zstd.
_CODECS = {"zstd": "ZSTD", "zstandard": "ZSTD", "snappy": "SNAPPY", "gzip": "GZIP", "uncompressed": "UNCOMPRESSED",
           "none": "UNCOMPRESSED", "lz4": "LZ4_RAW", "brotli": "BROTLI"}


class DataFileCodec(NewFilesCheck):
    """Every data file a compaction writes uses the table's write.parquet.compression-codec."""

    name = "data_file_codec"
    summary = "Every data file a compaction writes uses write.parquet.compression-codec"
    basis = Basis.TABLE_PROPERTY
    source = CONFIGURATION
    operations = ("replace",)
    content = DataFileContent.DATA
    files = "data files"

    @staticmethod
    def _codec(properties: dict) -> str:
        return properties.get("write.parquet.compression-codec", "zstd")

    def after(self, state, namespace: str, table_name: str) -> list[AssertionResult]:
        table = self._load_table(namespace, table_name)
        entries = new_entries(table, state, self.content)
        if not entries:
            return [self._not_applicable(f"no new {self.files}")]
        codec = self._codec(table.properties).strip().lower()
        expected = _CODECS.get(codec, codec.upper())
        wrong = []
        for entry in entries:
            with table.io.new_input(entry.data_file.file_path).open() as stream:
                metadata = pq.read_metadata(stream)
            used = {metadata.row_group(g).column(c).compression for g in range(metadata.num_row_groups) for c in range(metadata.num_columns)}
            if used - {expected}:
                wrong.append(f"{file_name(entry.data_file.file_path)} uses {', '.join(sorted(used))}")
        return [self._result(wrong, f"new {self.files} compressed with {expected}")]


class DeleteFileCodec(DataFileCodec):
    """Every position delete file a compaction writes uses write.delete.parquet.compression-codec,
    or the data file codec when that is unset, as Iceberg Java's Parquet.deleteContext does."""

    name = "delete_file_codec"
    summary = "Every delete file a compaction writes uses write.delete.parquet.compression-codec, else the data file codec"
    source = ICEBERG_JAVA + "parquet/src/main/java/org/apache/iceberg/parquet/Parquet.java#L641-L643"
    content = DataFileContent.POSITION_DELETES
    files = "position delete files"

    @staticmethod
    def _codec(properties: dict) -> str:
        return properties.get("write.delete.parquet.compression-codec") or DataFileCodec._codec(properties)


def _local_path(path: str) -> str:
    return "/" + path.removeprefix("file:").lstrip("/")


class DataPathLocation(NewFilesCheck):
    """Every file a compaction writes lies under write.data.path when the table sets it, as Iceberg
    Java's location providers place them."""

    name = "data_path"
    summary = "Every file a compaction writes lies under write.data.path when the table sets it"
    basis = Basis.TABLE_PROPERTY
    source = ICEBERG_JAVA + "core/src/main/java/org/apache/iceberg/LocationProviders.java#L104-L112"
    operations = ("replace",)

    def after(self, state, namespace: str, table_name: str) -> list[AssertionResult]:
        table = self._load_table(namespace, table_name)
        data_path = table.properties.get("write.data.path")
        if not data_path:
            return [self._not_applicable("no write.data.path")]
        paths = [entry.data_file.file_path for entry in live_entries(table) if entry.data_file.file_path not in state]
        if not paths:
            return [self._not_applicable("no new files")]
        root = _local_path(data_path).rstrip("/") + "/"
        return [self._result([p for p in paths if not _local_path(p).startswith(root)], f"new files under {data_path}")]
