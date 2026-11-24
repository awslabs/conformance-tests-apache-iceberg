"""Protocol interfaces for schema and write-options providers.

Defines structural (duck-typed) interfaces using typing.Protocol so that
conforming classes do NOT need to inherit from a base class. Both protocols
are @runtime_checkable, enabling isinstance() checks at runtime.

Also provides the concrete WriteOptions class that bundles an EncodingChoice
and a CompressionChoice, implementing the WriteOptionsProvider protocol with
compatibility validation and safe fallback logic.

Usage pattern:
    from iceberg_conformance.exhaustive.dimensions import SchemaProvider, WriteOptionsProvider
    from iceberg_conformance.exhaustive.dimensions import WriteOptions

    class MySchema:
        def get_spark_ddl(self) -> str: ...
        def get_field_ids(self) -> Dict[tuple, int]: ...
        def get_arrow_schema(self, field_ids) -> pa.Schema: ...
        def generate_value(self, column_name: str, row_id: int) -> Any: ...

    assert isinstance(MySchema(), SchemaProvider)  # True — structural match

    opts = WriteOptions(encoding=my_encoding, compression=my_compression, schema={"value": "int"})
    assert isinstance(opts, WriteOptionsProvider)  # True
"""

from typing import Any, Dict, List, Optional, Protocol, runtime_checkable

import pyarrow as pa


@runtime_checkable
class SchemaProvider(Protocol):
    """Protocol for classes that provide table schema information.

    Implementors supply Spark DDL for table creation, a PyArrow schema
    (with PARQUET:field_id metadata) for file writing, and a value
    generator for populating rows with type-appropriate data.
    """

    def get_spark_ddl(self) -> str:
        """Return Spark SQL DDL for CREATE TABLE column definitions.

        Example return values:
            "value INT NOT NULL"
            "col_0 INT NOT NULL, col_1 STRING NOT NULL"
        """
        ...

    def get_field_ids(self) -> Dict[tuple, int]:
        """Return the field-id map (field path -> id) for this schema.

        The default 'compute' source of field ids. Callers may ignore it and
        supply their own map to get_arrow_schema (e.g. harvested from a created
        Iceberg table) — that substitution is the field-id seam.
        """
        ...

    def get_arrow_schema(self, field_ids: Dict[tuple, int]) -> pa.Schema:
        """Return PyArrow schema, stamping the supplied field-id map.

        Each field carries metadata {'PARQUET:field_id': '<id>'} taken from
        field_ids (keyed by field path). The provider stamps the ids it is
        given; it does not assign them.
        """
        ...

    def generate_value(self, column_name: str, row_id: int) -> Any:
        """Generate a typed value for a given column and row.

        Args:
            column_name: Name of the column to generate a value for.
            row_id: The row identifier (used as seed for deterministic generation).

        Returns:
            A value of the appropriate type for the column (int, str, date, etc.).
        """
        ...


@runtime_checkable
class WriteOptionsProvider(Protocol):
    """Protocol for classes that provide Parquet write options.

    Implementors supply per-column compression and encoding settings,
    plus a compatibility validator that checks for incompatible combinations
    (e.g., DELTA_BINARY_PACKED on a STRING column).
    """

    def get_compression(self, column_name: str) -> Optional[str]:
        """Return compression codec name for a column, or None for writer default.

        Examples: "ZSTD", "SNAPPY", "GZIP", "LZ4", "BROTLI", None
        """
        ...

    def get_encoding(self, column_name: str) -> Optional[str]:
        """Return encoding name for a column, or None for writer default.

        Examples: "PLAIN", "DICTIONARY", "DELTA_BINARY_PACKED", None
        """
        ...

    def validate_compatibility(self) -> List[str]:
        """Validate encoding/compression/type compatibility.

        Returns:
            List of warning messages for incompatible combinations.
            Empty list means all combinations are valid.
        """
        ...


# ---------------------------------------------------------------------------
# Concrete implementation: WriteOptions
# ---------------------------------------------------------------------------

# Import siblings lazily to avoid circular imports at module level.
# These are only needed by WriteOptions (the concrete class), not by the
# Protocol definitions above which are purely structural.
from .encoding import EncodingChoice, is_compatible as _encoding_is_compatible
from .compression import CompressionChoice, is_compression_compatible as _compression_is_compatible


class WriteOptions:
    """Bundles encoding and compression choices, implementing WriteOptionsProvider.

    Provides per-column compression and encoding lookup, plus compatibility
    validation (encoding×type, encoding×compression) via validate_compatibility().
    Incompatible combinations are surfaced as messages for the caller to act on
    (the CLI gate skips them; the builder raises) — there is no silent fallback.

    Args:
        encoding: Optional EncodingChoice mapping column names to encoding names.
                  If None, writer defaults apply (no explicit encoding set).
        compression: Optional CompressionChoice mapping column names to codecs.
                     If None, writer defaults apply (no explicit compression set).
        schema: Optional dict mapping column_name → Iceberg type name (e.g.,
                {"value": "int", "name": "string"}). Required for
                validate_compatibility() to check encoding×type compatibility.
    """

    def __init__(
        self,
        encoding: Optional[EncodingChoice] = None,
        compression: Optional[CompressionChoice] = None,
        schema: Optional[Dict[str, str]] = None,
    ):
        self.encoding = encoding
        self.compression = compression
        self.schema = schema or {}

    # ------------------------------------------------------------------
    # WriteOptionsProvider protocol methods
    # ------------------------------------------------------------------

    def get_compression(self, column_name: str) -> Optional[str]:
        """Return compression codec name for a column, or None for writer default.

        Looks up the codec from the CompressionChoice. If compression is not
        configured, returns None (letting the writer use its default).

        Args:
            column_name: Name of the column.

        Returns:
            Codec name string (e.g., "ZSTD", "SNAPPY") or None.
        """
        if self.compression is None:
            return None
        return self.compression.get_codec(column_name).value

    def get_encoding(self, column_name: str) -> Optional[str]:
        """Return encoding name for a column, or None for writer default.

        Looks up the encoding from the EncodingChoice. If encoding is not
        configured, returns None (letting the writer use its default).

        Args:
            column_name: Name of the column.

        Returns:
            Encoding name string (e.g., "DICTIONARY", "PLAIN") or None.
        """
        if self.encoding is None:
            return None
        return self.encoding.column_encodings.get(column_name)

    def validate_compatibility(self) -> List[str]:
        """Validate encoding/compression/type compatibility for all columns.

        Checks two compatibility dimensions:
        1. Encoding × Type: Is the chosen encoding valid for the column's type?
           (e.g., DELTA_BINARY_PACKED is invalid for STRING columns)
        2. Encoding × Compression: Is the compression codec compatible with
           the encoding? (e.g., BROTLI may be incompatible with BYTE_STREAM_SPLIT)

        Returns:
            List of warning messages for incompatible combinations.
            Empty list means all combinations are valid.
        """
        warnings: List[str] = []

        for col_name, col_type in self.schema.items():
            encoding = self.get_encoding(col_name)
            compression = self.get_compression(col_name)

            # Check encoding × type compatibility
            if encoding is not None:
                if not _encoding_is_compatible(col_type, encoding):
                    warnings.append(
                        f"Column '{col_name}': encoding '{encoding}' is incompatible "
                        f"with type '{col_type}'"
                    )

            # Check encoding × compression compatibility
            if encoding is not None and compression is not None:
                if not _compression_is_compatible(encoding, compression):
                    warnings.append(
                        f"Column '{col_name}': compression '{compression}' is incompatible "
                        f"with encoding '{encoding}'"
                    )

        return warnings
