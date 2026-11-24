"""Tests for SchemaProvider and WriteOptionsProvider protocol interfaces.

Verifies that:
- Protocols are importable from the dimensions package (re-export identity)
- Conforming implementations return expected values
"""

from typing import Any, List, Optional

import pyarrow as pa

from iceberg_conformance.exhaustive.dimensions import SchemaProvider, WriteOptionsProvider
from iceberg_conformance.exhaustive.dimensions.interfaces import (
    SchemaProvider as DirectSchemaProvider,
)
from iceberg_conformance.exhaustive.dimensions.interfaces import (
    WriteOptionsProvider as DirectWriteOptionsProvider,
)


# =============================================================================
# Conforming implementations
# =============================================================================


class ConformingSchemaProvider:
    """A class that fully implements SchemaProvider protocol."""

    def get_spark_ddl(self) -> str:
        return "value INT NOT NULL"

    def get_field_ids(self):
        return {("value",): 1}

    def get_arrow_schema(self, field_ids) -> pa.Schema:
        return pa.schema([
            pa.field("value", pa.int32(), nullable=False)
                .with_metadata({"PARQUET:field_id": str(field_ids[("value",)])})
        ])

    def generate_value(self, column_name: str, row_id: int) -> Any:
        return row_id


class ConformingWriteOptionsProvider:
    """A class that fully implements WriteOptionsProvider protocol."""

    def get_compression(self, column_name: str) -> Optional[str]:
        return "ZSTD"

    def get_encoding(self, column_name: str) -> Optional[str]:
        return "DICTIONARY"

    def validate_compatibility(self) -> List[str]:
        return []


class WriteOptionsWithWarnings:
    """WriteOptionsProvider that reports compatibility warnings."""

    def get_compression(self, column_name: str) -> Optional[str]:
        return "SNAPPY"

    def get_encoding(self, column_name: str) -> Optional[str]:
        return "DELTA_BINARY_PACKED"

    def validate_compatibility(self) -> List[str]:
        return ["DELTA_BINARY_PACKED is incompatible with STRING columns"]


class NoneDefaultsWriteOptions:
    """WriteOptionsProvider that returns None for all options (use writer defaults)."""

    def get_compression(self, column_name: str) -> Optional[str]:
        return None

    def get_encoding(self, column_name: str) -> Optional[str]:
        return None

    def validate_compatibility(self) -> List[str]:
        return []


# =============================================================================
# Tests: SchemaProvider
# =============================================================================


class TestSchemaProviderProtocol:
    """Tests for SchemaProvider protocol."""

    def test_direct_import_same_as_package_import(self):
        """Both import paths give the same protocol."""
        assert SchemaProvider is DirectSchemaProvider

    def test_conforming_get_spark_ddl_works(self):
        """Conforming class's get_spark_ddl returns expected DDL."""
        obj = ConformingSchemaProvider()
        assert obj.get_spark_ddl() == "value INT NOT NULL"

    def test_conforming_get_arrow_schema_works(self):
        """Conforming class's get_arrow_schema returns valid PyArrow schema."""
        obj = ConformingSchemaProvider()
        schema = obj.get_arrow_schema(obj.get_field_ids())
        assert isinstance(schema, pa.Schema)
        assert len(schema) == 1
        assert schema.field(0).name == "value"
        assert schema.field(0).type == pa.int32()

    def test_conforming_generate_value_works(self):
        """Conforming class's generate_value returns appropriate values."""
        obj = ConformingSchemaProvider()
        assert obj.generate_value("value", 0) == 0
        assert obj.generate_value("value", 42) == 42


# =============================================================================
# Tests: WriteOptionsProvider
# =============================================================================


class TestWriteOptionsProviderProtocol:
    """Tests for WriteOptionsProvider protocol."""

    def test_direct_import_same_as_package_import(self):
        """Both import paths give the same protocol."""
        assert WriteOptionsProvider is DirectWriteOptionsProvider

    def test_conforming_get_compression_works(self):
        """Conforming class returns compression codec."""
        obj = ConformingWriteOptionsProvider()
        assert obj.get_compression("value") == "ZSTD"

    def test_conforming_get_encoding_works(self):
        """Conforming class returns encoding name."""
        obj = ConformingWriteOptionsProvider()
        assert obj.get_encoding("value") == "DICTIONARY"

    def test_conforming_validate_compatibility_empty(self):
        """Conforming class with valid combos returns empty list."""
        obj = ConformingWriteOptionsProvider()
        assert obj.validate_compatibility() == []

    def test_warnings_validate_compatibility(self):
        """Provider with issues returns non-empty warnings list."""
        obj = WriteOptionsWithWarnings()
        warnings = obj.validate_compatibility()
        assert len(warnings) == 1
        assert "DELTA_BINARY_PACKED" in warnings[0]

    def test_none_defaults_get_compression(self):
        """Provider with defaults returns None."""
        obj = NoneDefaultsWriteOptions()
        assert obj.get_compression("any_col") is None

    def test_none_defaults_get_encoding(self):
        """Provider with defaults returns None."""
        obj = NoneDefaultsWriteOptions()
        assert obj.get_encoding("any_col") is None
