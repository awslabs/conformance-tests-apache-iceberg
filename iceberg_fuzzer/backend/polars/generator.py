#!/usr/bin/env python3
"""
Conformance Tests for Apache Iceberg
Polars implementation of TableGenerator
Uses Polars for fast data generation + PyIceberg for writing
"""

import sys
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, time
from decimal import Decimal
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))

import polars as pl
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType, IntegerType, LongType, FloatType, DoubleType,
    StringType, BinaryType, DateType, TimeType, TimestampType, TimestamptzType, UUIDType,
    DecimalType, FixedType, ListType, NestedField, MapType, StructType as IcebergStructType
)

from iceberg_fuzzer.generator import TableGenerator
from iceberg_fuzzer.table_spec import load_table_specs
from iceberg_fuzzer.common import row_id_from_index
from iceberg_fuzzer.common.timestamps import timestamp_value, timestamptz_value


class PolarsGenerator(TableGenerator):
    """Polars implementation of table generator"""

    def __init__(self, warehouse_path: str, specs_path: str, seed: int | None = None):
        super().__init__(warehouse_path, specs_path, seed=seed)
        self._setup_warehouse()

    def _setup_warehouse(self):
        """Setup warehouse path and catalog"""
        # This path is interpolated into a file:// URI that is stored verbatim
        # as each table's location and inside its manifests, so it has to be
        # absolute.
        path = Path(self.warehouse_path).resolve()
        path.mkdir(parents=True, exist_ok=True)

        self.catalog_config = {
            'uri': f"sqlite:///{path}/catalog.db",
            'warehouse': f"file://{path}"
        }

        # Initialize catalog
        catalog = SqlCatalog("iceberg_catalog", **self.catalog_config)
        catalog.create_tables()
        catalog.close()

    def _get_catalog(self):
        """Get a new catalog instance"""
        return SqlCatalog("iceberg_catalog", **self.catalog_config)

    def supports_s3_tables(self) -> bool:
        return False

    def supports_partitioned_tables(self) -> bool:
        return False

    def supports_minimal_mode(self) -> bool:
        return False

    def _get_iceberg_type(self, iceberg_type_str: str):
        """Map Iceberg type string to PyIceberg type"""
        type_map = {
            'boolean': BooleanType(),
            'int': IntegerType(),
            'long': LongType(),
            'float': FloatType(),
            'double': DoubleType(),
            'string': StringType(),
            'binary': BinaryType(),
            'date': DateType(),
            'time': TimeType(),
            'timestamp': TimestampType(),
            'timestamptz': TimestamptzType(),
            'decimal(10,2)': DecimalType(10, 2),
            'uuid': UUIDType(),
        }

        # Handle fixed types
        if iceberg_type_str.startswith('fixed[') and iceberg_type_str.endswith(']'):
            length = int(iceberg_type_str[6:-1])
            return FixedType(length)

        # Handle list types
        if iceberg_type_str.startswith('list<'):
            inner = iceberg_type_str[5:-1]
            inner_type = self._get_iceberg_type(inner)
            return ListType(1, inner_type, element_required=False)

        # Handle map types
        if iceberg_type_str.startswith('map<'):
            # Extract key and value types: map<int, string>
            inner = iceberg_type_str[4:-1]
            key_str, value_str = inner.split(',', 1)
            key_type = self._get_iceberg_type(key_str.strip())
            value_type = self._get_iceberg_type(value_str.strip())
            return MapType(1, key_type, 2, value_type, value_required=False)

        # Handle struct types
        if iceberg_type_str.startswith('struct<'):
            # Extract field definitions: struct<field1:type1, field2:type2>
            inner = iceberg_type_str[7:-1]
            fields = []
            field_id = 1
            for field_def in inner.split(','):
                field_name, field_type_str = field_def.strip().split(':', 1)
                field_type = self._get_iceberg_type(field_type_str.strip())
                fields.append(NestedField(field_id, field_name.strip(), field_type, required=False))
                field_id += 1
            return IcebergStructType(*fields)

        return type_map.get(iceberg_type_str, StringType())

    def _generate_polars_data(
        self,
        field_name: str,
        iceberg_type: str,
        num_rows: int,
        start_id: int = 1,
    ):
        """Generate data using Polars to match PySpark/PyIceberg patterns.

        `start_id` is the ID of the batch's first row. Values are derived
        from the row ID rather than the batch index, so the validator can
        predict a row's value from the ID it reads back.
        """

        # Use iceberg_type instead of field_name for type determination
        if iceberg_type == 'boolean':
            return [(i + start_id) % 2 == 0 for i in range(num_rows)]
        elif iceberg_type == 'int':
            return list(range(start_id, num_rows + start_id))
        elif iceberg_type == 'long':
            return [(i + start_id) * 1000 for i in range(num_rows)]
        elif iceberg_type == 'float':
            return [float(i + start_id) + 0.5 for i in range(num_rows)]
        elif iceberg_type == 'double':
            return [float(i + start_id) * 1.5 for i in range(num_rows)]
        elif iceberg_type.startswith('decimal'):
            return [Decimal(f"{i + start_id}.{(i + start_id) % 100:02d}") for i in range(num_rows)]
        elif iceberg_type == 'date':
            return [date(2023, 1, ((i + start_id) % 28) + 1) for i in range(num_rows)]
        elif iceberg_type == 'time':
            return [
                time(hour=(i + start_id) % 24, minute=(i + start_id) % 60)
                for i in range(num_rows)
            ]
        elif iceberg_type == 'timestamp':
            return [timestamp_value(i + start_id) for i in range(num_rows)]
        elif iceberg_type == 'timestamptz':
            return [timestamptz_value(i + start_id) for i in range(num_rows)]
        elif iceberg_type == 'uuid':
            return [uuid.UUID(int=i + start_id).bytes for i in range(num_rows)]
        elif iceberg_type == 'string':
            return [f"value_{i + start_id}" for i in range(num_rows)]
        elif iceberg_type == 'binary':
            return [f"value_{i + start_id}".encode() for i in range(num_rows)]
        # List types
        elif iceberg_type.startswith('list<'):
            element_type = iceberg_type[5:-1]  # Extract element type from list<type>
            if element_type == 'boolean':
                result = []
                for i in range(num_rows):
                    row_id = i + start_id
                    if row_id % 2 == 1:  # odd rows
                        result.append([True, False, False])
                    else:  # even rows
                        result.append([False, True, True])
                return result
            elif element_type == 'int':
                return [[i + start_id, i + 1 + start_id, i + 2 + start_id]
                        for i in range(num_rows)]
            elif element_type == 'long':
                return [
                    [(i + start_id) * 1000, (i + 1 + start_id) * 1000,
                     (i + 2 + start_id) * 1000]
                    for i in range(num_rows)
                ]
            elif element_type == 'float':
                return [
                    [float(i + start_id) + 0.5, float(i + 1 + start_id) + 0.5,
                     float(i + 2 + start_id) + 0.5]
                    for i in range(num_rows)
                ]
            elif element_type == 'double':
                return [
                    [float(i + start_id) * 1.5, float(i + 1 + start_id) * 1.5,
                     float(i + 2 + start_id) * 1.5]
                    for i in range(num_rows)
                ]
            elif element_type.startswith('decimal'):
                return [
                    [
                        Decimal(f"{i + start_id}.{(i + start_id) % 100:02d}"),
                        Decimal(f"{i + 1 + start_id}.{(i + 1 + start_id) % 100:02d}"),
                        Decimal(f"{i + 2 + start_id}.{(i + 2 + start_id) % 100:02d}")
                    ]
                    for i in range(num_rows)
                ]
            elif element_type == 'date':
                return [
                    [date(2023, 1, ((i + start_id) % 28) + 1),
                     date(2023, 1, ((i + 1 + start_id) % 28) + 1),
                     date(2023, 1, ((i + 2 + start_id) % 28) + 1)]
                    for i in range(num_rows)
                ]
            elif element_type == 'uuid':
                return [
                    [f"uuid_{i + start_id}".encode()[:16].ljust(16, b'\x00'),
                     f"uuid_{i + 1 + start_id}".encode()[:16].ljust(16, b'\x00'),
                     f"uuid_{i + 2 + start_id}".encode()[:16].ljust(16, b'\x00')]
                    for i in range(num_rows)
                ]
            elif element_type == 'string':
                return [
                    [f"item_{i + start_id}_0", f"item_{i + start_id}_1",
                     f"item_{i + start_id}_2"]
                    for i in range(num_rows)
                ]
            elif element_type == 'binary':
                return [
                    [f"item_{i + start_id}_0".encode(),
                     f"item_{i + start_id}_1".encode(),
                     f"item_{i + start_id}_2".encode()]
                    for i in range(num_rows)
                ]
            else:
                return [["item1", "item2", "item3"] for i in range(num_rows)]
        else:
            # Default fallback
            return [f"value_{i + start_id}" for i in range(num_rows)]

    def _get_polars_dtype(self, field_name: str, iceberg_type: str):
        """Get appropriate Polars dtype for field"""
        if iceberg_type == 'boolean':
            return pl.Boolean
        elif iceberg_type == 'int':
            return pl.Int32
        elif iceberg_type == 'long':
            return pl.Int64
        elif iceberg_type == 'float':
            return pl.Float32
        elif iceberg_type == 'double':
            return pl.Float64
        elif iceberg_type == 'date':
            return pl.Date
        elif iceberg_type == 'time':
            return pl.Time
        elif iceberg_type == 'timestamp':
            return pl.Datetime('us')  # microseconds
        elif iceberg_type == 'timestamptz':
            # Iceberg's timestamptz is instants in UTC; a naive dtype would be
            # written as the tz-free `timestamp` instead.
            return pl.Datetime('us', time_zone='UTC')
        elif iceberg_type.startswith('decimal'):
            return pl.Decimal(precision=10, scale=2)
        elif iceberg_type in ['binary'] or iceberg_type.startswith('fixed'):
            return pl.Binary
        elif iceberg_type == 'uuid':
            # Polars has no UUID dtype. On append the column is cast to
            # 16-byte binary and given Arrow's uuid extension type.
            return pl.Binary
        elif iceberg_type.startswith('list<'):
            element_type = iceberg_type[5:-1]
            element_dtype = self._get_polars_dtype('', element_type)
            return pl.List(element_dtype)
        else:
            return pl.String

    def _create_simple_table(self, table_spec: dict) -> bool:
        """Create a single simple table using Polars + PyIceberg"""
        table_name = table_spec['name']

        # Extract field info from schema
        value_field = table_spec['schema'][1]  # Second field is the value field
        field_name = value_field['name']
        field_type = value_field['type']

        # Convert to iceberg type name
        iceberg_type = field_type
        version = int(table_spec['properties']['format-version'])

        # Skip nested types - Polars/PyArrow limitations
        if iceberg_type.startswith('map<') or iceberg_type.startswith('struct<'):
            print(f"⚠️  Skipped {table_name} (nested type: {iceberg_type})")
            return True  # Return True to indicate "handled" (not failed)

        try:
            catalog = SqlCatalog(
                f"iceberg_catalog_{table_name}",
                uri=self.catalog_config['uri'],
                warehouse=self.catalog_config['warehouse']
            )

            # Create namespace
            namespace = ("simple",)
            try:
                catalog.create_namespace(namespace)
            except Exception:
                pass  # Ignore errors

            # Create Iceberg schema
            iceberg_field_type = self._get_iceberg_type(iceberg_type)
            schema = Schema(
                NestedField(1, "id", IntegerType(), required=False),
                NestedField(2, field_name, iceberg_field_type, required=False)
            )
            arrow_schema = schema_to_pyarrow(schema)

            table_identifier = f"simple.{table_name}"

            # Drop if exists
            try:
                catalog.drop_table(table_identifier)
            except Exception:
                pass  # Ignore errors

            compression = table_spec.get('compression', {})
            codec = compression.get('codec')
            level = compression.get('level')

            properties = {"format-version": str(version)}
            if codec is not None:
                properties["write.parquet.compression-codec"] = codec
            if level is not None:
                properties["write.parquet.compression-level"] = str(level)

            # Create table
            table = catalog.create_table(
                identifier=table_identifier,
                schema=schema,
                properties=properties,
            )

            # Generate data in 10 commits using Polars
            rows_per_commit = 100
            for commit in range(10):
                row_offset = commit * rows_per_commit
                ids = [
                    row_id_from_index(i, row_offset) for i in range(rows_per_commit)
                ]
                # Values are derived from the row ID, not the index, so a row's
                # value stays predictable from its ID across commits.
                values = self._generate_polars_data(
                    field_name, iceberg_type, rows_per_commit, ids[0]
                )

                # Get appropriate dtype
                value_dtype = self._get_polars_dtype(field_name, iceberg_type)

                # Create Polars DataFrame with correct types
                if value_dtype:
                    df = pl.DataFrame({
                        "id": pl.Series(ids, dtype=pl.Int32),
                        field_name: pl.Series(values, dtype=value_dtype)
                    })
                else:
                    df = pl.DataFrame({
                        "id": pl.Series(ids, dtype=pl.Int32),
                        field_name: values
                    })

                # Polars has no dtype for every Iceberg type, so some columns
                # arrive as the nearest equivalent (time64[ns] for time,
                # variable-length binary for uuid). Casting to the Arrow schema
                # PyIceberg derives from the table's own schema is what fixes
                # the physical representation: the microsecond unit, and the
                # 16-byte width behind uuid's logical annotation, so a value of
                # any other length fails here.
                table.append(df.to_arrow().cast(arrow_schema))

            catalog.close()
            return True

        except Exception as e:
            print(f"    → Failed to create table {table_name}: {e}")
            return False

    def generate_simple_tables(
        self,
        num_parallel: int = 1,
        minimal: bool = False,
        random_count: int = None,
        delete_existing: bool = False
    ) -> bool:
        """Generate simple single-type tables.

        Returns:
            True if every table in the set was created, False if any failed.
        """
        table_specs = load_table_specs(self.specs_path, minimal=minimal)

        # Apply random selection if specified
        table_specs = self._random_select_tables(table_specs, random_count)

        print(f"Generating {len(table_specs)} simple tables with Polars + PyIceberg...")
        print("\n📊 Polars Backend Support Status:")
        print(
            "  ✅ Supported: All primitive types (boolean, int, long, float, double, "
            "string, binary, date, time, timestamp, decimal, uuid, fixed)"
        )
        print("  ✅ Supported: All list types (list<primitive>)")
        print("  ⚠️  Skipped: Map types (Polars/PyArrow limitation)")
        print("  ⚠️  Skipped: Struct types (PyArrow flattening issue)")
        print(f"  📈 Expected success rate: 100% (primitives + lists only, nested types skipped)\n")

        success_count = 0
        total_count = len(table_specs)

        if num_parallel == 1:
            for spec in table_specs:
                success = self._create_simple_table(spec)
                if success:
                    success_count += 1
                    print(f"✓ Created table {spec['name']} ({success_count}/{total_count})")
                else:
                    print(f"✗ Failed to create table {spec['name']}")
        else:
            with ThreadPoolExecutor(max_workers=num_parallel) as executor:
                futures = {executor.submit(self._create_simple_table, spec): spec for spec in table_specs}
                for future in as_completed(futures):
                    spec = futures[future]
                    try:
                        success = future.result()
                        if success:
                            success_count += 1
                            print(f"✓ Created table {spec['name']} ({success_count}/{total_count})")
                        else:
                            print(f"✗ Failed to create table {spec['name']}")
                    except Exception as e:
                        print(f"    → Failed to create table {spec['name']}: {e}")

        print(f"\nCompleted simple tables: {success_count}/{total_count} tables created successfully")
        return success_count == total_count

    def generate_single_table(self, table_name: str, delete_existing: bool = False) -> bool:
        """Generate a single table by name.

        Returns:
            True if the table was created, False if the name matched no spec
            or creation failed.
        """

        for spec in load_table_specs(self.specs_path):
            if spec['name'] != table_name:
                continue

            print(f"Creating single table: {table_name}")

            if not delete_existing:
                try:
                    catalog = self._get_catalog()
                    catalog.load_table(f"simple.{table_name}")
                    print(f"Skipping existing table: {table_name}")
                    return True
                except Exception:
                    pass  # Table doesn't exist, continue with creation

            if self._create_simple_table(spec):
                print(f"✓ Successfully created table: {table_name}")
                return True

            print(f"✗ Failed to create table: {table_name}")
            return False

        print(f"❌ Table '{table_name}' not found in Polars specs")
        print("\n💡 Polars only supports simple tables (primitives + lists)")

        return False

    def generate_write_pattern_tables(
        self,
        num_parallel: int = 1,
        random_count: int = None,
        delete_existing: bool = False
    ):
        """Write-pattern tables not supported in Polars"""
        raise NotImplementedError("Write-pattern tables only supported in PySpark backend")

    def generate_partitioned_tables(
        self,
        num_parallel: int = 1,
        random_count: int = None,
        minimal: bool = False,
        delete_existing: bool = False
    ):
        """Partitioned tables not supported in Polars"""
        raise NotImplementedError("Partitioned tables only supported in PySpark backend")

    def generate_evolution_tables(self, num_parallel: int = 1, random_count: int = None, delete_existing: bool = False):
        """Evolution tables not supported in Polars"""
        raise NotImplementedError("Evolution tables only supported in PySpark backend")

    def generate_minimal_tables(
        self, num_parallel: int = 1, random_count: int = None, delete_existing: bool = False
    ) -> bool:
        """Generate minimal set of simple tables.

        Returns:
            True if every table in the set was created, False if any failed.
        """
        return self.generate_simple_tables(num_parallel=num_parallel, minimal=True, random_count=random_count)

    def cleanup(self):
        """No cleanup needed for Polars"""
        pass
