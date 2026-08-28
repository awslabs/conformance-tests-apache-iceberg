#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
"""
Apache Iceberg Table Fuzzer
Polars implementation of TableGenerator
Uses Polars for fast data generation + PyIceberg for writing
"""

import sys
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))

import polars as pl
import yaml
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
from iceberg_fuzzer.common import iceberg_compression_codec, row_id_from_index


class PolarsGenerator(TableGenerator):
    """Polars implementation of table generator"""

    def __init__(self, warehouse_path: str, specs_path: str):
        super().__init__(warehouse_path, specs_path)
        self._setup_warehouse()

    def _setup_warehouse(self):
        """Setup warehouse path and catalog"""
        # The path goes into a file:// URI that is stored as each table's
        # location, so it has to be absolute: in `file://relative/path` the
        # first segment parses as the authority and no reader resolves it.
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
        first_row_id: int = 1,
    ):
        """Generate data using Polars to match PySpark/PyIceberg patterns.

        `first_row_id` is the ID of the batch's first row. Values are derived
        from the row ID rather than the batch index, so the validator can
        predict a row's value from the ID it reads back.
        """

        # Use iceberg_type instead of field_name for type determination
        if iceberg_type == 'boolean':
            return [(i + first_row_id) % 2 == 0 for i in range(num_rows)]
        elif iceberg_type == 'int':
            return list(range(first_row_id, num_rows + first_row_id))
        elif iceberg_type == 'long':
            return [(i + first_row_id) * 1000 for i in range(num_rows)]
        elif iceberg_type == 'float':
            return [float(i + first_row_id) + 0.5 for i in range(num_rows)]
        elif iceberg_type == 'double':
            return [float(i + first_row_id) * 1.5 for i in range(num_rows)]
        elif iceberg_type.startswith('decimal'):
            return [Decimal(f"{i + first_row_id}.{(i + first_row_id) % 100:02d}") for i in range(num_rows)]
        elif iceberg_type == 'date':
            return [date(2023, 1, ((i + first_row_id) % 28) + 1) for i in range(num_rows)]
        elif iceberg_type == 'time':
            return [
                time(hour=(i + first_row_id) % 24, minute=(i + first_row_id) % 60)
                for i in range(num_rows)
            ]
        elif iceberg_type in ['timestamp', 'timestamptz']:
            return [datetime(2023, 1, 1, (i + first_row_id) % 24, 0, 0) for i in range(num_rows)]
        elif iceberg_type == 'uuid':
            return [uuid.UUID(int=i + first_row_id).bytes for i in range(num_rows)]
        elif iceberg_type == 'string':
            return [f"value_{i + first_row_id}" for i in range(num_rows)]
        elif iceberg_type == 'binary':
            return [f"value_{i + first_row_id}".encode() for i in range(num_rows)]
        # List types
        elif iceberg_type.startswith('list<'):
            element_type = iceberg_type[5:-1]  # Extract element type from list<type>
            if element_type == 'boolean':
                result = []
                for i in range(num_rows):
                    row_id = i + first_row_id
                    if row_id % 2 == 1:  # odd rows
                        result.append([True, False, False])
                    else:  # even rows
                        result.append([False, True, True])
                return result
            elif element_type == 'int':
                return [[i + first_row_id, i + 1 + first_row_id, i + 2 + first_row_id]
                        for i in range(num_rows)]
            elif element_type == 'long':
                return [
                    [(i + first_row_id) * 1000, (i + 1 + first_row_id) * 1000,
                     (i + 2 + first_row_id) * 1000]
                    for i in range(num_rows)
                ]
            elif element_type == 'float':
                return [
                    [float(i + first_row_id) + 0.5, float(i + 1 + first_row_id) + 0.5,
                     float(i + 2 + first_row_id) + 0.5]
                    for i in range(num_rows)
                ]
            elif element_type == 'double':
                return [
                    [float(i + first_row_id) * 1.5, float(i + 1 + first_row_id) * 1.5,
                     float(i + 2 + first_row_id) * 1.5]
                    for i in range(num_rows)
                ]
            elif element_type.startswith('decimal'):
                return [
                    [
                        Decimal(f"{i + first_row_id}.{(i + first_row_id) % 100:02d}"),
                        Decimal(f"{i + 1 + first_row_id}.{(i + 1 + first_row_id) % 100:02d}"),
                        Decimal(f"{i + 2 + first_row_id}.{(i + 2 + first_row_id) % 100:02d}")
                    ]
                    for i in range(num_rows)
                ]
            elif element_type == 'date':
                return [
                    [date(2023, 1, ((i + first_row_id) % 28) + 1),
                     date(2023, 1, ((i + 1 + first_row_id) % 28) + 1),
                     date(2023, 1, ((i + 2 + first_row_id) % 28) + 1)]
                    for i in range(num_rows)
                ]
            elif element_type == 'uuid':
                return [
                    [f"uuid_{i + first_row_id}".encode()[:16].ljust(16, b'\x00'),
                     f"uuid_{i + 1 + first_row_id}".encode()[:16].ljust(16, b'\x00'),
                     f"uuid_{i + 2 + first_row_id}".encode()[:16].ljust(16, b'\x00')]
                    for i in range(num_rows)
                ]
            elif element_type == 'string':
                return [
                    [f"item_{i + first_row_id}_0", f"item_{i + first_row_id}_1",
                     f"item_{i + first_row_id}_2"]
                    for i in range(num_rows)
                ]
            elif element_type == 'binary':
                return [
                    [f"item_{i + first_row_id}_0".encode(),
                     f"item_{i + first_row_id}_1".encode(),
                     f"item_{i + first_row_id}_2".encode()]
                    for i in range(num_rows)
                ]
            else:
                return [["item1", "item2", "item3"] for i in range(num_rows)]
        else:
            # Default fallback
            return [f"value_{i + first_row_id}" for i in range(num_rows)]

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
            level = compression.get('level')

            # Create table
            table = catalog.create_table(
                identifier=table_identifier,
                schema=schema,
                properties={
                    "format-version": str(version),
                    "write.parquet.compression-codec": iceberg_compression_codec(
                        compression.get('codec', 'snappy')
                    ),
                    **({} if level is None else {
                        "write.parquet.compression-level": str(level)
                    }),
                }
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
    ):
        """Generate simple single-type tables"""
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
            with ThreadPoolExecutor(max_workers=num_parallel) as executor:
                futures = {executor.submit(self._create_simple_table, spec): spec for spec in table_specs}
                for future in as_completed(futures):
                    spec = futures[future]
                    try:
                        success = future.result()
                        if success:
                            success_count += 1
                            print(f"✓ Created table {spec['name']} ({success_count}/{total_count})")
                    except Exception as e:
                        print(f"    → Failed to create table {spec['name']}: {e}")

        print(f"\nCompleted simple tables: {success_count}/{total_count} tables created successfully")

    def generate_single_table(self, table_name: str, delete_existing: bool = False):
        """Generate a single table by name"""

        # Load table specs to find the requested table
        with open(self.specs_path, 'r') as f:
            specs = yaml.safe_load(f)

        # Get all type definitions
        all_types = []
        all_types.extend(specs.get('primitive_types', []))
        all_types.extend(specs.get('complex_types_list', []))

        # Generate all possible table specs to find match
        compression_configs = []
        for comp_config in specs.get('compression_configs', []):
            codec = comp_config['codec']
            levels = comp_config.get('levels', [None])
            for level in levels:
                compression_configs.append({'codec': codec, 'level': level})

        encoding_types = specs.get('encoding_types', ['PLAIN'])
        iceberg_versions = []
        for version_config in specs.get('iceberg_versions', [{'version': 2, 'namespace': 'v2'}]):
            iceberg_versions.append(f"v{version_config['version']}")

        # Find matching table spec
        for type_def in all_types:
            for compression in compression_configs:
                for encoding in encoding_types:
                    for version in iceberg_versions:
                        comp_str = f"{compression['codec']}"
                        if compression['level'] is not None:
                            comp_str += f"_lvl{compression['level']}"

                        generated_name = f"unpart_{type_def['name']}_{comp_str}_{encoding.lower()}_{version}"

                        if generated_name == table_name:
                            # Found match - create the table
                            spec = {
                                'name': table_name,
                                'field_name': type_def['name'],
                                'iceberg_type': type_def['iceberg_type'],
                                'compression': compression,
                                'encoding': encoding,
                                'version': version
                            }

                            print(f"Creating single table: {table_name}")

                            # Check if table exists and skip if delete_existing is False
                            if not delete_existing:
                                try:
                                    catalog = self._get_catalog()
                                    catalog.load_table(f"simple.{table_name}")
                                    print(f"Skipping existing table: {table_name}")
                                    return True
                                except Exception:
                                    pass  # Table doesn't exist, continue with creation

                            success = self._create_simple_table(spec)
                            if success:
                                print(f"✓ Successfully created table: {table_name}")
                                return True
                            else:
                                print(f"✗ Failed to create table: {table_name}")
                                return False

        # Table not found - suggest alternatives
        print(f"❌ Table '{table_name}' not found in Polars specs")

        suggestions = suggest_table_names(table_name)
        if suggestions:
            print(f"\n💡 Did you mean one of these?")
            for i, suggestion in enumerate(suggestions, 1):
                print(f"   {i}. {suggestion}")
        else:
            print(f"\n💡 Polars only supports simple tables (primitives + lists)")

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

    def generate_minimal_tables(self, num_parallel: int = 1, random_count: int = None, delete_existing: bool = False):
        """Generate minimal set of simple tables"""
        return self.generate_simple_tables(num_parallel=num_parallel, minimal=True, random_count=random_count)

    def cleanup(self):
        """No cleanup needed for Polars"""
        pass
