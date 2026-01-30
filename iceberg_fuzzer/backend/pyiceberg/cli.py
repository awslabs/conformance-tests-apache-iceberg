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
KONA - Iceberg Table Fuzzer
PyIceberg table generation implementation using PyIceberg library
"""

import argparse
from pathlib import Path
from datetime import datetime, date, time
from decimal import Decimal
import pyarrow as pa
from concurrent.futures import ThreadPoolExecutor, as_completed

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType, IntegerType, LongType, FloatType, DoubleType, StringType,
    BinaryType, DateType, TimeType, TimestampType, TimestamptzType,
    DecimalType, FixedType, UUIDType, ListType, NestedField
)

def create_iceberg_schema(table_spec):
    """Convert table spec to PyIceberg schema"""
    fields = []
    field_id = 1
    
    for field_spec in table_spec['schema']:
        field_name = field_spec['name']
        field_type_str = field_spec['type']
        
        # Map type strings to PyIceberg types
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
            'uuid': UUIDType()
        }
        
        if field_type_str in type_map:
            field_type = type_map[field_type_str]
        elif field_type_str.startswith('decimal('):
            parts = field_type_str[8:-1].split(',')
            precision = int(parts[0])
            scale = int(parts[1]) if len(parts) > 1 else 0
            field_type = DecimalType(precision, scale)
        elif field_type_str.startswith('fixed('):
            length = int(field_type_str[6:-1])
            field_type = FixedType(length)
        else:
            field_type = StringType()
        
        fields.append(NestedField(field_id=field_id, name=field_name, field_type=field_type, required=False))
        field_id += 1
    
    return Schema(*fields)

def generate_sample_data(table_spec, num_rows=100):
    """Generate sample data for a table"""
    data = {}
    
    for field_spec in table_spec['schema']:
        field_name = field_spec['name']
        field_type = field_spec['type']
        
        if field_name == 'id':
            data[field_name] = pa.array(list(range(num_rows)), type=pa.int32())
        elif field_type == 'boolean':
            data[field_name] = pa.array([i % 2 == 0 for i in range(num_rows)])
        elif field_type == 'int':
            data[field_name] = pa.array(list(range(num_rows)), type=pa.int32())
        elif field_type == 'long':
            data[field_name] = pa.array([i * 1000 for i in range(num_rows)], type=pa.int64())
        elif field_type == 'float':
            data[field_name] = pa.array([float(i) + 0.5 for i in range(num_rows)], type=pa.float32())
        elif field_type == 'double':
            data[field_name] = pa.array([float(i) * 1.5 for i in range(num_rows)], type=pa.float64())
        elif field_type == 'string':
            data[field_name] = pa.array([f"value_{i}" for i in range(num_rows)])
        elif field_type == 'date':
            base_date = date(2023, 1, 1)
            data[field_name] = pa.array([base_date.replace(day=min(28, (i % 28) + 1)) for i in range(num_rows)], type=pa.date32())
        elif field_type == 'time':
            data[field_name] = pa.array([time(hour=(i % 24), minute=(i % 60)) for i in range(num_rows)], type=pa.time64('us'))
        elif field_type == 'timestamp':
            base_ts = datetime(2023, 1, 1)
            data[field_name] = pa.array([base_ts.replace(hour=(i % 24)) for i in range(num_rows)], type=pa.timestamp('us'))
        elif field_type == 'timestamptz':
            base_ts = datetime(2023, 1, 1)
            data[field_name] = pa.array([base_ts.replace(hour=(i % 24)) for i in range(num_rows)], type=pa.timestamp('us', tz='UTC'))
        elif field_type.startswith('decimal('):
            parts = field_type[8:-1].split(',')
            precision = int(parts[0])
            scale = int(parts[1]) if len(parts) > 1 else 0
            data[field_name] = pa.array([Decimal(f"{i}.{i%100:02d}") for i in range(num_rows)], type=pa.decimal128(precision, scale))
        elif field_type == 'uuid':
            import uuid
            # Generate deterministic UUIDs based on row index
            data[field_name] = pa.array([uuid.UUID(int=i).bytes for i in range(num_rows)], type=pa.binary(16))
        elif field_type.startswith('fixed('):
            length = int(field_type[6:-1])
            data[field_name] = pa.array([f"fixed_{i}".ljust(length, '0')[:length].encode() for i in range(num_rows)], type=pa.binary(length))
        elif field_type.startswith('list<') or field_type.startswith('map<') or field_type.startswith('struct<'):
            # Complex types not fully supported in PyIceberg - use string placeholder
            data[field_name] = pa.array([f"value_{i}" for i in range(num_rows)])
        else:
            data[field_name] = pa.array([f"value_{i}" for i in range(num_rows)])
    
    return pa.Table.from_arrays(list(data.values()), names=list(data.keys()))

def create_table_with_commits(catalog_config, table_name, table_spec, num_commits=10, rows_per_commit=100):
    """Create table and add multiple commits with separate catalog instance"""
    try:
        # Create separate catalog instance for each worker to avoid concurrency issues
        # Add timeout to handle SQLite locking with parallel workers
        uri = catalog_config['uri']
        if 'sqlite:///' in uri and '?' not in uri:
            uri += '?timeout=30'
        
        catalog = SqlCatalog(
            f"iceberg_catalog_{table_name}",
            uri=uri,
            warehouse=catalog_config['warehouse']
        )
        
        namespace = ("simple",)
        try:
            catalog.create_namespace(namespace)
        except:
            pass
        
        iceberg_schema = create_iceberg_schema(table_spec)
        table_identifier = f"simple.{table_name}"
        
        # Force drop and recreate to avoid conflicts
        try:
            catalog.drop_table(table_identifier)
        except:
            pass
        
        table = catalog.create_table(
            identifier=table_identifier,
            schema=iceberg_schema,
            properties={
                "format-version": table_spec.get('iceberg_version', 'v2').replace('v', ''),
                "write.parquet.compression-codec": table_spec.get('compression', {}).get('codec', 'gzip'),
                **({} if table_spec.get('compression', {}).get('level') is None 
                   else {"write.parquet.compression-level": str(table_spec['compression']['level'])})
            }
        )
        
        for _ in range(num_commits):
            data = generate_sample_data(table_spec, rows_per_commit)
            table.append(data)
        
        catalog.close()
        return True
    except Exception as e:
        print(f"Error creating table {table_name}: {e}")
        return False

from iceberg_fuzzer.table_spec import load_table_specs

def create_comprehensive_schema():
    """Create comprehensive schema with multiple data types"""
    fields = [NestedField(field_id=1, name="id", field_type=IntegerType(), required=False)]
    field_id = 2
    
    # 20 primitive types
    primitive_types = [
        ("boolean_col", BooleanType()),
        ("int_col", IntegerType()),
        ("long_col", LongType()),
        ("float_col", FloatType()),
        ("double_col", DoubleType()),
        ("string_col", StringType()),
        ("binary_col", BinaryType()),
        ("date_col", DateType()),
        ("time_col", TimeType()),
        ("timestamp_col", TimestampType()),
        ("timestamptz_col", TimestamptzType()),
        ("decimal_col", DecimalType(10, 2)),
        ("uuid_col", UUIDType()),
        ("fixed_col", FixedType(16)),
        ("string_small_col", StringType()),
        ("string_medium_col", StringType()),
        ("string_long_col", StringType()),
        ("binary_small_col", BinaryType()),
        ("binary_medium_col", BinaryType()),
        ("binary_long_col", BinaryType())
    ]
    
    for name, type_def in primitive_types:
        fields.append(NestedField(field_id=field_id, name=name, field_type=type_def, required=False))
        field_id += 1
    
    # 3 list types (simplified)
    list_types = [
        ("list_boolean_col", ListType(element_id=field_id+1000, element_type=BooleanType(), element_required=False)),
        ("list_int_col", ListType(element_id=field_id+1001, element_type=IntegerType(), element_required=False)),
        ("list_string_col", ListType(element_id=field_id+1002, element_type=StringType(), element_required=False))
    ]
    
    for name, type_def in list_types:
        fields.append(NestedField(field_id=field_id, name=name, field_type=type_def, required=False))
        field_id += 1
    
    return Schema(*fields)

def generate_comprehensive_row_data(row_id, num_rows=1):
    """Generate deterministic comprehensive row data based on ID"""
    data = {}
    
    # ID column
    data["id"] = pa.array([row_id + i for i in range(num_rows)], type=pa.int32())
    
    # Primitive columns
    data["boolean_col"] = pa.array([(row_id + i) % 2 == 0 for i in range(num_rows)])
    data["int_col"] = pa.array([row_id + i for i in range(num_rows)], type=pa.int32())
    data["long_col"] = pa.array([(row_id + i) * 1000 for i in range(num_rows)], type=pa.int64())
    data["float_col"] = pa.array([float(row_id + i) + 0.5 for i in range(num_rows)], type=pa.float32())
    data["double_col"] = pa.array([float(row_id + i) * 1.5 for i in range(num_rows)], type=pa.float64())
    data["string_col"] = pa.array([f"value_{row_id + i}" for i in range(num_rows)])
    data["binary_col"] = pa.array([f"binary_{row_id + i}".encode() for i in range(num_rows)])
    
    # Date/time columns
    base_date = date(2023, 1, 1)
    data["date_col"] = pa.array([base_date.replace(day=min(28, ((row_id + i) % 28) + 1)) for i in range(num_rows)], type=pa.date32())
    data["time_col"] = pa.array([time(hour=((row_id + i) % 24), minute=((row_id + i) % 60)) for i in range(num_rows)], type=pa.time64('us'))
    
    base_ts = datetime(2023, 1, 1)
    data["timestamp_col"] = pa.array([base_ts.replace(hour=((row_id + i) % 24)) for i in range(num_rows)], type=pa.timestamp('us'))
    data["timestamptz_col"] = pa.array([base_ts.replace(hour=((row_id + i) % 24)) for i in range(num_rows)], type=pa.timestamp('us', tz='UTC'))
    
    # Special types
    data["decimal_col"] = pa.array([Decimal(f"{row_id + i}.{(row_id + i) % 100:02d}") for i in range(num_rows)], type=pa.decimal128(10, 2))
    import uuid
    data["uuid_col"] = pa.array([uuid.UUID(int=(row_id + i)).bytes for i in range(num_rows)], type=pa.binary(16))
    data["fixed_col"] = pa.array([f"fixed_{row_id + i}".ljust(16, '0')[:16].encode() for i in range(num_rows)], type=pa.binary(16))
    
    # String variants
    data["string_small_col"] = pa.array([f"s{row_id + i}" for i in range(num_rows)])
    data["string_medium_col"] = pa.array([f"medium_value_{row_id + i}" for i in range(num_rows)])
    data["string_long_col"] = pa.array([f"long_value_{row_id + i}_" + "x" * 50 for i in range(num_rows)])
    
    # Binary variants
    data["binary_small_col"] = pa.array([f"b{row_id + i}".encode() for i in range(num_rows)])
    data["binary_medium_col"] = pa.array([f"medium_binary_{row_id + i}".encode() for i in range(num_rows)])
    data["binary_long_col"] = pa.array([f"long_binary_{row_id + i}".encode() + b"x" * 50 for i in range(num_rows)])
    
    # List columns - fix element types to match schema
    data["list_boolean_col"] = pa.array([[True, False, (row_id + i) % 2 == 0] for i in range(num_rows)])
    data["list_int_col"] = pa.array([[row_id + i, row_id + i + 1, row_id + i + 2] for i in range(num_rows)], type=pa.list_(pa.int32()))
    data["list_string_col"] = pa.array([[f"item_{row_id + i}_{j}" for j in range(3)] for i in range(num_rows)])
    
    return pa.Table.from_arrays(list(data.values()), names=list(data.keys()))

def execute_write_pattern(table, pattern_name):
    """Execute specific write pattern"""
    if pattern_name == "insert_heavy":
        # ✅ Pattern 1: 10 commits of 100 rows each
        for i in range(10):
            data = generate_comprehensive_row_data(i * 100 + 1, 100)
            table.append(data)
    
    elif pattern_name == "update_heavy":
        # ❌ Pattern 2: Limited - fallback to INSERT-only
        for i in range(10):
            data = generate_comprehensive_row_data(i * 100 + 1, 100)
            table.append(data)
        
    elif pattern_name == "delete_heavy":
        # ❌ Pattern 3: Limited - fallback to INSERT-only
        for i in range(10):
            data = generate_comprehensive_row_data(i * 100 + 1, 100)
            table.append(data)
    
    elif pattern_name == "mixed_chaos":
        # ❌ Pattern 4: Limited - fallback to INSERT-only
        for i in range(10):
            data = generate_comprehensive_row_data(i * 100 + 1, 100)
            table.append(data)
    
    elif pattern_name == "batch_replace":
        # ✅ Pattern 5: Uses overwrite operations
        data = generate_comprehensive_row_data(1, 500)
        table.append(data)
        data = generate_comprehensive_row_data(1, 1000)
        table.overwrite(data)
    
    elif pattern_name == "gradual_growth":
        # ✅ Pattern 6: Incremental inserts
        for i in range(10):
            data = generate_comprehensive_row_data(i * 100 + 1, 100)
            table.append(data)
    
    elif pattern_name == "burst_ops":
        # ✅ Pattern 7: Burst + overwrite pattern
        for i in range(3):
            data = generate_comprehensive_row_data(i * 50 + 1, 50)
            table.append(data)
        data = generate_comprehensive_row_data(1, 1000)
        table.overwrite(data)
    
    elif pattern_name == "truncate_cycles":
        # ❌ Pattern 8: Limited - uses overwrite workaround
        data = generate_comprehensive_row_data(1, 600)
        table.append(data)
        data = generate_comprehensive_row_data(1, 1000)
        table.overwrite(data)

def create_comprehensive_table(catalog_config, pattern_name, version, delete_mode):
    """Create a comprehensive table with specific pattern"""
    uri = catalog_config['uri']
    if 'sqlite:///' in uri and '?' not in uri:
        uri += '?timeout=30'
    
    catalog = SqlCatalog(
        f"comprehensive_catalog_{pattern_name}",
        uri=uri,
        warehouse=catalog_config['warehouse']
    )
    
    table_name = f"unpart_{pattern_name}_zstd_plain_dictionary_{version}_{delete_mode}"
    table_identifier = f"write_pattern.{table_name}"
    
    try:
        catalog.drop_table(table_identifier)
    except:
        pass
    
    try:
        catalog.create_namespace(("write_pattern",))
    except:
        pass
    
    schema = create_comprehensive_schema()
    
    table = catalog.create_table(
        identifier=table_identifier,
        schema=schema,
        properties={
            "format-version": version.replace('v', ''),
            "write.parquet.compression-codec": "zstd",
            "write.delete.mode": "copy-on-write" if delete_mode == "cow" else "merge-on-read"
        }
    )
    
    execute_write_pattern(table, pattern_name)
    catalog.close()
    return table_name

def main():
    parser = argparse.ArgumentParser(description='Generate Iceberg tables using real PyIceberg')
    parser.add_argument('--backend', default='pyiceberg', help='Backend to use')
    parser.add_argument('--warehouse-path', required=True, help='Warehouse path')
    parser.add_argument('--specs', required=True, help='Path to table specs YAML file')
    parser.add_argument('--num-parallel', type=int, default=1, help='Number of parallel workers')
    parser.add_argument('--table-type', choices=['simple', 'write-patterns', 'all'], default='simple', 
                       help='Type of tables to generate: simple (single-type), write-patterns (comprehensive), or all')
    
    args = parser.parse_args()
    
    warehouse_path = Path(args.warehouse_path)
    warehouse_path.mkdir(parents=True, exist_ok=True)
    
    catalog_config = {
        'uri': f"sqlite:///{warehouse_path}/catalog.db",
        'warehouse': f"file://{warehouse_path}"
    }
    
    # Create initial catalog to set up tables
    catalog = SqlCatalog("iceberg_catalog", **catalog_config)
    catalog.create_tables()
    catalog.close()
    
    if args.table_type in ['simple', 'all']:
        generate_simple_tables(args, catalog_config)
    
    if args.table_type in ['write-patterns', 'all']:
        generate_write_pattern_tables(args, catalog_config)

def generate_simple_tables(args, catalog_config):
    """Generate single-type tables (12,800 tables)"""
    table_specs = load_table_specs(args.specs)
    
    print(f"Generating {len(table_specs)} single-type tables with {args.num_parallel} parallel workers...")
    
    success_count = 0
    total_count = len(table_specs)
    
    if args.num_parallel == 1:
        for spec in table_specs:
            success = create_table_with_commits(catalog_config, spec['name'], spec)
            if success:
                success_count += 1
                print(f"✓ Created table {spec['name']} ({success_count}/{total_count})")
            else:
                print(f"✗ Failed to create table {spec['name']}")
    else:
        with ThreadPoolExecutor(max_workers=args.num_parallel) as executor:
            future_to_table = {
                executor.submit(create_table_with_commits, catalog_config, spec['name'], spec): spec['name']
                for spec in table_specs
            }
            
            for future in as_completed(future_to_table):
                table_name = future_to_table[future]
                try:
                    success = future.result()
                    if success:
                        success_count += 1
                        print(f"✓ Created table {table_name} ({success_count}/{total_count})")
                    else:
                        print(f"✗ Failed to create table {table_name}")
                except Exception as e:
                    print(f"✗ Exception creating table {table_name}: {e}")
    
    print(f"\nCompleted simple tables: {success_count}/{total_count} tables created successfully")

def generate_write_pattern_tables(args, catalog_config):
    """Generate comprehensive write-pattern tables (16 tables - only working patterns)"""
    # Only include patterns that work properly with PyIceberg
    working_patterns = ["insert_heavy", "batch_replace", "gradual_growth", "burst_ops"]
    versions = ["v1", "v2"]
    delete_modes = ["cow", "posdelete"]
    
    total_tables = len(working_patterns) * len(versions) * len(delete_modes)
    created = 0
    
    print(f"Generating {total_tables} write-pattern tables (only working patterns)...")
    
    for pattern in working_patterns:
        for version in versions:
            for delete_mode in delete_modes:
                try:
                    table_name = create_comprehensive_table(catalog_config, pattern, version, delete_mode)
                    created += 1
                    print(f"✓ Created {table_name} ({created}/{total_tables})")
                except Exception as e:
                    print(f"✗ Failed to create {pattern}_{version}_{delete_mode}: {e}")
    
    print(f"\nCompleted write-pattern tables: {created}/{total_tables} tables created successfully")
    
    # Show skipped patterns
    skipped_patterns = ["update_heavy", "delete_heavy", "mixed_chaos", "truncate_cycles"]
    print(f"\nSkipped patterns (PyIceberg limitations): {', '.join(skipped_patterns)}")
    print("These patterns fall back to INSERT-only operations and don't provide intended compaction scenarios")

if __name__ == '__main__':
    main()
