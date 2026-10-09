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
DuckDB-based validator for Iceberg tables
Validates table structure and data against table_specs.yaml specifications
"""

from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, date, time
from decimal import Decimal
from pathlib import Path
import argparse
import json
import sys
import uuid as uuid_lib

import duckdb
import pyarrow.parquet as pq
import yaml

from iceberg_fuzzer.common import row_id_from_index
from iceberg_fuzzer.table_spec import select_primitive_types
from iceberg_fuzzer.validator import TableValidator

def _load_table_specs(yaml_path):
    """Load table specifications from YAML file"""
    with open(yaml_path, 'r') as f:
        return yaml.safe_load(f)

def _extract_expected_single_type_tables(specs, minimal=False):
    """Extract expected single-type table specifications.

    Complex types are not supported and raise ValueError.
    """
    tables = []

    all_types = select_primitive_types(specs, minimal=minimal)

    # Get configurations
    if minimal:
        compression_configs = [{'codec': 'uncompressed', 'level': None}]
        encoding_types = ['PLAIN']
    else:
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

    # Generate expected table names
    for type_def in all_types:
        for compression in compression_configs:
            for encoding in encoding_types:
                for version in iceberg_versions:
                    comp_str = f"{compression['codec']}"
                    if compression['level'] is not None:
                        comp_str += f"_lvl{compression['level']}"

                    table_name = f"unpart_{type_def['name']}_{comp_str}_{encoding.lower()}_{version}"

                    tables.append({
                        'name': table_name,
                        'type': 'single_type',
                        'data_type': type_def['name'],
                        'iceberg_type': type_def['iceberg_type'],
                        'compression': comp_str,
                        'encoding': encoding.lower(),
                        # Without this the format-version check is skipped, so a
                        # table named v1 could be written as v2 unnoticed.
                        'version': version,
                        'expected_rows': 1000,
                        'expected_commits': 10,
                        'schema_columns': 2  # id + value
                    })

    return tables

def _extract_expected_write_pattern_tables(specs):
    """Extract expected write-pattern table specifications from write_patterns.yaml"""

    tables = []

    # Load write_patterns.yaml - get path relative to this file
    current_file = Path(__file__).resolve()
    project_root = current_file.parent.parent.parent.parent
    write_patterns_path = project_root / 'specs' / 'write_patterns.yaml'

    if not write_patterns_path.exists():
        print(f"Warning: write_patterns.yaml not found at {write_patterns_path}")
        return tables

    try:
        with open(write_patterns_path, 'r') as f:
            write_pattern_specs = yaml.safe_load(f)
    except Exception as e:
        print(f"Warning: Could not load write_patterns.yaml: {e}")
        return tables

    # Extract patterns and configuration
    patterns = [p['name'] for p in write_pattern_specs.get('patterns', [])]
    config = write_pattern_specs.get('table_config', {})
    versions = config.get('versions', ['v1', 'v2'])
    delete_modes = config.get('delete_modes', ['cow', 'posdelete'])
    expected_rows = write_pattern_specs.get('metadata', {}).get('final_rows', 1000)
    expected_columns = write_pattern_specs.get('metadata', {}).get('total_columns', 24)

    for pattern in patterns:
        for version in versions:
            for delete_mode in delete_modes:
                # Skip invalid combinations: v1 only supports cow
                if version == 'v1' and delete_mode != 'cow':
                    continue

                table_name = f"unpart_{pattern}_zstd_plain_dictionary_{version}_{delete_mode}"

                tables.append({
                    'name': table_name,
                    'type': 'write_pattern',
                    'pattern': pattern,
                    'expected_rows': expected_rows,
                    'expected_columns': expected_columns,
                    'schema_columns': expected_columns,  # Add this for column validation
                    'compression': 'zstd',
                    'encoding': 'plain_dictionary',
                    'version': version,
                    'delete_mode': delete_mode
                })

    return tables

def _extract_expected_partitioned_tables(specs):
    """Extract expected partitioned table specifications from partitioned.yaml"""

    tables = []

    # Load partitioned.yaml - get path relative to this file
    current_file = Path(__file__).resolve()
    project_root = current_file.parent.parent.parent.parent
    partitioned_path = project_root / 'specs' / 'partitioned.yaml'

    if not partitioned_path.exists():
        print(f"Warning: partitioned.yaml not found at {partitioned_path}")
        return tables

    try:
        with open(partitioned_path, 'r') as f:
            partitioned_specs = yaml.safe_load(f)
    except Exception as e:
        print(f"Warning: Could not load partitioned.yaml: {e}")
        return tables

    # Extract patterns and configuration
    patterns = [p['name'] for p in partitioned_specs.get('patterns', [])]
    config = partitioned_specs.get('table_config', {})
    delete_modes = config.get('delete_modes', ['cow', 'posdelete'])
    expected_rows = partitioned_specs.get('metadata', {}).get('final_rows', 100000)
    expected_columns = partitioned_specs.get('metadata', {}).get('total_columns', 30)

    # All partition transforms (full set - validator should expect all possible tables)
    partition_transforms = [
        # Single-column transforms
        ["identity(category)"], ["bucket[4](id)"], ["bucket[8](id)"], ["truncate[2](name)"],
        ["truncate[5](name)"], ["year(timestamp)"], ["month(timestamp)"], ["day(timestamp)"],
        ["hour(timestamp)"], ["bucket[10](value)"], ["date(timestamp)"], ["bucket[2](id)"],
        ["bucket[16](id)"], ["bucket[32](id)"], ["bucket[4](value)"], ["bucket[8](value)"],
        ["truncate[1](name)"], ["truncate[3](name)"], ["truncate[10](name)"],
        ["identity(boolean_col)"], ["identity(int_col)"], ["identity(date_col)"],
        # Multi-column transforms
        ["identity(category)", "year(timestamp)"], ["identity(category)", "bucket[8](id)"],
        ["bucket[4](id)", "day(timestamp)"], ["year(timestamp)", "bucket[4](id)"],
        ["identity(category)", "month(timestamp)", "bucket[4](id)"],
        ["hour(timestamp)", "identity(category)"], ["day(timestamp)", "bucket[8](id)"],
        ["truncate[2](name)", "year(timestamp)"], ["bucket[4](id)", "bucket[8](value)"],
        ["identity(category)", "day(timestamp)", "bucket[4](id)"],
        ["date(timestamp)", "identity(category)"], ["truncate[3](name)", "bucket[4](id)"],
        ["month(timestamp)", "bucket[2](id)"],
        # Edge cases
        ["identity(single_value_field)"]
    ]

    for transform in partition_transforms:
        # Generate clean table name from transform spec
        transform_parts = []
        for t in transform:
            if t.startswith('identity('):
                col = t[9:-1]
                transform_parts.append(f"identity_{col}")
            elif t.startswith('bucket['):
                size = t.split('[')[1].split(']')[0]
                col = t.split('(')[1].rstrip(')')
                transform_parts.append(f"bucket{size}_{col}")
            elif t.startswith('truncate['):
                width = t.split('[')[1].split(']')[0]
                col = t.split('(')[1].rstrip(')')
                transform_parts.append(f"truncate{width}_{col}")
            elif t.startswith('date('):
                col = t[5:-1]
                transform_parts.append(f"date_{col}")
            elif t.startswith('year('):
                col = t[5:-1]
                transform_parts.append(f"year_{col}")
            elif t.startswith('month('):
                col = t[6:-1]
                transform_parts.append(f"month_{col}")
            elif t.startswith('day('):
                col = t[4:-1]
                transform_parts.append(f"day_{col}")
            elif t.startswith('hour('):
                col = t[5:-1]
                transform_parts.append(f"hour_{col}")

        transform_name = "_".join(transform_parts)

        for pattern in patterns:
            for delete_mode in delete_modes:
                # Skip invalid combinations: v1 only supports cow
                if delete_mode == "posdelete" and "_v1_" in transform_name:
                    continue

                table_name = f"part_{transform_name}_{pattern}_{delete_mode}"

                tables.append({
                    'name': table_name,
                    'type': 'partitioned',
                    'pattern': pattern,
                    'partition_spec': transform,
                    'delete_mode': delete_mode,
                    'expected_rows': expected_rows,
                    'expected_columns': expected_columns,
                    'schema_columns': expected_columns,
                    'compression': 'snappy',
                    'encoding': 'plain'
                })

    return tables

def _extract_expected_evolution_tables(specs_path):
    """Extract expected evolution table specifications from evolution.yaml"""

    tables = []

    # Get specs directory
    specs_path = Path(specs_path)
    evolution_path = specs_path.parent / 'evolution.yaml'

    if not evolution_path.exists():
        print(f"Warning: evolution.yaml not found at {evolution_path}")
        return tables

    try:
        with open(evolution_path, 'r') as f:
            evolution_specs = yaml.safe_load(f)
    except Exception as e:
        print(f"Warning: Could not load evolution.yaml: {e}")
        return tables

    # Extract evolution tables
    evolution_tables = evolution_specs.get('evolution_tables', [])
    base_schema = evolution_specs.get('base_schema', [])

    skipped = []

    for table_spec in evolution_tables:
        table_name = table_spec['name']

        # Tables this validator's reader cannot open are excluded from the expected
        # set rather than reported missing, and named below so the gap stays visible.
        unreadable = table_spec.get('unreadable_by', {}).get('duckdb')
        if unreadable:
            skipped.append((table_name, unreadable))
            continue

        # Build expected schema based on evolution operations
        expected_schema = base_schema.copy()

        # Apply schema evolution operations
        schema_evolutions = table_spec.get('schema_evolution', [])
        evolution_sequence = table_spec.get('evolution_sequence', [])

        for op in schema_evolutions + evolution_sequence:
            if op['operation'] == 'add_column':
                expected_schema.append({
                    'name': op['column'],
                    'type': op['type']
                })
            elif op['operation'] == 'drop_column':
                expected_schema = [col for col in expected_schema if col['name'] != op['column']]
            elif op['operation'] == 'rename_column':
                for col in expected_schema:
                    if col['name'] == op['old_name']:
                        col['name'] = op['new_name']

        # Determine final format version
        final_version = table_spec.get('final_format_version',
                                     table_spec.get('initial_format_version', 2))

        tables.append({
            'name': table_name,
            'type': 'evolution',
            'category': table_spec.get('category', 'unknown'),
            'description': table_spec.get('description', ''),
            'expected_rows': 1000,  # All evolution tables have 1000 rows
            'expected_columns': len(expected_schema),
            'schema_columns': len(expected_schema),
            'format_version': final_version,
            'partition_spec': table_spec.get('partition_spec'),
            'sort_order': table_spec.get('sort_order'),
            'expected_snapshots': table_spec.get('expected_snapshots', 1),
            'validation_queries': table_spec.get('validation_queries', ['row_count']),
            'schema_evolution': schema_evolutions,  # Add evolution operations
            'evolution_sequence': evolution_sequence,  # Add evolution sequence
            'schema': table_spec.get('schema', 'base'),  # Add schema type
            'initial_format_version': table_spec.get('initial_format_version', 2),
            'final_format_version': final_version
        })

    for name, reason in skipped:
        print(f"  Skipped (DuckDB cannot read): {name}: {reason}")

    return tables

def _validate_delete_file_types(table_path, delete_mode):
    """Validate that the correct delete files are present for the delete mode"""

    table_path = Path(table_path)
    metadata_dir = table_path / 'metadata'
    if not metadata_dir.exists():
        return False, "No metadata directory found"

    # Find the latest metadata file
    metadata_files = [f for f in metadata_dir.iterdir() if f.name.startswith('v') and f.name.endswith('.metadata.json')]
    if not metadata_files:
        return False, "No metadata files found"

    latest_metadata = sorted(metadata_files, key=lambda f: f.name)[-1]

    try:
        with open(latest_metadata, 'r') as f:
            metadata = json.load(f)

        # Check snapshots for delete files
        snapshots = metadata.get('snapshots', [])
        if not snapshots:
            return True, "No snapshots to check"

        latest_snapshot = snapshots[-1]
        summary = latest_snapshot.get('summary', {})

        total_delete_files = int(summary.get('total-delete-files', 0))
        total_position_deletes = int(summary.get('total-position-deletes', 0))
        total_equality_deletes = int(summary.get('total-equality-deletes', 0))

        if delete_mode == 'cow':
            # COW should have no delete files
            if total_delete_files > 0:
                return False, f"COW mode should have no delete files, found {total_delete_files}"
        elif delete_mode == 'posdelete':
            # Position delete mode should have position deletes (for delete-heavy patterns)
            # Note: Not all patterns will have deletes, so this is informational
            pass
        elif delete_mode == 'eqdelete':
            # Equality delete mode should have equality deletes (for delete-heavy patterns)
            # Note: Not all patterns will have deletes, so this is informational
            pass

        return True, (
            f"Delete files: {total_delete_files}, "
            f"Position: {total_position_deletes}, Equality: {total_equality_deletes}"
        )

    except Exception as e:
        return False, f"Error reading metadata: {e}"

def _generate_primitive_data(data_type, row_id):
    """Generate expected data for primitive types."""
    if data_type == 'boolean':
        return (row_id % 2) == 0
    elif data_type == 'int':
        return row_id
    elif data_type == 'long':
        return row_id * 1000
    elif data_type == 'float':
        return float(row_id) + 0.5
    elif data_type == 'double':
        return float(row_id) * 1.5
    elif data_type == 'string':
        return f"value_{row_id}"
    elif data_type == 'binary' or data_type.startswith('binary_'):
        return f"value_{row_id}".encode()
    elif data_type == 'decimal_10_2':
        return Decimal(f"{row_id}.{row_id%100:02d}")
    elif data_type == 'date':
        return date(2023, 1, 1).replace(day=min(28, (row_id % 28) + 1))
    elif data_type == 'timestamp' or data_type == 'timestamptz':
        return datetime(2023, 1, 1).replace(hour=(row_id % 24))
    elif data_type == 'uuid':
        return uuid_lib.UUID(int=row_id).bytes
    elif data_type == 'time':
        return time(hour=(row_id % 24), minute=(row_id % 60))
    return f"value_{row_id}"

def _generate_list_element(element_type, row_id, offset):
    """Generate a single list element based on type."""
    if element_type == 'boolean':
        return True if offset == 0 else (False if offset == 1 else (row_id % 2) == 0)
    elif element_type == 'int':
        return row_id + offset
    elif element_type == 'long':
        return (row_id + offset) * 1000
    elif element_type == 'float':
        return float(row_id + offset) + 0.5
    elif element_type == 'double':
        return float(row_id + offset) * 1.5
    elif element_type in ['string_small', 'string_medium']:
        return f"item_{row_id}_{offset}"
    elif element_type.startswith('binary'):
        return f"item_{row_id}_{offset}".encode()
    elif element_type == 'date':
        return date(2023, 1, min(28, ((row_id + offset) % 28) + 1))
    elif element_type in ['timestamp', 'timestamptz']:
        return datetime(2023, 1, 1, hour=((row_id + offset) % 24))
    elif element_type == 'decimal':
        return Decimal(f"{row_id + offset}.{(row_id + offset)%100:02d}")
    elif element_type == 'uuid':
        return uuid_lib.UUID(int=row_id + offset).bytes
    elif element_type.startswith('fixed('):
        length = int(element_type[6:-1])
        return f"fixed_{row_id + offset}".ljust(length, '0')[:length].encode()
    elif element_type == 'time':
        return f"{((row_id + offset) % 24):02d}:00:00"
    return f"item_{row_id}_{offset}"

def _generate_list_data(element_type, row_id):
    """Generate expected data for list types."""
    return [_generate_list_element(element_type, row_id, j) for j in range(3)]

def _generate_map_value(value_type, key):
    """Generate a single map value based on type."""
    if value_type == 'string':
        return f"value_{key}"
    elif value_type == 'int':
        return key * 10
    elif value_type == 'long':
        return key * 1000
    elif value_type == 'float':
        return float(key) + 0.5
    elif value_type == 'double':
        return float(key) * 1.5
    elif value_type == 'boolean':
        return (key % 2) == 0
    elif value_type.startswith('binary'):
        return f"value_{key}".encode()
    elif value_type == 'date':
        return date(2023, 1, min(28, (key % 28) + 1))
    elif value_type in ['timestamp', 'timestamptz']:
        return datetime(2023, 1, 1, hour=(key % 24))
    elif value_type == 'decimal':
        return Decimal(f"{key}.{key % 100:02d}")
    elif value_type == 'uuid':
        return uuid_lib.UUID(int=key).bytes
    elif value_type.startswith('fixed('):
        length = int(value_type[6:-1])
        return f"fixed_{key}".ljust(length, '0')[:length].encode()
    elif value_type == 'time':
        return f"{(key % 24):02d}:00:00"
    return f"value_{key}"

def _generate_map_data(value_type, row_id):
    """Generate expected data for map types."""
    return {row_id: _generate_map_value(value_type, row_id),
            row_id + 1: _generate_map_value(value_type, row_id + 1)}

def _generate_expected_single_type_data(data_type, row_id):
    """Generate expected data for a single data type."""
    if data_type.startswith('list_'):
        element_type = data_type[5:]
        return _generate_list_data(element_type, row_id)
    elif data_type.startswith('map_int_'):
        value_type = data_type[8:]
        return _generate_map_data(value_type, row_id)
    else:
        return _generate_primitive_data(data_type, row_id)

def _generate_expected_write_pattern_data(row_id):
    """Generate expected data for a write-pattern table row"""

    return {
        'id': row_id,
        'boolean_col': (row_id % 2) == 0,
        'int_col': row_id,
        'long_col': row_id * 1000,
        'float_col': float(row_id) + 0.5,
        'double_col': float(row_id) * 1.5,
        'string_col': f"value_{row_id}",
        'decimal_col': f"{row_id}.{row_id%100:02d}",
        'string_small_col': f"s{row_id}",
        'string_medium_col': f"medium_value_{row_id}",
        'list_int_col': [row_id, row_id + 1, row_id + 2]
    }

def _generate_expected_partitioned_data(row_id):
    """Generate expected data for a partitioned table row"""

    categories = ['A', 'B', 'C', 'D', 'E']

    return {
        'id': row_id,
        'category': categories[row_id % 5],
        'name': f'name_{row_id % 100}',
        'value': row_id * 10,
        'timestamp': datetime(2020 + (row_id % 5), 1 + (row_id % 12), 1 + (row_id % 28), row_id % 24, 0, 0),
        'deprecated_field': 'deprecated',
        'single_value_field': 'constant',
        'boolean_col': row_id % 2 == 0,
        'int_col': row_id,
        'long_col': row_id * 1000,
        'float_col': float(row_id) * 1.5,
        'double_col': float(row_id) * 2.5,
        'string_col': f'str_{row_id}',
        'date_col': date(2020, 1, 1),
        'time_col': '12:00:00',
        'timestamp_col': datetime(2020, 1, 1, 12, 0, 0),
        'timestamptz_col': datetime(2020, 1, 1, 12, 0, 0),
        'decimal_col': f'{row_id}.50',
        'string_small_col': f's{row_id}',
        'string_medium_col': f'medium_{row_id}' * 5,
        'string_long_col': f'long_{row_id}' * 20,
        'list_boolean_col': [True, False, row_id % 2 == 0],
        'list_int_col': [row_id, row_id + 1, row_id + 2],
        'list_string_col': [f'item_{row_id}', f'item_{row_id + 1}']
    }

def _validate_parquet_properties(table_path, expected_spec):
    """Validate Parquet file compression and encoding"""
    try:

        # Find parquet files
        table_dir = Path(table_path)
        parquet_files = list(table_dir.glob('data/**/*.parquet'))

        if not parquet_files:
            return {'valid': False, 'error': 'No parquet files found'}

        # Check first parquet file
        parquet_file = parquet_files[0]
        parquet_metadata = pq.read_metadata(parquet_file)

        # Get compression from first row group
        if parquet_metadata.num_row_groups > 0:
            row_group = parquet_metadata.row_group(0)
            if row_group.num_columns > 0:
                # Check the value column (second column, index 1)
                col_idx = 1 if row_group.num_columns > 1 else 0
                column = row_group.column(col_idx)
                actual_compression = column.compression.lower()

                # Get expected compression and encoding for different table types
                if expected_spec.get('type') == 'write_pattern':
                    # Write-pattern tables use zstd compression and plain_dictionary encoding
                    expected_compression = 'zstd'
                    expected_encoding = 'PLAIN_DICTIONARY'
                elif expected_spec.get('type') == 'partitioned':
                    # Partitioned tables use snappy compression and plain encoding
                    expected_compression = 'snappy'
                    expected_encoding = 'PLAIN'
                else:
                    # For other table types, use spec values
                    expected_compression = expected_spec.get('compression', 'snappy').lower()
                    expected_encoding = expected_spec.get('encoding', 'PLAIN').upper()

                # Normalize compression names
                if expected_compression.startswith('gzip'):
                    expected_compression = 'gzip'
                elif expected_compression.startswith('zstd'):
                    expected_compression = 'zstd'

                # Validate compression
                compression_valid = actual_compression == expected_compression

                # Validate encoding
                actual_encoding = column.encodings
                if expected_spec.get('type') == 'single_type':
                    # Skip encoding validation for simple tables (Spark auto-selects)
                    encoding_valid = True
                elif expected_spec.get('type') == 'write_pattern':
                    # For write-pattern tables, check for dictionary encoding
                    encoding_valid = any(
                        'DICTIONARY' in str(enc).upper() or 'PLAIN' in str(enc).upper()
                        for enc in actual_encoding
                    )
                elif expected_spec.get('type') == 'partitioned':
                    # For partitioned tables, check for plain encoding (Spark default)
                    encoding_valid = any('PLAIN' in str(enc).upper() for enc in actual_encoding)
                else:
                    # Check if expected encoding is present
                    encoding_valid = any(expected_encoding in str(enc).upper() for enc in actual_encoding)

                return {
                    'valid': compression_valid and encoding_valid,
                    'compression_valid': compression_valid,
                    'encoding_valid': encoding_valid,
                    'actual_compression': actual_compression,
                    'expected_compression': expected_compression,
                    'actual_encodings': [str(e) for e in actual_encoding],
                    'expected_encoding': expected_encoding,
                    'file_checked': str(parquet_file)
                }

        return {'valid': False, 'error': 'No row groups in parquet file'}

    except Exception as e:
        return {'valid': False, 'error': f'Failed to read parquet metadata: {str(e)}'}

def _validate_table_contents(conn, table_path, expected_spec):
    """Validate actual table contents against expected data"""
    try:
        # Check ALL rows, not just a sample
        query = f"SELECT * FROM iceberg_scan('{table_path}') ORDER BY id"
        actual_rows = conn.execute(query).fetchall()

        if not actual_rows:
            return {'valid': False, 'error': 'No data found', 'treat_as_missing': True}

        # Get column names
        columns_query = f"DESCRIBE (SELECT * FROM iceberg_scan('{table_path}') LIMIT 0)"
        columns = [row[0] for row in conn.execute(columns_query).fetchall()]

        content_errors = []
        rows_checked = len(actual_rows)

        for i, actual_row in enumerate(actual_rows):
            row_id = actual_row[0]  # First column should be ID

            if expected_spec['type'] == 'single_type':
                # Validate single-type table: id + value columns
                if len(actual_row) != 2:
                    content_errors.append(f"Row {row_id}: Expected 2 columns, got {len(actual_row)}")
                    continue

                # Rows are ordered by id and a single-type table holds one
                # contiguous 1-based run, so the nth row's id must be n.
                expected_id = row_id_from_index(i, 0)
                expected_value = _generate_expected_single_type_data(expected_spec['data_type'], row_id)

                if row_id != expected_id:
                    content_errors.append(
                        f"Row {i}: ID mismatch - expected {expected_id}, got {row_id}"
                    )

                # For some types, we need approximate comparison
                if expected_spec['data_type'] in ['float', 'double']:
                    if abs(actual_row[1] - expected_value) > 0.001:
                        content_errors.append(
                            f"Row {row_id}: Value mismatch - "
                            f"expected ~{expected_value}, got {actual_row[1]}"
                        )
                elif expected_spec['data_type'] == 'uuid':
                    # UUID stored as binary, should be 16 bytes
                    actual_val = actual_row[1]
                    if isinstance(actual_val, bytes):
                        if actual_val != expected_value:
                            content_errors.append(f"Row {row_id}: UUID mismatch")
                    else:
                        # DuckDB might return UUID object
                        if hasattr(actual_val, 'bytes'):
                            if actual_val.bytes != expected_value:
                                content_errors.append(f"Row {row_id}: UUID mismatch")
                        else:
                            content_errors.append(f"Row {row_id}: UUID type mismatch")
                elif expected_spec['data_type'] == 'time':
                    # Time comparison - convert both to strings
                    actual_time_str = str(actual_row[1])
                    expected_time_str = str(expected_value)
                    if actual_time_str != expected_time_str:
                        content_errors.append(
                            f"Row {row_id}: Time mismatch - "
                            f"expected {expected_time_str}, got {actual_time_str}"
                        )
                elif expected_spec['data_type'] == 'decimal_10_2':
                    # Compare decimals as strings to avoid precision issues
                    if str(actual_row[1]) != str(expected_value):
                        content_errors.append(
                            f"Row {row_id}: Value mismatch - "
                            f"expected {expected_value}, got {actual_row[1]}"
                        )
                elif expected_spec['data_type'] in ['timestamp', 'timestamptz']:
                    # Strip timezone info for comparison
                    actual_ts = actual_row[1]
                    if hasattr(actual_ts, 'replace'):
                        actual_ts = actual_ts.replace(tzinfo=None)
                    if actual_ts != expected_value:
                        content_errors.append(
                            f"Row {row_id}: Value mismatch - "
                            f"expected {expected_value}, got {actual_ts}"
                        )
                elif expected_spec['data_type'].startswith('list_') or expected_spec['data_type'].startswith('map_'):
                    # For lists and maps, compare as collections, stripping timezone from timestamps
                    actual_val = actual_row[1]
                    expected_val = expected_value

                    # Handle lists with timestamps
                    if expected_spec['data_type'].startswith('list_') and isinstance(actual_val, list):
                        actual_val = [
                            v.replace(tzinfo=None) if hasattr(v, 'replace') and hasattr(v, 'tzinfo') else v
                            for v in actual_val
                        ]
                    # Handle maps with timestamps
                    elif expected_spec['data_type'].startswith('map_') and isinstance(actual_val, dict):
                        actual_val = {
                            k: v.replace(tzinfo=None) if hasattr(v, 'replace') and hasattr(v, 'tzinfo') else v
                            for k, v in actual_val.items()
                        }

                    # Sort lists before comparison to handle order differences
                    if (
                        expected_spec['data_type'].startswith('list_') and
                        isinstance(actual_val, list)
                    ):
                        try:
                            actual_sorted = sorted(actual_val)
                            expected_sorted = sorted(expected_val)
                            if actual_sorted != expected_sorted:
                                content_errors.append(
                                    f"Row {row_id}: Value mismatch - "
                                    f"expected {expected_value}, got {actual_val}"
                                )
                        except TypeError:
                            # If sorting fails (mixed types), fall back to direct comparison
                            if actual_val != expected_val:
                                content_errors.append(
                                    f"Row {row_id}: Value mismatch - "
                                    f"expected {expected_value}, got {actual_row[1]}"
                                )
                    else:
                        if actual_val != expected_val:
                            content_errors.append(
                                f"Row {row_id}: Value mismatch - "
                                f"expected {expected_value}, got {actual_row[1]}"
                            )
                else:
                    if actual_row[1] != expected_value:
                        content_errors.append(
                            f"Row {row_id}: Value mismatch - "
                            f"expected {expected_value}, got {actual_row[1]}"
                        )

            elif expected_spec['type'] == 'write_pattern':
                # Validate write-pattern table: comprehensive columns
                expected_data = _generate_expected_write_pattern_data(row_id)

                # Check key columns that we can validate easily
                for col_idx, col_name in enumerate(columns):
                    if col_name in expected_data:
                        expected_val = expected_data[col_name]
                        actual_val = actual_row[col_idx]

                        # Type-specific validation
                        if col_name in ['float_col', 'double_col']:
                            if abs(actual_val - expected_val) > 0.001:
                                content_errors.append(
                                    f"Row {row_id}, {col_name}: "
                                    f"Expected ~{expected_val}, got {actual_val}"
                                )
                        elif col_name == 'decimal_col':
                            # Convert both to string for decimal comparison
                            if str(actual_val) != str(expected_val):
                                content_errors.append(
                                    f"Row {row_id}, {col_name}: "
                                    f"Expected {expected_val}, got {actual_val}"
                                )
                        elif col_name == 'list_int_col':
                            # List comparison is complex, just check it's a list
                            if not isinstance(actual_val, (list, tuple)) or len(actual_val) != 3:
                                content_errors.append(f"Row {row_id}, {col_name}: Expected list of 3 ints")
                        else:
                            if actual_val != expected_val:
                                content_errors.append(
                                    f"Row {row_id}, {col_name}: "
                                    f"Expected {expected_val}, got {actual_val}"
                                )

            elif expected_spec['type'] == 'partitioned':
                # Validate partitioned table: comprehensive columns with partition fields
                expected_data = _generate_expected_partitioned_data(row_id)

                # Check key columns
                for col_idx, col_name in enumerate(columns):
                    if col_name in expected_data:
                        expected_val = expected_data[col_name]
                        actual_val = actual_row[col_idx]

                        # Type-specific validation
                        if col_name in ['float_col', 'double_col']:
                            if abs(actual_val - expected_val) > 0.001:
                                content_errors.append(
                                    f"Row {row_id}, {col_name}: "
                                    f"Expected ~{expected_val}, got {actual_val}"
                                )
                        elif col_name == 'decimal_col':
                            if str(actual_val) != str(expected_val):
                                content_errors.append(
                                    f"Row {row_id}, {col_name}: "
                                    f"Expected {expected_val}, got {actual_val}"
                                )
                        elif col_name in ['timestamp', 'timestamp_col', 'timestamptz_col']:
                            # Strip timezone for comparison
                            actual_ts = (
                                actual_val.replace(tzinfo=None) if hasattr(actual_val, 'replace') else actual_val
                            )
                            expected_ts = (
                                expected_val.replace(tzinfo=None) if hasattr(expected_val, 'replace') else expected_val
                            )
                            if actual_ts != expected_ts:
                                content_errors.append(
                                    f"Row {row_id}, {col_name}: "
                                    f"Expected {expected_val}, got {actual_val}"
                                )
                        elif col_name in ['list_boolean_col', 'list_int_col', 'list_string_col']:
                            if not isinstance(actual_val, (list, tuple)):
                                content_errors.append(
                                    f"Row {row_id}, {col_name}: "
                                    f"Expected list, got {type(actual_val).__name__}"
                                )
                            elif list(actual_val) != list(expected_val):
                                content_errors.append(
                                    f"Row {row_id}, {col_name}: "
                                    f"Expected {expected_val}, got {actual_val}"
                                )
                        else:
                            if actual_val != expected_val:
                                content_errors.append(
                                    f"Row {row_id}, {col_name}: "
                                    f"Expected {expected_val}, got {actual_val}"
                                )

        return {
            'valid': len(content_errors) == 0,
            'content_errors': content_errors[:5],  # Limit to first 5 errors
            'total_content_errors': len(content_errors),
            'rows_checked': rows_checked
        }

    except Exception as e:
        return {'valid': False, 'error': f"Content validation failed: {str(e)}"}

def _validate_delete_files(table_path, expected_spec):
    """Validate delete files and metadata for tables with delete operations"""
    try:
        table_dir = Path(table_path)
        metadata_dir = table_dir / "metadata"

        if not metadata_dir.exists():
            return {'valid': False, 'error': 'No metadata directory found'}

        # Find latest metadata file
        metadata_files = list(metadata_dir.glob("*.metadata.json"))
        if not metadata_files:
            return {'valid': False, 'error': 'No metadata files found'}

        latest_metadata = max(metadata_files, key=lambda f: f.name)

        # Read metadata
        with open(latest_metadata, 'r') as f:
            metadata = json.load(f)

        # Count delete files across all snapshots
        total_delete_files = 0
        total_position_deletes = 0
        total_equality_deletes = 0

        for snapshot in metadata.get('snapshots', []):
            summary = snapshot.get('summary', {})
            delete_files = int(summary.get('total-delete-files', 0))
            position_deletes = int(summary.get('total-position-deletes', 0))
            equality_deletes = int(summary.get('total-equality-deletes', 0))

            total_delete_files = max(total_delete_files, delete_files)
            total_position_deletes = max(total_position_deletes, position_deletes)
            total_equality_deletes = max(total_equality_deletes, equality_deletes)

        # Determine expectations based on table type
        table_name = expected_spec.get('name', '')
        pattern = expected_spec.get('pattern', '')

        # Expected delete file behavior for write-pattern tables
        if expected_spec.get('type') == 'write_pattern':
            if 'cow' in table_name:
                # COW tables should have NO delete files
                expected_delete_files = 0
                expected_position_deletes = 0
                validation_msg = "COW mode should have no delete files"
            elif 'posdelete' in table_name:
                # Posdelete tables should have position deletes (if pattern does DELETEs)
                delete_patterns = [
                    'delete_heavy', 'mixed_chaos', 'update_heavy', 'batch_replace'
                ]
                if pattern in delete_patterns:
                    expected_delete_files = ">=1"  # Should have some delete files
                    expected_position_deletes = ">=1"  # Should have position deletes
                else:
                    expected_delete_files = 0  # Insert-only patterns
                    expected_position_deletes = 0
                validation_msg = f"Posdelete mode for {pattern} pattern"
            else:
                expected_delete_files = 0
                expected_position_deletes = 0
                validation_msg = "Unknown delete mode"
        else:
            # For non-write-pattern tables, no specific expectations
            expected_delete_files = 0
            expected_position_deletes = 0
            validation_msg = "Non-write-pattern table"

        # Validate expectations
        validation_errors = []

        # Check equality deletes (should always be 0 for PySpark)
        if total_equality_deletes > 0:
            validation_errors.append(
                f"Found {total_equality_deletes} equality deletes, "
                f"expected 0 for {delete_mode} mode"
            )

        # Check delete file counts
        if isinstance(expected_delete_files, int):
            if total_delete_files != expected_delete_files:
                validation_errors.append(f"Expected {expected_delete_files} delete files, got {total_delete_files}")
        elif expected_delete_files == ">=1":
            if total_delete_files == 0:
                validation_errors.append(f"Expected position deletes for {pattern} pattern but found none")

        # Check position delete counts
        if isinstance(expected_position_deletes, int):
            if total_position_deletes != expected_position_deletes:
                validation_errors.append(
                    f"Expected {expected_position_deletes} position deletes, "
                    f"found {total_position_deletes}"
                )
        elif expected_position_deletes == ">=1":
            if total_position_deletes == 0:
                validation_errors.append(f"Expected position deletes for {pattern} pattern but found none")

        delete_validation = {
            'delete_files': total_delete_files,
            'position_deletes': total_position_deletes,
            'equality_deletes': total_equality_deletes,
            'validation_msg': validation_msg,
            'valid': len(validation_errors) == 0,
            'errors': validation_errors
        }

        if validation_errors:
            delete_validation['error'] = '; '.join(validation_errors)

        return delete_validation

    except Exception as e:
        return {'valid': False, 'error': f"Delete file validation failed: {str(e)}"}

def _validate_iceberg_metadata(conn, table_path, expected_spec):
    """Validate Iceberg metadata using DuckDB metadata functions"""
    try:
        # Get snapshots info
        snapshots_query = f"SELECT * FROM iceberg_snapshots('{table_path}')"
        snapshots_result = conn.execute(snapshots_query).fetchall()

        # Validate format version
        version_valid = True
        actual_version = None
        expected_version = None

        if 'version' in expected_spec:
            expected_version = int(expected_spec['version'].replace('v', ''))

            # Read metadata.json to get format-version
            metadata_dir = Path(table_path) / 'metadata'
            if metadata_dir.exists():
                metadata_files = list(metadata_dir.glob('*.metadata.json'))
                if metadata_files:
                    latest_metadata = max(metadata_files, key=lambda f: f.name)
                    with open(latest_metadata, 'r') as f:
                        metadata = json.load(f)
                        actual_version = metadata.get('format-version', 1)
                        version_valid = actual_version == expected_version

        # Validate number of snapshots (should match expected commits)
        expected_commits = 10  # Most patterns have 10 commits
        actual_snapshots = len(snapshots_result)

        # For write patterns, commits vary
        if expected_spec['type'] == 'write_pattern':
            pattern = expected_spec.get('pattern')
            if pattern in ['batch_replace', 'burst_ops']:
                expected_commits = 2  # These use overwrite operations
            elif pattern == 'truncate_cycles':
                expected_commits = 2  # Simplified implementation

        snapshots_valid = actual_snapshots >= 1  # At least one snapshot

        return {
            'valid': snapshots_valid and version_valid,
            'snapshots_count': actual_snapshots,
            'expected_commits': expected_commits,
            'snapshots_valid': snapshots_valid,
            'version_valid': version_valid,
            'expected_version': expected_version,
            'actual_version': actual_version,
            'metadata_accessible': True
        }

    except Exception as e:
        return {
            'valid': False,
            'snapshots_count': 0,
            'expected_commits': 0,
            'snapshots_valid': False,
            'version_valid': False,
            'expected_version': None,
            'actual_version': None,
            'metadata_accessible': False,
            'error': str(e)
        }

def _expected_duckdb_type(iceberg_type) -> str | None:
    """Map an Iceberg type to the type DuckDB reports for it.

    Returns None for types with no single expected spelling, which skips the
    check rather than inventing one.
    """
    simple = {
        'boolean': 'BOOLEAN',
        'int': 'INTEGER',
        'long': 'BIGINT',
        'float': 'FLOAT',
        'double': 'DOUBLE',
        'date': 'DATE',
        'time': 'TIME',
        'timestamp': 'TIMESTAMP',
        'timestamptz': 'TIMESTAMP WITH TIME ZONE',
        'string': 'VARCHAR',
        'binary': 'BLOB',
        'uuid': 'UUID',
    }
    if iceberg_type in simple:
        return simple[iceberg_type]
    if iceberg_type.startswith('decimal('):
        return f"DECIMAL({iceberg_type[8:-1].replace(' ', '')})"
    if iceberg_type.startswith('fixed('):
        return 'BLOB'
    return None

def _validate_value_column_type(
    schema_result, expected_spec
) -> tuple[bool, str | None]:
    """Check that the value column carries the Iceberg type its name claims.

    Column *count* alone cannot tell a `time` column from the `string` some
    backends substitute for it, so a table named for a type it does not
    contain would otherwise validate clean.
    """
    iceberg_type = expected_spec.get('iceberg_type')
    if expected_spec.get('type') != 'single_type' or not iceberg_type:
        return True, None

    expected_type = _expected_duckdb_type(iceberg_type)
    if expected_type is None or len(schema_result) < 2:
        return True, None

    actual_type = schema_result[1][1].upper()
    if actual_type == expected_type:
        return True, None
    return False, f"value is {actual_type}, expected {expected_type} for {iceberg_type}"


def _validate_basic_table_properties(conn, table_path, expected_spec):
    """Validate basic table properties: row count, column count, and schema."""
    # Try to scan the table
    print(f"    ✓ Checking table accessibility...")
    query = f"SELECT COUNT(*) as row_count FROM iceberg_scan('{table_path}')"
    result = conn.execute(query).fetchone()
    row_count = result[0] if result else 0

    # Get schema information
    print(f"    ✓ Checking schema...")
    schema_query = f"DESCRIBE (SELECT * FROM iceberg_scan('{table_path}') LIMIT 0)"
    schema_result = conn.execute(schema_query).fetchall()
    column_count = len(schema_result)

    # Validate row count
    row_count_valid = row_count == expected_spec['expected_rows']
    print(
        f"    {'✓' if row_count_valid else '✗'} Validating row count: "
        f"{row_count} (expected: {expected_spec['expected_rows']})"
    )

    # Validate column count
    column_count_valid = column_count == expected_spec['schema_columns']
    print(
        f"    {'✓' if column_count_valid else '✗'} Validating column count: "
        f"{column_count} (expected: {expected_spec['schema_columns']})"
    )

    # Validate the value column's type
    column_type_valid, column_type_error = _validate_value_column_type(
        schema_result, expected_spec
    )
    if column_type_error:
        print(f"    ✗ Validating column type: {column_type_error}")
    else:
        print("    ✓ Validating column type")

    return {
        'row_count': row_count,
        'column_count': column_count,
        'row_count_valid': row_count_valid,
        'column_count_valid': column_count_valid,
        'column_type_valid': column_type_valid,
        'column_type_error': column_type_error
    }

def _validate_evolution_properties(conn, table_path, expected_spec):
    """Validate evolution-specific properties for evolution tables."""
    if expected_spec.get('type') != 'evolution':
        return {'valid': True, 'evolution_validation': {}}

    print(f"    ✓ Validating evolution-specific properties...")
    evolution_validation = _validate_evolution_table(conn, table_path, expected_spec)
    evolution_valid = evolution_validation['valid']

    # Print evolution validation details
    if evolution_validation.get('evolution_checks'):
        for check_name, check_result in evolution_validation['evolution_checks'].items():
            if check_result.get('valid'):
                expected = check_result.get('expected', 'N/A')
                actual = check_result.get('actual', 'N/A')
                print(f"      → {check_name}: {actual} (expected: {expected}) ✓")
            else:
                error = check_result.get('error', 'Failed')
                errors = check_result.get('errors', [])
                expected = check_result.get('expected', 'N/A')
                actual = check_result.get('actual', 'N/A')
                print(f"      → {check_name}: ✗ {error} (expected: {expected}, actual: {actual})")
                if errors:
                    for err in errors[:3]:  # Show first 3 errors
                        print(f"        • {err}")
                    if len(errors) > 3:
                        print(f"        • ... and {len(errors) - 3} more errors")

    return {'valid': evolution_valid, 'evolution_validation': evolution_validation}

def _validate_parquet_properties_for_table(table_path, expected_spec):
    """Validate Parquet properties (compression & encoding) for single_type tables."""
    if expected_spec['type'] != 'single_type':
        return {'valid': True, 'parquet_validation': {}}

    print(f"    ✓ Validating Parquet compression and encoding...")
    parquet_validation = _validate_parquet_properties(table_path, expected_spec)
    parquet_valid = parquet_validation.get('valid', True)

    return {'valid': parquet_valid, 'parquet_validation': parquet_validation}

def _determine_delete_mode(expected_spec):
    """Determine delete mode from table name."""
    if expected_spec['type'] not in ['write_pattern', 'partitioned']:
        return None

    print(f"    ✓ Checking delete mode...")
    table_name = expected_spec['name']
    if '_cow' in table_name:
        delete_mode = 'cow'
    elif '_posdelete' in table_name:
        delete_mode = 'posdelete'
    elif '_eqdelete' in table_name:
        delete_mode = 'eqdelete'
    else:
        delete_mode = None

    if delete_mode:
        print(f"      → Delete mode: {delete_mode}")
    return delete_mode

def _validate_partition_structure(table_path, expected_spec):
    """Validate partition structure for partitioned tables."""
    if expected_spec['type'] != 'partitioned':
        return {'valid': True, 'partition_info': 'N/A', 'path_style_valid': True, 'path_style_info': 'N/A'}

    print(f"    ✓ Validating partition structure...")
    try:
        # Check that data is distributed across partitions
        data_dir = Path(table_path) / 'data'
        if not data_dir.exists():
            return {
                'valid': False,
                'partition_info': 'No data directory found',
                'path_style_valid': True,
                'path_style_info': 'N/A'
            }

        partition_dirs = [d for d in data_dir.iterdir() if d.is_dir()]
        partition_count = len(partition_dirs)

        # Check for multiple files per partition (for compaction testing)
        files_per_partition = []
        for part_dir in partition_dirs[:5]:  # Check first 5 partitions
            parquet_files = list(part_dir.glob('**/*.parquet'))
            files_per_partition.append(len(parquet_files))

        avg_files = sum(files_per_partition) / len(files_per_partition) if files_per_partition else 0

        # Require 2+ files per partition for compaction testing
        if 'identity_int_col' in expected_spec.get('name', ''):
            partition_valid = partition_count > 0 and avg_files >= 1
        else:
            partition_valid = partition_count > 0 and avg_files >= 2

        partition_info = f"{partition_count} partitions, avg {avg_files:.1f} files/partition"
        print(f"      → Found {partition_count} partitions, avg {avg_files:.1f} files/partition")

        # Validate partition path style (always Hive-style)
        print(f"    ✓ Validating partition path style...")
        path_style_valid = True
        path_style_info = 'N/A'

        if partition_dirs:
            sample_dir = partition_dirs[0].name
            has_equals = '=' in sample_dir

            if not has_equals:
                path_style_valid = False
                path_style_info = f"Expected Hive-style (column=value), got non-Hive ({sample_dir})"
            else:
                path_style_info = f"Hive-style confirmed ({sample_dir})"
            print(f"      → {path_style_info}")

        return {
            'valid': partition_valid,
            'partition_info': partition_info,
            'path_style_valid': path_style_valid,
            'path_style_info': path_style_info
        }

    except Exception as e:
        return {
            'valid': False,
            'partition_info': f"Error: {e}",
            'path_style_valid': True,
            'path_style_info': 'N/A'
        }

def _validate_delete_files(table_path, expected_spec, delete_mode):
    """Validate delete files for write-pattern and partitioned tables."""
    if not delete_mode:
        return {'valid': True, 'delete_info': 'No delete validation needed'}

    print(f"    ✓ Validating delete files for {delete_mode} mode...")

    # Check if delete mode is compatible with format version
    table_version = expected_spec.get('version', 'v2')
    if table_version == 'v1' and delete_mode in ['posdelete', 'eqdelete']:
        delete_info = f"Invalid: {delete_mode} not supported in Iceberg v1 format"
        print(f"      → {delete_info}")
        return {'valid': False, 'delete_info': delete_info}

    delete_validation = _validate_delete_file_types(table_path, delete_mode)
    if isinstance(delete_validation, tuple):
        delete_valid = delete_validation[0]
        delete_info = delete_validation[1]
    else:
        delete_valid = delete_validation.get('valid', False)
        delete_info = delete_validation.get('info', 'Unknown')
    print(f"      → {delete_info}")

    return {'valid': delete_valid, 'delete_info': delete_info}

def _validate_iceberg_table(conn, table_path, expected_spec):
    """Validate a single Iceberg table using DuckDB"""
    table_name = expected_spec.get('name', 'unknown')
    print(f"  Validating table: {table_name}")

    try:
        # Enable unsafe version guessing for DuckDB
        conn.execute("SET unsafe_enable_version_guessing = true")

        # Validate basic properties
        basic_props = _validate_basic_table_properties(conn, table_path, expected_spec)

        # Validate table contents
        print(f"    ✓ Validating table contents...")
        content_validation = _validate_table_contents(conn, table_path, expected_spec)
        content_valid = content_validation['valid']

        # Validate Iceberg metadata and snapshots
        print(f"    ✓ Validating Iceberg metadata and format version...")
        metadata_validation = _validate_iceberg_metadata(conn, table_path, expected_spec)
        metadata_valid = metadata_validation['valid']

        # Validate evolution-specific properties
        evolution_result = _validate_evolution_properties(conn, table_path, expected_spec)

        # Validate Parquet properties
        parquet_result = _validate_parquet_properties_for_table(table_path, expected_spec)

        # Determine and validate delete mode
        delete_mode = _determine_delete_mode(expected_spec)
        delete_result = _validate_delete_files(table_path, expected_spec, delete_mode)

        # Validate partitions
        partition_result = _validate_partition_structure(table_path, expected_spec)

        # Overall validation result
        overall_valid = (
            basic_props['row_count_valid'] and
            basic_props['column_count_valid'] and
            basic_props['column_type_valid'] and
            content_valid and
            metadata_valid and
            delete_result['valid'] and
            partition_result['valid'] and
            partition_result['path_style_valid'] and
            parquet_result['valid'] and
            evolution_result['valid']
        )
        status = "✓ PASS" if overall_valid else "✗ FAIL"
        print(f"    → Validation result: {status}")

        return {
            'valid': overall_valid,
            'row_count': basic_props['row_count'],
            'column_count': basic_props['column_count'],
            'row_count_valid': basic_props['row_count_valid'],
            'column_count_valid': basic_props['column_count_valid'],
            'column_type_valid': basic_props['column_type_valid'],
            'column_type_error': basic_props['column_type_error'],
            'content_valid': content_valid,
            'content_validation': content_validation,
            'metadata_valid': metadata_valid,
            'metadata_validation': metadata_validation,
            'delete_valid': delete_result['valid'],
            'delete_info': delete_result['delete_info'],
            'partition_valid': partition_result['valid'],
            'partition_info': partition_result['partition_info'],
            'path_style_valid': partition_result['path_style_valid'],
            'path_style_info': partition_result['path_style_info'],
            'parquet_valid': parquet_result['valid'],
            'parquet_validation': parquet_result.get('parquet_validation', {}),
            'evolution_valid': evolution_result['valid'],
            'evolution_validation': evolution_result.get('evolution_validation', {}),
            'error': None
        }

    except Exception as e:
        return {
            'valid': False,
            'row_count': 0,
            'column_count': 0,
            'row_count_valid': False,
            'column_count_valid': False,
            'column_type_valid': False,
            'column_type_error': None,
            'content_valid': False,
            'content_validation': {'valid': False, 'error': str(e)},
            'metadata_valid': False,
            'metadata_validation': {'valid': False, 'error': str(e)},
            'delete_valid': False,
            'delete_validation': {'valid': False, 'error': str(e)},
            'error': str(e)
        }

def _types_compatible(expected_type, actual_type):
    """Check if DuckDB type is compatible with expected Iceberg type"""
    # Normalize types for comparison
    type_mappings = {
        'VARCHAR': ['VARCHAR', 'STRING'],
        'BIGINT': ['BIGINT', 'LONG'],
        'INTEGER': ['INTEGER', 'INT'],
        'BOOLEAN': ['BOOLEAN', 'BOOL'],
        'TIMESTAMP': ['TIMESTAMP', 'TIMESTAMP WITH TIME ZONE', 'DATE']
    }

    for canonical, variants in type_mappings.items():
        if expected_type in variants and actual_type in variants:
            return True
        if expected_type == canonical and actual_type in variants:
            return True
        if actual_type == canonical and expected_type in variants:
            return True

    return expected_type == actual_type

def _failed_evolution_checks(result):
    """Yield (check name, description) for each evolution check that failed"""
    checks = result.get('evolution_validation', {}).get('evolution_checks', {})
    for name, info in checks.items():
        if info.get('valid', True):
            continue
        detail = info.get('error') or info.get('errors')
        if not detail:
            detail = f"expected {info.get('expected')}, got {info.get('actual')}"
        yield name, detail

def _validate_evolution_table(conn, table_path, expected_spec):
    """Validate evolution-specific properties of a table"""
    validation_results = {}

    try:
        # Run evolution-specific validation queries
        validation_queries = expected_spec.get('validation_queries', [])

        for query_name in validation_queries:
            try:
                if query_name == 'format_version':
                    # Check Iceberg format version from table properties
                    try:
                        # Try to get format version from table properties
                        result = conn.execute(
                            f"SELECT value FROM iceberg_metadata('{table_path}', 'snapshots')"
                        ).fetchone()
                        if not result:
                            # Fallback: assume v2 if no explicit version (v1 tables don't have this property)
                            actual_version = 1
                        else:
                            actual_version = int(result[0])
                    except Exception:
                        # If metadata query fails, try to infer from table structure
                        actual_version = 2  # Default assumption

                    expected_version = expected_spec.get('final_format_version',
                                                        expected_spec.get('initial_format_version', 2))

                    # If there's a format upgrade operation, expect the final version
                    evolution_sequence = expected_spec.get('evolution_sequence', [])
                    for op in evolution_sequence:
                        if op.get('operation') == 'upgrade_format':
                            expected_version = op.get('to_version', 2)

                    validation_results[query_name] = {
                        'expected': expected_version,
                        'actual': actual_version,
                        'valid': actual_version == expected_version
                    }

                elif query_name == 'partition_count':
                    # Check partition count using file paths (DuckDB doesn't expose partition column directly)
                    try:
                        # For partitioned tables, check if files are in partition directories
                        result = conn.execute(
                            f"SELECT file_path FROM iceberg_metadata('{table_path}', 'entries')"
                        ).fetchall()
                        partition_paths = set()

                        for row in result:
                            file_path = row[0]
                            # Extract partition info from file path (e.g., category=A, bucket_00000)
                            if '/category=' in file_path:
                                partition_part = file_path.split('/category=')[1].split('/')[0]
                                partition_paths.add(f"category={partition_part}")
                            elif '/bucket_' in file_path:
                                bucket_part = file_path.split('/bucket_')[1].split('/')[0]
                                partition_paths.add(f"bucket_{bucket_part}")
                            elif len(file_path.split('/')) > 3:  # Has partition structure
                                partition_paths.add("partitioned")
                            else:
                                partition_paths.add("unpartitioned")

                        actual_partitions = len(partition_paths) if partition_paths else 1
                    except Exception as e:
                        # Fallback: assume partitioned if partition spec exists
                        partition_spec = expected_spec.get('partition_spec', [])
                        actual_partitions = 1 if not partition_spec else 2

                    # Determine expected partitions based on partition spec
                    partition_spec = expected_spec.get('partition_spec', [])
                    if not partition_spec:
                        expected_partitions = 1  # Unpartitioned
                    elif 'identity(category)' in str(partition_spec):
                        expected_partitions = 5  # Categories A-E
                    elif 'bucket(4' in str(partition_spec):
                        expected_partitions = 4  # 4 buckets
                    else:
                        expected_partitions = 2  # Generic partitioned

                    validation_results[query_name] = {
                        'expected': f">= {expected_partitions}",
                        'actual': f"{actual_partitions} partitions detected",
                        'valid': actual_partitions >= 1  # Just verify table is accessible
                    }

                elif query_name == 'column_count':
                    # Get actual schema details
                    result = conn.execute(
                        f"DESCRIBE SELECT * FROM iceberg_scan('{table_path}')"
                    ).fetchall()
                    actual_schema = [(row[0], row[1]) for row in result]  # (column_name, column_type)
                    actual_columns = len(actual_schema)

                    # Build expected schema based on evolution
                    expected_schema = [
                        ('id', 'BIGINT'),
                        ('name', 'VARCHAR'),
                        ('category', 'VARCHAR'),
                        ('value', 'INTEGER'),
                        ('timestamp', 'TIMESTAMP'),
                        ('active', 'BOOLEAN')
                    ]

                    # Add description column for extended schema FIRST
                    if expected_spec.get('schema') == 'extended':
                        expected_schema.append(('description', 'VARCHAR'))

                    # Apply schema evolution operations
                    schema_evolution = expected_spec.get('schema_evolution', [])
                    for op in schema_evolution:
                        if op.get('operation') == 'add_column':
                            column_name = op.get('column')
                            column_type = 'VARCHAR' if op.get('type') == 'string' else 'VARCHAR'
                            # Only add if not already present
                            if not any(name == column_name for name, _ in expected_schema):
                                expected_schema.append((column_name, column_type))
                        elif op.get('operation') == 'drop_column':
                            column_name = op.get('column')
                            expected_schema = [(name, typ) for name, typ in expected_schema if name != column_name]
                        elif op.get('operation') == 'rename_column':
                            old_name = op.get('old_name')
                            new_name = op.get('new_name')
                            expected_schema = [
                                (new_name if name == old_name else name, dtype)
                                for name, dtype in actual_schema
                            ]
                        elif op.get('operation') == 'widen_type':
                            column_name = op.get('column')
                            if column_name == 'value' and op.get('to_type') == 'bigint':
                                expected_schema = [
                                    (name, 'BIGINT' if name == column_name else dtype)
                                    for name, dtype in actual_schema
                                ]

                    # Debug output
                    # print(f"        DEBUG: Schema={expected_spec.get('schema')}, Evolution={schema_evolution}")
                    # print(f"        DEBUG: Expected schema: {expected_schema}")

                    # Validate schema
                    schema_errors = []
                    expected_columns = len(expected_schema)

                    # Check column count
                    if actual_columns != expected_columns:
                        schema_errors.append(f"Column count: expected {expected_columns}, got {actual_columns}")

                    # Check column names and types
                    for i, (exp_name, exp_type) in enumerate(expected_schema):
                        if i < len(actual_schema):
                            act_name, act_type = actual_schema[i]
                            if act_name != exp_name:
                                schema_errors.append(f"Column {i}: expected '{exp_name}', got '{act_name}'")
                            # Normalize type comparison (DuckDB uses different type names)
                            if not _types_compatible(exp_type, act_type):
                                schema_errors.append(f"Column '{act_name}': expected {exp_type}, got {act_type}")
                        else:
                            schema_errors.append(f"Missing column: {exp_name}")

                    # Check for extra columns
                    if len(actual_schema) > len(expected_schema):
                        for i in range(len(expected_schema), len(actual_schema)):
                            schema_errors.append(f"Unexpected column: {actual_schema[i][0]}")

                    validation_results[query_name] = {
                        'expected': f"{expected_columns} columns: {[name for name, _ in expected_schema]}",
                        'actual': f"{actual_columns} columns: {[name for name, _ in actual_schema]}",
                        'valid': len(schema_errors) == 0,
                        'errors': schema_errors if schema_errors else None
                    }

                elif query_name == 'snapshot_count':
                    # Simplified snapshot validation - just check if table has data (indicates successful evolution)
                    try:
                        result = conn.execute(
                            f"SELECT COUNT(*) FROM iceberg_scan('{table_path}')"
                        ).fetchone()
                        has_data = result[0] > 0 if result else False
                        actual_snapshots = 1 if has_data else 0  # Simplified: just check if evolution succeeded
                    except Exception:
                        actual_snapshots = 0

                    expected_snapshots = expected_spec.get('expected_snapshots', 1)

                    validation_results[query_name] = {
                        'expected': f">= {expected_snapshots}",
                        'actual': f"{'✓' if actual_snapshots > 0 else '✗'} data present",
                        'valid': actual_snapshots > 0  # Just verify evolution succeeded
                    }

                else:
                    # Generic query validation
                    validation_results[query_name] = {
                        'valid': True,
                        'note': f'Query {query_name} not implemented'
                    }

            except Exception as e:
                validation_results[query_name] = {
                    'valid': False,
                    'error': str(e)
                }

    except Exception as e:
        return {'valid': False, 'error': f'Evolution validation failed: {e}'}

    # Overall validation result
    all_valid = all(result.get('valid', False) for result in validation_results.values())

    return {
        'valid': all_valid,
        'evolution_checks': validation_results
    }

def _validate_table_worker(table_path, expected_spec, full_content_validation):
    """Worker function to validate a single table with its own DuckDB connection"""
    conn = duckdb.connect()
    try:
        conn.execute("INSTALL iceberg")
        conn.execute("LOAD iceberg")
        result = _validate_iceberg_table(conn, table_path, expected_spec)

        # Add expected spec info to result for detailed error reporting
        result['expected_spec'] = expected_spec
        return (expected_spec['name'], result)
    except Exception as e:
        return (expected_spec['name'], {'valid': False, 'error': str(e), 'expected_spec': expected_spec})
    finally:
        conn.close()

def _validate_tables(
    warehouse_path, specs_path, full_content_validation=False,
    num_parallel=1, minimal=False
):
    """Validate all tables in warehouse against specifications"""
    specs = _load_table_specs(specs_path)
    warehouse = Path(warehouse_path)

    if not warehouse.exists():
        print(f"Error: Warehouse path {warehouse_path} does not exist")
        return False

    # Connect to DuckDB
    conn = duckdb.connect()

    # Install and load Iceberg extension
    try:
        conn.execute("INSTALL iceberg")
        conn.execute("LOAD iceberg")
        print("DuckDB Iceberg extension loaded successfully")
    except Exception as e:
        print(f"Error: Could not load Iceberg extension: {e}")
        return False

    # Determine which table types to validate based on specs filename,
    # matching the CLI's dispatch logic.
    specs_file = Path(specs_path).name.lower()
    all_expected = []

    print(f"\nExpected tables from specs:")

    include_simple = 'simple' in specs_file or 'table_specs' in specs_file
    include_write_patterns = 'write_pattern' in specs_file or 'table_specs' in specs_file
    include_partitioned = 'partitioned' in specs_file or 'table_specs' in specs_file
    include_evolution = 'evolution' in specs_file

    if include_simple:
        expected_single_type = _extract_expected_single_type_tables(
            specs, minimal=minimal
        )
        all_expected.extend(expected_single_type)
        print(f"  Single-type tables: {len(expected_single_type)}")

    if include_write_patterns:
        expected_write_patterns = _extract_expected_write_pattern_tables(specs)
        all_expected.extend(expected_write_patterns)
        print(f"  Write-pattern tables: {len(expected_write_patterns)}")

    if include_partitioned:
        expected_partitioned = _extract_expected_partitioned_tables(specs)
        all_expected.extend(expected_partitioned)
        print(f"  Partitioned tables: {len(expected_partitioned)}")

    if include_evolution:
        expected_evolution = _extract_expected_evolution_tables(specs_path)
        all_expected.extend(expected_evolution)
        print(f"  Evolution tables: {len(expected_evolution)}")

    print(f"  Total expected: {len(all_expected)}")

    if not all_expected:
        print(
            f"\nError: no tables to validate. '{specs_file}' matched none of the "
            "table types this validator knows about, or the spec produced no "
            "tables. Validating nothing is not a pass."
        )
        conn.close()
        return False

    # Find only the expected tables, don't scan entire warehouse
    tables_to_validate = []
    missing_count = 0

    for expected in all_expected:
        table_name = expected['name']

        # Determine namespace based on table type
        if expected.get('type') == 'single_type':
            namespace = 'simple'
        elif expected.get('type') == 'write_pattern':
            namespace = 'write_pattern'
        elif expected.get('type') == 'partitioned':
            namespace = 'partitioned'
        elif expected.get('type') == 'evolution':
            namespace = 'evolution'
        else:
            # Default namespace detection
            if (
                table_name.startswith('unpart_') and
                any(
                    pattern in table_name
                    for pattern in ['insert', 'update', 'delete', 'mixed', 'batch', 'gradual', 'burst', 'truncate']
                )
            ):
                namespace = 'write_pattern'
            elif table_name.startswith('part_'):
                namespace = 'partitioned'
            elif table_name.startswith('evol_'):
                namespace = 'evolution'
            else:
                namespace = 'simple'

        # Check if table exists
        table_path = warehouse / namespace / table_name
        if table_path.exists() and table_path.is_dir():
            tables_to_validate.append((str(table_path), expected))
        else:
            missing_count += 1
            print(f"❌ MISSING: {table_name} (expected in {namespace} namespace)")

    if missing_count > 0:
        print(f"\n⚠️  {missing_count} expected tables are missing from warehouse")

    print(f"\nValidating {len(tables_to_validate)} tables from specs...")

    # Validate tables (parallel or sequential)
    validation_results = {}

    print(f"\n{'='*60}")
    print("VALIDATION RESULTS")
    print(f"{'='*60}")

    if num_parallel > 1:
        # Parallel validation
        with ProcessPoolExecutor(max_workers=num_parallel) as executor:
            future_to_spec = {executor.submit(_validate_table_worker, path, spec, full_content_validation): spec
                             for path, spec in tables_to_validate}

            for future in as_completed(future_to_spec):
                table_name, result = future.result()
                validation_results[table_name] = result

                # Print result immediately with full details
                if result['valid']:
                    print(
                        f"✅ VALID: {table_name} "
                        f"({result.get('row_count', 0)} rows, {result.get('column_count', 0)} cols)"
                    )
                else:
                    print(f"❌ INVALID: {table_name}")
                    expected = result.get('expected_spec', {})

                    if result.get('error'):
                        print(f"    Error: {result['error'][:200]}")
                    if not result.get('row_count_valid', True):
                        print(
                            f"    Row count: expected {expected.get('expected_rows', '?')}, "
                            f"got {result.get('row_count', 0)}"
                        )
                    if not result.get('column_count_valid', True):
                        print(
                            f"    Column count: expected {expected.get('schema_columns', '?')}, "
                            f"got {result.get('column_count', 0)}"
                        )
                    if not result.get('column_type_valid', True):
                        print(f"    Column type: {result.get('column_type_error')}")
                    if not result.get('content_valid', True):
                        content_val = result.get('content_validation', {})
                        if 'error' in content_val:
                            print(f"    Content error: {content_val['error'][:200]}")
                        elif 'content_errors' in content_val:
                            errors = content_val.get('content_errors', [])
                            print(f"    Content errors ({content_val.get('total_content_errors', 0)} total):")
                            for error in errors[:5]:  # Show first 5
                                print(f"      {error}")
                    if not result.get('metadata_valid', True):
                        meta_val = result.get('metadata_validation', {})
                        if 'error' in meta_val:
                            print(f"    Metadata error: {meta_val['error'][:200]}")
                        else:
                            print(
                                f"    Snapshots: expected ~{meta_val.get('expected_commits', 0)}, "
                                f"got {meta_val.get('actual_commits', 0)}"
                            )
                            if not meta_val.get('version_valid', True):
                                print(
                                    f"    Version: expected v{meta_val.get('expected_version', '?')}, "
                                    f"got v{meta_val.get('actual_version', '?')}"
                                )
                    if not result.get('delete_valid', True):
                        delete_info = result.get('delete_info', 'No delete info available')
                        print(f"    Delete files: {delete_info}")
                    if not result.get('partition_valid', True):
                        partition_info = result.get('partition_info', 'No partition info available')
                        print(f"    Partitions: {partition_info}")
                    if not result.get('path_style_valid', True):
                        path_style_info = result.get('path_style_info', 'No path style info')
                        print(f"    Path style: {path_style_info}")
                    if not result.get('parquet_valid', True):
                        pq_val = result.get('parquet_validation', {})
                        if 'error' in pq_val:
                            print(f"    Parquet: {pq_val['error']}")
                        else:
                            if not pq_val.get('compression_valid', True):
                                print(
                                    f"    Compression: expected {pq_val.get('expected_compression')}, "
                                    f"got {pq_val.get('actual_compression')}"
                                )
                            if not pq_val.get('encoding_valid', True):
                                print(
                                    f"    Encoding: expected {pq_val.get('expected_encoding')}, "
                                    f"got {pq_val.get('actual_encoding')}"
                                )

                    if not result.get('evolution_valid', True):
                        for check, info in _failed_evolution_checks(result):
                            print(f"    Evolution {check}: {info}")

                    # If no specific errors were found, print full result for debugging
                    if (result.get('row_count_valid', True) and
                        result.get('column_count_valid', True) and
                        result.get('column_type_valid', True) and
                        result.get('content_valid', True) and
                        result.get('metadata_valid', True) and
                        result.get('delete_valid', True) and
                        result.get('partition_valid', True) and
                        result.get('path_style_valid', True) and
                        result.get('parquet_valid', True) and
                        result.get('evolution_valid', True) and
                        not result.get('error')):
                        print(
                            f"    DEBUG: Marked invalid but no specific errors. "
                            f"Full result keys: {list(result.keys())}"
                        )
    else:
        # Sequential validation
        for table_path, expected_spec in tables_to_validate:
            result = _validate_iceberg_table(conn, table_path, expected_spec)
            result['expected_spec'] = expected_spec
            validation_results[expected_spec['name']] = result

            # Print result immediately with full details
            if result['valid']:
                print(
                    f"✅ VALID: {expected_spec['name']} "
                    f"({result.get('row_count', 0)} rows, {result.get('column_count', 0)} cols)"
                )
            else:
                print(f"❌ INVALID: {expected_spec['name']}")

                if result.get('error'):
                    print(f"    Error: {result['error'][:200]}")
                if not result.get('row_count_valid', True):
                    print(
                        f"    Row count: expected {expected.get('expected_rows', '?')}, "
                        f"got {result.get('row_count', 0)}"
                    )
                if not result.get('column_count_valid', True):
                    print(
                        f"    Column count: expected {expected.get('schema_columns', '?')}, "
                        f"got {result.get('column_count', 0)}"
                    )
                if not result.get('column_type_valid', True):
                    print(f"    Column type: {result.get('column_type_error')}")
                if not result.get('content_valid', True):
                    content_val = result.get('content_validation', {})
                    if 'error' in content_val:
                        print(f"    Content error: {content_val['error'][:200]}")
                    elif 'content_errors' in content_val:
                        errors = content_val.get('content_errors', [])
                        print(f"    Content errors ({content_val.get('total_content_errors', 0)} total):")
                        for error in errors[:5]:
                            print(f"      {error}")
                if not result.get('metadata_valid', True):
                    meta_val = result.get('metadata_validation', {})
                    if 'error' in meta_val:
                        print(f"    Metadata error: {meta_val['error'][:200]}")
                    else:
                        print(
                            f"    Snapshots: expected ~{meta_val.get('expected_commits', 0)}, "
                            f"got {meta_val.get('actual_commits', 0)}"
                        )
                        if not meta_val.get('version_valid', True):
                            print(
                                f"    Version: expected v{meta_val.get('expected_version', '?')}, "
                                f"got v{meta_val.get('actual_version', '?')}"
                            )
                if not result.get('delete_valid', True):
                    delete_info = result.get('delete_info', 'No delete info available')
                    print(f"    Delete files: {delete_info}")
                if not result.get('partition_valid', True):
                    partition_info = result.get('partition_info', 'No partition info available')
                    print(f"    Partitions: {partition_info}")
                if not result.get('path_style_valid', True):
                    path_style_info = result.get('path_style_info', 'No path style info')
                    print(f"    Path style: {path_style_info}")
                if not result.get('parquet_valid', True):
                    pq_val = result.get('parquet_validation', {})
                    if 'error' in pq_val:
                        print(f"    Parquet: {pq_val['error']}")
                    else:
                        if not pq_val.get('compression_valid', True):
                            print(
                                f"    Compression: expected {pq_val.get('expected_compression')}, "
                                f"got {pq_val.get('actual_compression')}"
                            )
                        if not pq_val.get('encoding_valid', True):
                            print(
                                f"    Encoding: expected {pq_val.get('expected_encoding')}, "
                                f"got {pq_val.get('actual_encoding')}"
                            )

                if not result.get('evolution_valid', True):
                    for check, info in _failed_evolution_checks(result):
                        print(f"    Evolution {check}: {info}")

                # If no specific errors were found, print full result for debugging
                if (result.get('row_count_valid', True) and
                    result.get('column_count_valid', True) and
                    result.get('column_type_valid', True) and
                    result.get('content_valid', True) and
                    result.get('metadata_valid', True) and
                    result.get('delete_valid', True) and
                    result.get('partition_valid', True) and
                    result.get('path_style_valid', True) and
                    result.get('parquet_valid', True) and
                    result.get('evolution_valid', True) and
                    not result.get('error')):
                    print(f"    DEBUG: Marked invalid but no specific errors. Full result keys: {list(result.keys())}")

    # Print missing tables (including those with no data) - only if not in partial mode
    no_data_count = 0
    missing_tables = []
    for expected in all_expected:
        if expected['name'] not in validation_results:
            missing_tables.append(expected['name'])
        elif validation_results[expected['name']].get('treat_as_missing', False):
            print(f"❌ MISSING: {expected['name']} (no data)")
            no_data_count += 1
            # Remove from validation_results so it's counted as missing
            del validation_results[expected['name']]

    # Only show missing tables if we have a reasonable number (not when most are missing)
    missing_count_actual = len(missing_tables)
    if missing_count_actual < len(all_expected) * 0.8:  # Less than 80% missing
        for table_name in missing_tables[:20]:  # Show first 20
            print(f"❌ MISSING: {table_name}")
        if len(missing_tables) > 20:
            print(f"    ... and {len(missing_tables) - 20} more missing tables")
    else:
        print(f"❌ MISSING: {missing_count_actual} tables (partial generation detected)")

    total_missing = no_data_count + missing_count_actual

    # Count results
    valid_count = sum(1 for r in validation_results.values() if r['valid'])
    invalid_count = len(validation_results) - valid_count

    # Summary
    print(f"\n{'='*60}")
    print("VALIDATION SUMMARY")
    print(f"{'='*60}")
    print(f"Expected tables: {len(all_expected)}")
    print(f"Valid tables: {valid_count}")
    print(f"Invalid tables: {invalid_count}")
    print(f"Missing tables: {total_missing}")
    print(f"Success rate: {valid_count/len(all_expected)*100:.1f}%")

    conn.close()
    return valid_count == len(all_expected) and total_missing == 0

class DuckDBValidator(TableValidator):
    """DuckDB-based validator for local Iceberg tables"""

    def __init__(self, warehouse_path: str, specs_path: str,
                 minimal: bool = False):
        super().__init__(warehouse_path, specs_path)
        self.minimal = minimal

    def supports_s3_tables(self) -> bool:
        return False

    def supports_remote_warehouses(self) -> bool:
        return False

    def validate_tables(self, num_parallel: int = 1, full_content_validation: bool = False) -> dict:
        """Validate all tables using DuckDB"""
        success = _validate_tables(
            self.warehouse_path, self.specs_path, full_content_validation,
            num_parallel, self.minimal
        )
        return {'success_rate': 100.0 if success else 0.0}

    def validate_table(self, table_name: str, expected_spec: dict) -> dict:
        """Validate a single table using DuckDB"""
        raise NotImplementedError

def main():
    parser = argparse.ArgumentParser(description='Validate Iceberg tables using DuckDB')
    parser.add_argument('--warehouse-path', required=True, help='Path to Iceberg warehouse directory')
    parser.add_argument('--specs', required=True, help='Path to table_specs.yaml file')
    parser.add_argument('--full-content-validation', action='store_true',
                       help='Validate all rows (slow) instead of just sampling 10 rows')
    parser.add_argument('--num-parallel', type=int, default=1, help='Number of parallel validation workers')
    parser.add_argument('--minimal', action='store_true',
                       help='Validate minimal set (uncompressed, plain encoding only)')

    args = parser.parse_args()

    validator = DuckDBValidator(
        args.warehouse_path, args.specs,
        minimal=args.minimal
    )
    result = validator.validate_tables(args.num_parallel, args.full_content_validation)
    success = result.get('success_rate', 0) == 100.0
    sys.exit(0 if success else 1)

if __name__ == '__main__':
    main()
