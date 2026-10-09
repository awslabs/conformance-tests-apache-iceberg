#!/usr/bin/env python3
"""Shared utilities for table spec loading"""

import yaml

_COMPLEX_TYPE_PREFIXES = ('list<', 'map<', 'struct<')

# Iceberg types Spark DDL cannot represent.
_TYPES_SPARK_CANNOT_EXPRESS = ('time', 'uuid')


def select_primitive_types(specs, minimal=False):
    """Return the primitive type definitions a generate run covers.

    Every entry must declare `iceberg_type`; complex types are not supported.
    Either raises ValueError.
    """
    all_types = specs.get('primitive_types', [])

    untyped = [t.get('name', '<unnamed>') for t in all_types if not t.get('iceberg_type')]
    if untyped:
        raise ValueError(f"primitive_types entries without an `iceberg_type`: {untyped}")

    complex_types = [
        t['name'] for t in all_types
        if t['iceberg_type'].startswith(_COMPLEX_TYPE_PREFIXES)
    ]
    if complex_types:
        raise ValueError(f"Complex types are not supported: {complex_types}")

    # Minimal mode is the set every backend can write, so the types Spark
    # cannot express are left out.
    if minimal:
        all_types = [
            t for t in all_types
            if t['iceberg_type'] not in _TYPES_SPARK_CANNOT_EXPRESS
        ]

    return all_types


def load_table_specs(yaml_path, minimal=False):
    """Load table specs from YAML with optional minimal filtering"""
    with open(yaml_path, 'r') as f:
        specs = yaml.safe_load(f)

    if 'tables' in specs:
        return specs['tables']

    all_types = select_primitive_types(specs, minimal=minimal)

    # Generate table specs
    tables = []
    compression_configs = [{'codec': 'uncompressed', 'level': None}] if minimal else []
    encoding_types = ['PLAIN'] if minimal else specs.get('encoding_types', ['PLAIN'])

    if not minimal:
        for comp_config in specs.get('compression_configs', []):
            codec = comp_config['codec']
            levels = comp_config.get('levels', [None])
            for level in levels:
                compression_configs.append({'codec': codec, 'level': level})

    for data_type in all_types:
        for compression in compression_configs:
            for encoding in encoding_types:
                for version in specs.get('format_versions', [1, 2]):
                    table_name = f"unpart_{data_type['name']}_{compression['codec']}"
                    if compression['level']:
                        table_name += f"_lvl{compression['level']}"
                    table_name += f"_{encoding.lower()}_v{version}"

                    # `name` labels the table (binary_small, decimal_10_2); the
                    # column needs the canonical `iceberg_type` (binary,
                    # decimal(10,2)) that the backends' type maps key on.
                    tables.append({
                        'name': table_name,
                        'schema': [
                            {'name': 'id', 'type': 'int'},
                            {'name': 'value', 'type': data_type['iceberg_type']}
                        ],
                        'properties': {'format-version': str(version)},
                        # The name encodes these too, but a backend should not have
                        # to parse the name to find out what it is writing.
                        'compression': dict(compression),
                        'encoding': encoding,
                    })

    return tables
