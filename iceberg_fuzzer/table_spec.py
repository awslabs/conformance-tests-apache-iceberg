#!/usr/bin/env python3
"""Shared utilities for table spec loading"""

import yaml

def load_table_specs(yaml_path, minimal=False):
    """Load table specs from YAML with optional minimal filtering"""
    with open(yaml_path, 'r') as f:
        specs = yaml.safe_load(f)
    
    if 'tables' in specs:
        return specs['tables']
    
    # Collect primitive types
    all_types = specs.get('primitive_types', [])
    
    # Filter out time types if minimal mode
    if minimal:
        all_types = [t for t in all_types if not ('time' in t.get('name', '') and t.get('name') not in ['timestamp', 'timestamptz'])]
        all_types = [t for t in all_types if t.get('name') not in ['uuid', 'fixed_16']]
    
    # Generate table specs
    tables = []
    compression_configs = [{'codec': 'none', 'level': None}] if minimal else []
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
                    
                    tables.append({
                        'name': table_name,
                        'schema': [
                            {'name': 'id', 'type': 'int'},
                            {'name': 'value', 'type': data_type['name']}
                        ],
                        'properties': {'format-version': str(version)}
                    })
    
    return tables
