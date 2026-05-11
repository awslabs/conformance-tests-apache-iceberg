"""Backward-compatibility shim. Use iceberg_fuzzer.config.simple instead."""

import yaml

from iceberg_fuzzer.config.simple import produce_configs


def load_table_specs(yaml_path, minimal=False):
    """Load table specs from YAML. Delegates to config/simple.produce_configs."""
    with open(yaml_path, 'r') as f:
        spec = yaml.safe_load(f)

    if 'tables' in spec:
        return spec['tables']

    return produce_configs(spec, minimal=minimal)
