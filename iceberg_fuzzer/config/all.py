"""ConfigProducer for the combined 'all' spec (table_specs.yaml).

Produces configs for simple + write_pattern + partitioned tables from a single file.
Each config is tagged with '_spec_type' so backends can dispatch correctly.
"""

from iceberg_fuzzer.config.simple import produce_configs as produce_simple
from iceberg_fuzzer.config.write_pattern import produce_configs as produce_write_pattern
from iceberg_fuzzer.config.partitioned import produce_configs as produce_partitioned


def produce_configs(spec: dict, minimal: bool = False) -> list[dict]:
    """Produce combined table configs from the all-in-one spec YAML."""
    configs = []

    # Simple tables — top-level keys
    for c in produce_simple(spec, minimal=minimal):
        c['_spec_type'] = 'simple'
        configs.append(c)

    # Write-pattern tables — nested under 'write_patterns'
    wp_section = spec.get('write_patterns', {})
    if wp_section:
        wp_spec = {
            'type': 'write_pattern',
            'patterns': wp_section.get('patterns', []),
            'table_config': wp_section.get('table_config', {}),
        }
        for c in produce_write_pattern(wp_spec, minimal=minimal):
            c['_spec_type'] = 'write_pattern'
            configs.append(c)

    # Partitioned tables — nested under 'partitioned_tables'
    pt_section = spec.get('partitioned_tables', {})
    if pt_section:
        pt_spec = {'type': 'partitioned'}
        for c in produce_partitioned(pt_spec, minimal=minimal):
            c['_spec_type'] = 'partitioned'
            configs.append(c)

    return configs
