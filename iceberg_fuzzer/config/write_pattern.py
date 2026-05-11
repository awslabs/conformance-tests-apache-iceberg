"""ConfigProducer for write-pattern tables."""

_DEFAULT_PATTERNS = [
    "insert_heavy", "update_heavy", "delete_heavy", "mixed_chaos",
    "batch_replace", "gradual_growth", "burst_ops", "truncate_cycles",
]
_DEFAULT_VERSIONS = ["v1", "v2"]
_DEFAULT_DELETE_MODES = ["cow", "posdelete"]


def produce_configs(spec: dict, minimal: bool = False) -> list[dict]:
    """Produce table configs from the write_pattern spec YAML.

    Enumerates patterns × versions × delete_modes, skipping invalid combos.
    """
    table_config = spec.get('table_config', {})
    patterns = [p['name'] for p in spec.get('patterns', [])] or _DEFAULT_PATTERNS
    versions = table_config.get('versions', _DEFAULT_VERSIONS)
    delete_modes = table_config.get('delete_modes', _DEFAULT_DELETE_MODES)

    if minimal:
        patterns = patterns[:2]

    tables = []
    for pattern in patterns:
        for version in versions:
            for delete_mode in delete_modes:
                # v1 only supports cow
                if version == "v1" and delete_mode != "cow":
                    continue

                table_name = f"unpart_{pattern}_zstd_plain_dictionary_{version}_{delete_mode}"
                tables.append({
                    'name': table_name,
                    'pattern': pattern,
                    'version': version,
                    'delete_mode': delete_mode,
                })

    return tables
