"""ConfigProducer for simple (single-type) tables."""


def produce_configs(spec: dict, minimal: bool = False) -> list[dict]:
    """Produce table configs from the simple spec YAML.

    Computes the cartesian product of types × compressions × encodings × versions.
    """
    all_types = list(spec.get('primitive_types', []))

    if minimal:
        all_types = [
            t for t in all_types
            if not ('time' in t.get('name', '') and t.get('name') not in ['timestamp', 'timestamptz'])
        ]
        all_types = [t for t in all_types if t.get('name') not in ['uuid', 'fixed_16']]

    compression_configs = []
    if minimal:
        compression_configs = [{'codec': 'none', 'level': None}]
    else:
        for comp_config in spec.get('compression_configs', []):
            codec = comp_config['codec']
            levels = comp_config.get('levels', [None])
            for level in levels:
                compression_configs.append({'codec': codec, 'level': level})

    encoding_types = ['PLAIN'] if minimal else spec.get('encoding_types', ['PLAIN'])

    tables = []
    for data_type in all_types:
        for compression in compression_configs:
            for encoding in encoding_types:
                for version in spec.get('format_versions', [1, 2]):
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
                        'properties': {'format-version': str(version)},
                    })

    return tables
