"""ConfigProducer for partitioned tables."""

_MINIMAL_TRANSFORMS = [
    ["identity(category)"],
    ["bucket[4](id)"],
    ["month(timestamp)"],
    ["identity(category)", "bucket[4](id)"],
]

_FULL_TRANSFORMS = [
    # Single-column transforms (22)
    ["identity(category)"],
    ["bucket[4](id)"],
    ["bucket[8](id)"],
    ["truncate[2](name)"],
    ["truncate[5](name)"],
    ["year(timestamp)"],
    ["month(timestamp)"],
    ["day(timestamp)"],
    ["hour(timestamp)"],
    ["bucket[10](value)"],
    ["date(timestamp)"],
    ["bucket[2](id)"],
    ["bucket[16](id)"],
    ["bucket[32](id)"],
    ["bucket[4](value)"],
    ["bucket[8](value)"],
    ["truncate[1](name)"],
    ["truncate[3](name)"],
    ["truncate[10](name)"],
    ["identity(boolean_col)"],
    ["identity(int_col)"],
    ["identity(date_col)"],
    # Multi-column transforms (14)
    ["identity(category)", "year(timestamp)"],
    ["identity(category)", "bucket[8](id)"],
    ["bucket[4](id)", "day(timestamp)"],
    ["year(timestamp)", "bucket[4](id)"],
    ["identity(category)", "month(timestamp)", "bucket[4](id)"],
    ["hour(timestamp)", "identity(category)"],
    ["day(timestamp)", "bucket[8](id)"],
    ["truncate[2](name)", "year(timestamp)"],
    ["bucket[4](id)", "bucket[8](value)"],
    ["identity(category)", "day(timestamp)", "bucket[4](id)"],
    ["date(timestamp)", "identity(category)"],
    ["truncate[3](name)", "bucket[4](id)"],
    ["month(timestamp)", "bucket[2](id)"],
    # Edge cases (1)
    ["identity(single_value_field)"],
]


def _transform_to_name_part(t: str) -> str:
    """Convert a transform spec string to a name-safe string."""
    if t.startswith('identity('):
        return f"identity_{t[9:-1]}"
    elif t.startswith('bucket['):
        size = t.split('[')[1].split(']')[0]
        col = t.split('(')[1].rstrip(')')
        return f"bucket{size}_{col}"
    elif t.startswith('truncate['):
        width = t.split('[')[1].split(']')[0]
        col = t.split('(')[1].rstrip(')')
        return f"truncate{width}_{col}"
    elif t.startswith('date('):
        return f"date_{t[5:-1]}"
    elif t.startswith('year('):
        return f"year_{t[5:-1]}"
    elif t.startswith('month('):
        return f"month_{t[6:-1]}"
    elif t.startswith('day('):
        return f"day_{t[4:-1]}"
    elif t.startswith('hour('):
        return f"hour_{t[5:-1]}"
    return t.replace('(', '_').replace(')', '').replace('[', '').replace(']', '')


def produce_configs(spec: dict, minimal: bool = False) -> list[dict]:
    """Produce table configs from the partitioned spec YAML.

    Enumerates transforms × write_patterns × delete_modes.
    """
    if minimal:
        partition_transforms = _MINIMAL_TRANSFORMS
        write_patterns = ["mixed_chaos"]
        delete_modes = ["cow", "posdelete"]
    else:
        partition_transforms = _FULL_TRANSFORMS
        write_patterns = ["insert_heavy", "update_heavy", "delete_heavy", "mixed_chaos"]
        delete_modes = ["cow", "posdelete"]

    tables = []
    for transform in partition_transforms:
        transform_name = "_".join(_transform_to_name_part(t) for t in transform)

        for pattern in write_patterns:
            for mode in delete_modes:
                table_name = f"part_{transform_name}_{pattern}_{mode}"
                tables.append({
                    'name': table_name,
                    'transform': transform,
                    'pattern': pattern,
                    'mode': mode,
                })

    return tables
