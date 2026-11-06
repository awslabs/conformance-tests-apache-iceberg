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
"""The partitioned table matrix: which tables exist and what they are named.

`generate` and `validate` have to agree exactly, table name for table name, or
validate reports tables missing that generate never intended to write. Both
derive the set from here.
"""

# Transforms are written in the generator's own syntax, `bucket[N](col)` rather
# than Iceberg's `bucket(N, col)`, because `create_partitioned_table` parses
# this form.
MINIMAL_TRANSFORMS = [
    ["identity(category)"],
    ["bucket[4](id)"],
    ["month(timestamp)"],
    ["identity(category)", "bucket[4](id)"],
]

FULL_TRANSFORMS = [
    # Single-column
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

    # Multi-column, where the order of fields is itself part of the coverage
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
    ["day(timestamp)", "identity(category)"],
    ["truncate[3](name)", "bucket[4](id)"],
    # Also in MINIMAL_TRANSFORMS, so the minimal set stays a subset of this one.
    ["identity(category)", "bucket[4](id)"],
    ["month(timestamp)", "bucket[2](id)"],

    # A column holding one distinct value, so the table has one partition
    ["identity(single_value_field)"],
]

MINIMAL_WRITE_PATTERNS = ["mixed_chaos"]

FULL_WRITE_PATTERNS = ["insert_heavy", "update_heavy", "delete_heavy", "mixed_chaos"]

DELETE_MODES = ["cow", "posdelete"]

# Prefix length of each transform keyword, used to slice the column name out of
# `keyword(col)`.
_SINGLE_ARG_TRANSFORMS = {
    'identity(': len('identity('),
    'year(': len('year('),
    'month(': len('month('),
    'day(': len('day('),
    'hour(': len('hour('),
}

_PARAMETERIZED_TRANSFORMS = ('bucket[', 'truncate[')


def transform_field_name(transform_field: str) -> str:
    """Render one partition field as the fragment that goes in a table name.

    `identity(category)` becomes `identity_category` and `bucket[4](id)`
    becomes `bucket4_id`, so the name stays a legal Iceberg identifier.
    """
    for keyword, prefix_len in _SINGLE_ARG_TRANSFORMS.items():
        if transform_field.startswith(keyword):
            return f"{keyword[:-1]}_{transform_field[prefix_len:-1]}"

    for keyword in _PARAMETERIZED_TRANSFORMS:
        if transform_field.startswith(keyword):
            param = transform_field.split('[')[1].split(']')[0]
            col = transform_field.split('(')[1].rstrip(')')
            return f"{keyword[:-1]}{param}_{col}"

    raise ValueError(f"unrecognized partition transform: {transform_field!r}")


def transform_name(transform: list[str]) -> str:
    """Render a whole partition spec as the transform part of a table name."""
    return "_".join(transform_field_name(field) for field in transform)


def partitioned_table_specs(minimal: bool = False) -> list[dict]:
    """Enumerate the partitioned tables, in a stable order.

    Each entry carries the name, the transform it was built from, the write
    pattern, and the delete mode.
    """
    transforms = MINIMAL_TRANSFORMS if minimal else FULL_TRANSFORMS
    write_patterns = MINIMAL_WRITE_PATTERNS if minimal else FULL_WRITE_PATTERNS

    specs = []
    for transform in transforms:
        name = transform_name(transform)
        for pattern in write_patterns:
            for delete_mode in DELETE_MODES:
                specs.append({
                    'name': f"part_{name}_{pattern}_{delete_mode}",
                    'transform': transform,
                    'pattern': pattern,
                    'delete_mode': delete_mode,
                })
    return specs
