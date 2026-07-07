"""Data types dimension — typed value filling.

INT-ONLY scope for this first change, but structured so adding types is a
one-line change rather than editing parallel switches:

- Each primitive type is one ``IcebergType`` handler bundling its Arrow type,
  Spark DDL, and value generation, registered by name in ``REGISTRY``. Adding
  a type = one new handler + one REGISTRY entry (no if/elif switch to grow).
- Field-id assignment is separated from schema building via a seam:
  ``get_arrow_schema`` receives a ``FieldIdMap`` (path -> id) and only stamps
  it; the *caller* decides where the ids come from (computed here via
  ``compute_field_ids``, or harvested from a created Iceberg table). The
  provider never assigns ids.

Additional types (long, string, complex list/map/struct, ...) and the
breadth-first ``compute_field_ids`` for nested types land in follow-up changes.
"""

import copy
import random
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Tuple

import pyarrow as pa

from . import Dimension


# ---------------------------------------------------------------------------
# Type registry — one handler per primitive type
# ---------------------------------------------------------------------------

class IcebergType:
    """One Iceberg primitive type: its Arrow type, Spark DDL, and value gen.

    Subclass per type and register in REGISTRY. Everything a type needs lives
    on its handler, so adding a type can't half-happen (you can't add the value
    logic but forget the Arrow type — they're on the same object).
    """
    arrow_type: pa.DataType
    spark_ddl: str

    def generate(self, row_id: int) -> Any:
        raise NotImplementedError


class IntType(IcebergType):
    arrow_type = pa.int32()
    spark_ddl = "INT"

    def generate(self, row_id: int) -> Any:
        return row_id


# name -> handler. Adding a primitive type is a new IcebergType subclass plus
# one entry here — there is no if/elif switch anywhere.
REGISTRY: Dict[str, IcebergType] = {
    "int": IntType(),
    "integer": IntType(),
}


def _handler(iceberg_type: str) -> IcebergType:
    """Look up the handler for a type string, or raise if unsupported."""
    t = iceberg_type.strip().lower()
    try:
        return REGISTRY[t]
    except KeyError:
        raise ValueError(
            f"Unsupported data type {iceberg_type!r}: supported types are "
            f"{sorted(set(REGISTRY))} (more land in follow-up changes)")


# ---------------------------------------------------------------------------
# Field-id seam — id assignment is separate from schema building
# ---------------------------------------------------------------------------

FieldPath = Tuple[str, ...]
"""Path of field names from a root column to a (possibly nested) node. For a
top-level column it is a 1-tuple, e.g. ('value',); nested types extend it,
e.g. ('r', 'a') for struct field a, ('xs', 'element') for a list element."""

FieldIdMap = Dict[FieldPath, int]
"""Maps each field path to its Iceberg field id. Produced by the caller
(computed via compute_field_ids, or harvested from a created Iceberg table) and
handed to get_arrow_schema, which only STAMPS the ids — it never assigns them.
This is the seam that lets the builder swap id sources without touching the
provider."""


def compute_field_ids(columns: List[Tuple[str, str]]) -> FieldIdMap:
    """Assign Iceberg field ids to a schema's fields (the 'compute' strategy).

    INT-only / single-column: the one column gets id 1. Breadth-first
    assignment for nested types (list/map/struct) lands with those types.
    """
    col_name, _col_type = columns[0]
    return {(col_name,): 1}


def generate_value(iceberg_type: str, row_id: int) -> Any:
    """Generate a deterministic value for a type and row id, via its handler."""
    return _handler(iceberg_type).generate(row_id)


@dataclass
class DataTypeChoice:
    """Schema definition: a single (column_name, type_string) column.

    Implements the SchemaProvider protocol, providing:
    - Spark DDL for CREATE TABLE
    - PyArrow schema with PARQUET:field_id metadata (via the field-id seam)
    - Type-appropriate value generation for the column

    Single-column only, enforced at construction: a table is always exactly one
    column (which, in later changes, may be a composite type). value_strategy is
    per-row, so a single column keeps its meaning unambiguous.

    Args:
        columns: List holding exactly one (column_name, iceberg_type) tuple.
        nullable: If True, the column is nullable (DDL omits NOT NULL, Arrow
                  field has nullable=True). Default: False.
    """
    columns: List[Tuple[str, str]]
    nullable: bool = False

    def __post_init__(self):
        if len(self.columns) != 1:
            raise ValueError(
                f"DataTypeChoice must have exactly one column (single-column "
                f"tables only); got {len(self.columns)}: "
                f"{[c[0] for c in self.columns]}")
        _name, col_type = self.columns[0]
        _handler(col_type)  # raises if the type is unsupported

    # -- SchemaProvider protocol -------------------------------------------

    def get_spark_ddl(self) -> str:
        """Return Spark SQL DDL for the CREATE TABLE column definition."""
        col_name, col_type = self.columns[0]
        ddl = f"{col_name} {_handler(col_type).spark_ddl}"
        return ddl if self.nullable else f"{ddl} NOT NULL"

    def get_field_ids(self) -> FieldIdMap:
        """Return this schema's computed field-id map (the 'compute' strategy).

        Convenience for callers that want the computed ids. Callers may ignore
        it and supply their own map (e.g. harvested from a created Iceberg
        table) to get_arrow_schema — that substitution is the seam.
        """
        return compute_field_ids(self.columns)

    def get_arrow_schema(self, field_ids: FieldIdMap) -> pa.Schema:
        """Return a PyArrow schema, stamping the supplied field ids.

        The provider does NOT assign field ids — it stamps whatever the caller
        supplies (keyed by field path). The caller obtains the map from
        get_field_ids (compute) or by harvesting a created Iceberg table. See
        the FieldIdMap seam docs above.
        """
        col_name, col_type = self.columns[0]
        return pa.schema([self._to_field((col_name,), col_type,
                                         self.nullable, field_ids)])

    def _to_field(self, path: FieldPath, col_type: str, nullable: bool,
                  field_ids: FieldIdMap) -> pa.Field:
        """Build one PyArrow field, stamping its field id from field_ids[path].

        Primitive-only for now; complex types recurse into child paths here.
        """
        fid = field_ids[path]  # KeyError if the caller under-supplied ids
        arrow_type = _handler(col_type).arrow_type
        return (pa.field(path[-1], arrow_type, nullable=nullable)
                .with_metadata({"PARQUET:field_id": str(fid)}))

    def generate_value(self, column_name: str, row_id: int) -> Any:
        """Generate a typed value for a given column and row."""
        for col_name, col_type in self.columns:
            if col_name == column_name:
                return generate_value(col_type, row_id)
        raise ValueError(
            f"Column {column_name!r} not found in schema. "
            f"Available: {[c[0] for c in self.columns]}")


class TypedValueFiller:
    """Fills a config's rows with concrete typed values per its value_strategy.

    Reads value_strategy/null_rows from the shape config, calls
    DataTypeChoice.generate_value for the (single) column, and stamps
    row['typed_value'] on every row. This is the glue between the shape's
    value strategy and the chosen data type.
    """

    def fill(self, config: Dict[str, Any], type_choice: DataTypeChoice) -> Dict[str, Any]:
        """Return a copy of config with row['typed_value'] filled per strategy."""
        value_strategy = config.get("value_strategy", "row_numbers")

        value_map: Dict[int, Any] = {}

        if value_strategy == "row_numbers":
            for data_file in config.get("data_files", []):
                for row in data_file.get("rows", []):
                    row_id = row["row_id"]
                    value_map[row_id] = self._generate_typed_value(row_id, type_choice)

        elif value_strategy == "nulls":
            # A row is null iff the shape marked it so with value=None. This is
            # the authoritative signal, set on every null row by the generator.
            # We read it directly rather than the config's 'null_rows' index:
            # value=None is always present, whereas null_rows is redundant
            # bookkeeping that a code path could omit — so trusting value=None
            # can't drift from what the shape actually wrote.
            for data_file in config.get("data_files", []):
                for row in data_file.get("rows", []):
                    row_id = row["row_id"]
                    if row.get("value") is None:
                        value_map[row_id] = None
                    else:
                        value_map[row_id] = self._generate_typed_value(row_id, type_choice)

        elif value_strategy == "repetition":
            # Rows sharing an abstract 'value' get the same typed value.
            unique_values: Dict[Any, Any] = {}
            for data_file in config.get("data_files", []):
                for row in data_file.get("rows", []):
                    abstract_value = row.get("value")
                    if abstract_value not in unique_values:
                        unique_values[abstract_value] = self._generate_typed_value(
                            len(unique_values), type_choice)
            for data_file in config.get("data_files", []):
                for row in data_file.get("rows", []):
                    value_map[row["row_id"]] = unique_values[row.get("value")]

        result = copy.deepcopy(config)
        for data_file in result.get("data_files", []):
            for row in data_file.get("rows", []):
                row["typed_value"] = value_map.get(row["row_id"])
        result["schema"] = type_choice.columns
        return result

    def _generate_typed_value(self, seed: int, type_choice: DataTypeChoice) -> Any:
        _col_name, col_type = type_choice.columns[0]
        return generate_value(col_type, seed)


class DataTypeDimension(Dimension):
    """Enumerates/samples single-column data-type schemas (int only for now)."""

    SUPPORTED_TYPES = ["int"]

    def enumerate(self) -> Iterator[DataTypeChoice]:
        """Yield the int schema in both non-nullable and nullable variants.

        The nullable variant lets the composer pair the 'nulls' value_strategy
        with a nullable type.
        """
        for type_str in self.SUPPORTED_TYPES:
            yield DataTypeChoice(columns=[("value", type_str)], nullable=False)
            yield DataTypeChoice(columns=[("value", type_str)], nullable=True)

    def sample(self, rng: random.Random) -> DataTypeChoice:
        """Sample one single-column int schema with random nullability."""
        type_str = rng.choice(self.SUPPORTED_TYPES)
        return DataTypeChoice(columns=[("value", type_str)],
                              nullable=rng.choice([False, True]))
