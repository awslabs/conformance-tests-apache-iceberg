"""Data types dimension — typed value filling.

Supports primitive Iceberg types (int, long, string, decimal(P,S), ...) and
complex types (list, map, struct) with arbitrary nesting. The design goal is
that adding a new primitive is localized to the registry — a new handler plus
one REGISTRY entry — with no if/elif switch to update anywhere else:

- Each primitive type is one ``IcebergType`` handler bundling its Arrow type,
  Spark DDL, and value generation, registered by name in ``REGISTRY`` (with
  regex-parsed handlers for the parametrized ``decimal(P,S)`` / ``fixed(L)``).
  Adding a primitive = one new handler + one REGISTRY entry (no if/elif switch).
- Complex types are NOT registry entries — they compose other types. A single
  recursive resolver (``parse_type`` + the ``_child_specs`` walk) handles
  list/map/struct and delegates to the registry at the leaves. It has a fixed
  ~3 branches and never grows as primitives are added.
- Field-id assignment is separated from schema building via a seam:
  ``get_arrow_schema`` receives a ``FieldIdMap`` (path -> id) and only stamps
  it; the *caller* decides where the ids come from (computed here via
  ``compute_field_ids``, or harvested from a created Iceberg table). Nested ids
  are assigned breadth-first, matching Iceberg's own assignment order.
"""

import copy
import random
import re
import struct
from collections import deque
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, Iterator, List, Tuple, Union

import pyarrow as pa

from . import Dimension


# ---------------------------------------------------------------------------
# Module constants
# ---------------------------------------------------------------------------

# Base date/datetime that date/timestamp/timestamptz values advance FROM. The
# specific date is arbitrary — values only need to be deterministic and
# injective per row_id, not anchored to any real-world instant — but a fixed,
# recent, unambiguous date (no leap-day / pre-epoch / DST edge) keeps generated
# values easy to eyeball and stable across runs.
_EPOCH_DATE = date(2023, 1, 1)
_EPOCH_DATETIME = datetime(2023, 1, 1)

# Offset applied to a row id before advancing a date/time from the epoch. Row
# ids can be huge and negative (sentinel padding uses -(i + 1_000_000)), and
# date(1,1,1)..date(9999,12,31) is only ~3.65M days wide — a raw offset
# overflows. Bounding into [0, _DATE_SPAN) keeps every generated value a valid
# date while staying injective across the realistic row-id range.
_DATE_SPAN = 3_000_000  # days, comfortably inside date32's supported range


def _bounded(row_id: int, span: int) -> int:
    """Map any int (incl. large negatives) into [0, span) injectively within a
    contiguous run — Python's % always returns a non-negative result here."""
    return row_id % span


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


class BooleanType(IcebergType):
    arrow_type = pa.bool_()
    spark_ddl = "BOOLEAN"

    def generate(self, row_id: int) -> Any:
        return (row_id % 2) == 0


class IntType(IcebergType):
    arrow_type = pa.int32()
    spark_ddl = "INT"

    def generate(self, row_id: int) -> Any:
        return row_id


class LongType(IcebergType):
    arrow_type = pa.int64()
    spark_ddl = "BIGINT"

    def generate(self, row_id: int) -> Any:
        return row_id * 1000


class FloatType(IcebergType):
    arrow_type = pa.float32()
    spark_ddl = "FLOAT"

    def generate(self, row_id: int) -> Any:
        # A fraction with ~15 significant digits: float64 holds it, float32 must
        # round it away. Written to this float32 column it is stored as the
        # narrowed value, so a reader/writer that mishandles 32-bit precision is
        # actually exercised (the old row_id+0.5 was exact in both widths and
        # tested nothing about precision). Validation narrows the expected value
        # to float32 before comparing — see the precision-aware read-back.
        return float(row_id) + 0.123456789012345


class DoubleType(IcebergType):
    arrow_type = pa.float64()
    spark_ddl = "DOUBLE"

    def generate(self, row_id: int) -> Any:
        # Same idea, distinct constant so a float column and a double column
        # hold different values for the same row. The extra digits survive in
        # float64 but would be lost in float32 — the basis for asserting the two
        # widths store different physical values.
        return float(row_id) + 0.987654321098765


class StringType(IcebergType):
    arrow_type = pa.string()
    spark_ddl = "STRING"

    def generate(self, row_id: int) -> Any:
        return f"value_{row_id}"


class BinaryType(IcebergType):
    arrow_type = pa.binary()
    spark_ddl = "BINARY"

    def generate(self, row_id: int) -> Any:
        return f"value_{row_id}".encode()


class DateType(IcebergType):
    arrow_type = pa.date32()
    spark_ddl = "DATE"

    def generate(self, row_id: int) -> Any:
        # Advance a bounded number of days from the epoch: always a valid date
        # (no overflow for extreme/negative padding ids) and injective over the
        # realistic id range.
        return _EPOCH_DATE + timedelta(days=_bounded(row_id, _DATE_SPAN))


class TimestampType(IcebergType):
    # Iceberg 'timestamp' is without zone → Spark TIMESTAMP_NTZ.
    arrow_type = pa.timestamp('us')
    spark_ddl = "TIMESTAMP_NTZ"

    def generate(self, row_id: int) -> Any:
        # Advance seconds (wide calendar spread) PLUS a microsecond component so
        # the sub-second field varies — the Arrow type is 'us' granularity, and
        # whole-second values would never exercise microsecond round-tripping.
        # Injective over the realistic id range: the seconds term alone is
        # injective on [0, _DATE_SPAN), so the (seconds, micros) pair is too.
        return _EPOCH_DATETIME + timedelta(
            seconds=_bounded(row_id, _DATE_SPAN),
            microseconds=_bounded(row_id, 1_000_000))


class TimestamptzType(IcebergType):
    # Iceberg 'timestamptz' is with zone → Spark TIMESTAMP.
    arrow_type = pa.timestamp('us', tz='UTC')
    spark_ddl = "TIMESTAMP"

    def generate(self, row_id: int) -> Any:
        # Same seconds + microseconds spread as TimestampType (see there), with
        # a UTC zone since Iceberg timestamptz is with-zone.
        return (_EPOCH_DATETIME.replace(tzinfo=timezone.utc)
                + timedelta(seconds=_bounded(row_id, _DATE_SPAN),
                            microseconds=_bounded(row_id, 1_000_000)))


class DecimalType(IcebergType):
    """Parametrized decimal(P, S). Not a fixed REGISTRY entry — built by
    _handler from a parsed 'decimal(p,s)' string (or the bare 'decimal')."""

    # Iceberg decimal(P, S) requires 1 <= P <= 38 and 0 <= S <= P. Validate here
    # rather than letting a bad P/S through: pa.decimal128 accepts S > P happily
    # and generate() even returns plausible-looking values, so the mistake only
    # surfaces much later as an opaque ArrowInvalid at array-construction time
    # ("Decimal type with precision 5 does not fit into precision inferred from
    # first array element: 2"). Failing at handler lookup names the real problem.
    MAX_PRECISION = 38

    def __init__(self, precision: int = 10, scale: int = 2):
        if not 1 <= precision <= self.MAX_PRECISION:
            raise ValueError(
                f"decimal precision must be between 1 and {self.MAX_PRECISION}, "
                f"got {precision}")
        if not 0 <= scale <= precision:
            raise ValueError(
                f"decimal scale must be between 0 and the precision ({precision}), "
                f"got {scale}")
        self.precision = precision
        self.scale = scale
        self.arrow_type = pa.decimal128(precision, scale)
        self.spark_ddl = f"DECIMAL({precision},{scale})"

    def generate(self, row_id: int) -> Any:
        # Keep the value within the declared decimal(P, S) by construction:
        #   - fractional part capped to `scale` digits (% 10**scale),
        #   - integer part capped to the remaining (precision - scale) digits,
        # so no row_id can overflow precision or scale for any P/S.
        int_digits = self.precision - self.scale
        int_part = row_id % (10 ** int_digits) if int_digits > 0 else 0
        if self.scale:
            frac = row_id % (10 ** self.scale)
            return Decimal(f"{int_part}.{frac:0{self.scale}d}")
        return Decimal(str(int_part))


class FixedType(IcebergType):
    """Parametrized fixed(L) → fixed-length binary. Built by _handler from a
    parsed 'fixed(l)' string."""

    def __init__(self, length: int = 16):
        # Iceberg fixed(L) requires L >= 1. pa.binary(0) is a legal Arrow type, so
        # fixed(0) would otherwise build a zero-width column that no Iceberg
        # schema can express.
        if length < 1:
            raise ValueError(f"fixed length must be >= 1, got {length}")
        self.length = length
        self.arrow_type = pa.binary(length)   # fixed_size_binary[length] on disk
        # Spark DDL has no fixed-length-binary type: FIXED(L)/BINARY(L) both fail
        # with UNSUPPORTED_DATATYPE (verified), only bare BINARY works. So the
        # Spark table column is plain `binary` while the Parquet file is truly
        # fixed_size_binary[L]. Iceberg tolerates this and reads back fine, but
        # at the Spark/DDL level fixed(L) is indistinguishable from binary.
        # TODO(fidelity): a true fixed(L) column needs the PyIceberg/Java
        # table-creation path (Spark DDL cannot express the length).
        self.spark_ddl = "BINARY"

    def generate(self, row_id: int) -> Any:
        # Exactly `length` bytes (pa.binary(length) requires the exact width).
        return bytes([(row_id + i) % 256 for i in range(self.length)])


# Deliberately absent: uuid. Iceberg's native uuid is a 16-byte fixed value, but
# tables here are created with Spark SQL DDL and Spark rejects a uuid column
# outright — `CREATE TABLE (value UUID)` -> [UNSUPPORTED_DATATYPE] Unsupported
# data type "UUID" (verified on Spark 3.5.3/Iceberg 1.7.1 and 4.0.0/Iceberg
# 1.11.0). Only binary/string DDL are accepted, so uuid could only be FAKED as a
# STRING of uuid text — indistinguishable from a plain string at both the Iceberg
# and Parquet layers, i.e. reporting coverage the fuzzer doesn't have. Omitted
# from the registry so _handler rejects it instead. (fixed(L) is different, and
# stays supported: its Parquet column really is fixed_size_binary[L] — a real
# physical type behind a lossy DDL, not a fabrication.)
#
# TODO(fidelity): native uuid needs table creation via the PyIceberg/Java schema
# API instead of Spark DDL, then writing fixed_size_binary(16) = uuid.bytes. That
# path would also unblock native fixed(L) and required list-elements/map-values.
#
# name -> handler for the fixed (non-parametrized) primitives. Adding one is a
# new IcebergType subclass plus one entry here — there is no if/elif switch.
REGISTRY: Dict[str, IcebergType] = {
    "boolean": BooleanType(),
    "bool": BooleanType(),
    "int": IntType(),
    "integer": IntType(),
    "long": LongType(),
    "bigint": LongType(),
    "float": FloatType(),
    "double": DoubleType(),
    "string": StringType(),
    "binary": BinaryType(),
    "date": DateType(),
    "timestamp": TimestampType(),
    "timestamp_ntz": TimestampType(),
    "timestamptz": TimestamptzType(),
}

_DECIMAL_RE = re.compile(r"decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)")
_FIXED_RE = re.compile(r"fixed\(\s*(\d+)\s*\)")


def _handler(iceberg_type: str) -> IcebergType:
    """Look up (or build) the primitive handler for a type string, or raise.

    Two lookup mechanisms, by necessity:
    - Non-parametrized types are pre-built singletons in REGISTRY, matched by
      exact name (one IntType() serves every 'int' column).
    - Parametrized types (decimal(P,S), fixed(L)) can't be registry entries:
      each distinct (P,S)/(L) needs its OWN handler instance (different
      arrow_type and DDL), the parameter space is unbounded (no finite set of
      keys to pre-populate), and a dict can't match a pattern and extract
      groups. So they're parsed out of the string and CONSTRUCTED on demand
      here. The bare 'decimal'/'fixed' forms build a default-parameter instance.

    The bare-name cases could instead live in REGISTRY (mapping 'decimal' ->
    DecimalType()), but the regex branches would remain for the parametrized
    forms regardless — so keeping both cases here keeps all decimal/fixed
    handling in one place rather than splitting it across REGISTRY and _handler.
    """
    t = iceberg_type.strip().lower()
    if t in REGISTRY:
        return REGISTRY[t]
    m = _DECIMAL_RE.fullmatch(t)
    if m:
        return DecimalType(int(m.group(1)), int(m.group(2)))
    if t == "decimal":
        return DecimalType()
    m = _FIXED_RE.fullmatch(t)
    if m:
        return FixedType(int(m.group(1)))
    raise ValueError(
        f"Unsupported data type {iceberg_type!r}: supported primitives are "
        f"{sorted(set(REGISTRY))} plus decimal(P,S) / fixed(L), and complex "
        f"types list<..>/map<..>/struct<..>")


# ---------------------------------------------------------------------------
# Complex type parsing — one recursive resolver, ~3 branches, never grows
# ---------------------------------------------------------------------------

ParsedType = Union[str, Dict[str, Any]]
"""A parsed type is either a primitive string ('int', 'decimal(10,2)', ...) or
a dict describing a complex type:
  - {'kind': 'list', 'element': ParsedType}
  - {'kind': 'map', 'key': ParsedType, 'value': ParsedType}
  - {'kind': 'struct', 'fields': List[Tuple[str, ParsedType, bool]]}

Each struct field is (name, type, required). `required` comes from a trailing
`NOT NULL` marker on the field's type (e.g. 'struct<a:int NOT NULL, b:string>'):
Spark DDL accepts NOT NULL inside STRUCT<...>, and Iceberg records the field as
required. List elements and map values are always optional here — Spark DDL
CANNOT express a required list element / map value (ARRAY<INT NOT NULL> and
MAP<K, V NOT NULL> are parse errors), so those require the JVM-schema path.
"""


def parse_type(type_str: str) -> ParsedType:
    """Parse a type string into a primitive string or a complex-type dict.

    Recurses for nesting: 'struct<a: list<int>, b: map<string,int>>' works.
    """
    s = type_str.strip()
    low = s.lower()

    if low.startswith("list<") and s.endswith(">"):
        return {"kind": "list", "element": parse_type(s[5:-1])}
    if low.startswith("map<") and s.endswith(">"):
        key_str, value_str = _split_top_level_comma(s[4:-1], max_splits=1)
        return {"kind": "map",
                "key": parse_type(key_str), "value": parse_type(value_str)}
    if low.startswith("struct<") and s.endswith(">"):
        return {"kind": "struct", "fields": _parse_struct_fields(s[7:-1])}
    # Primitive — keep as-is (handler lookup normalizes case).
    return s


def _split_top_level_comma(s: str, max_splits: int = -1) -> List[str]:
    """Split on commas that are NOT inside angle brackets (so nested types stay
    intact). max_splits caps the number of splits (-1 = unlimited)."""
    parts: List[str] = []
    depth = 0
    current: List[str] = []
    splits_done = 0
    for ch in s:
        if ch == "<":
            depth += 1
            current.append(ch)
        elif ch == ">":
            depth -= 1
            current.append(ch)
        elif ch == "," and depth == 0 and (max_splits == -1 or splits_done < max_splits):
            parts.append("".join(current).strip())
            current = []
            splits_done += 1
        else:
            current.append(ch)
    parts.append("".join(current).strip())
    return parts


def _parse_struct_fields(inner: str) -> List[Tuple[str, ParsedType, bool]]:
    """Parse 'a: int NOT NULL, b: string' into
    [('a', 'int', True), ('b', 'string', False)], respecting angle-bracket depth
    for nested field types.

    A trailing (case-insensitive) 'NOT NULL' on a field's type marks it required.
    It is stripped before the type is parsed, so it composes with nested types:
    'struct<a: list<int> NOT NULL>' -> field a required, element still optional.
    """
    fields: List[Tuple[str, ParsedType, bool]] = []
    for field_str in _split_top_level_comma(inner):
        field_str = field_str.strip()
        if not field_str:
            continue
        colon_idx = field_str.index(":")
        name = field_str[:colon_idx].strip()
        type_part = field_str[colon_idx + 1:].strip()
        required = False
        if type_part.upper().endswith("NOT NULL"):
            required = True
            type_part = type_part[:-len("NOT NULL")].strip()
        fields.append((name, parse_type(type_part), required))
    return fields


def _is_complex(parsed: ParsedType) -> bool:
    return isinstance(parsed, dict)


def _child_specs(parsed: ParsedType) -> List[Tuple[str, ParsedType, bool]]:
    """Direct children of a parsed type as (name, parsed_type, nullable).

    Nullability: map keys are always required; list elements and map values are
    always optional (Spark DDL can't express them otherwise). Struct fields are
    optional by default but required when marked NOT NULL in the type string
    (see _parse_struct_fields). Primitives have no children. This is the single
    source of truth for both field-id assignment and Arrow-field nullability.
    """
    if not _is_complex(parsed):
        return []
    kind = parsed["kind"]
    if kind == "list":
        return [("element", parsed["element"], True)]
    if kind == "map":
        return [("key", parsed["key"], False), ("value", parsed["value"], True)]
    if kind == "struct":
        # A field is nullable iff it is NOT marked required (NOT NULL).
        return [(name, ftype, not required)
                for name, ftype, required in parsed["fields"]]
    return []


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
    """Assign Iceberg field ids breadth-first (the 'compute' strategy).

    Roots (top-level columns) get ids 1..N first, then each level of nested
    children is numbered in order — exactly how Iceberg assigns ids when a table
    is created. For a nested type the child order per node is:
      list  -> [element]; map -> [key, value]; struct -> [field_0, field_1, ...]
    """
    field_ids: FieldIdMap = {}
    queue: "deque[Tuple[FieldPath, ParsedType]]" = deque()
    next_id = 1

    for col_name, col_type in columns:
        path: FieldPath = (col_name,)
        field_ids[path] = next_id
        next_id += 1
        queue.append((path, parse_type(col_type)))

    while queue:
        path, parsed = queue.popleft()
        for child_name, child_parsed, _nullable in _child_specs(parsed):
            child_path = path + (child_name,)
            field_ids[child_path] = next_id
            next_id += 1
            queue.append((child_path, child_parsed))

    return field_ids


def generate_value(iceberg_type: str, row_id: int) -> Any:
    """Generate a deterministic value for a type and row id.

    Primitives delegate to their handler; complex types recurse, seeding each
    child from the parent row_id so values stay deterministic per row.
    """
    return _generate(parse_type(iceberg_type), row_id)


def check_required_fields(iceberg_type: str, value: Any) -> None:
    """Raise ValueError if a PRESENT value violates a required (NOT NULL) field.

    Guards the invariant that a required struct field is never null when its
    struct is present. A wholly-null value (value is None) is allowed — that is
    the top-level column being null, which is governed separately by the shape's
    nullable flag and does not implicate inner required fields. Iceberg rejects a
    null-in-required-field write with an opaque commit error; this fails early
    and clearly instead. Recurses so nesting (struct-in-list, etc.) is covered.
    """
    _check_required(parse_type(iceberg_type), value, path="value")


def _check_required(parsed: ParsedType, value: Any, path: str) -> None:
    if value is None or not _is_complex(parsed):
        return  # absent value or primitive leaf — nothing to enforce here
    kind = parsed["kind"]
    if kind == "list":
        for i, v in enumerate(value):
            _check_required(parsed["element"], v, f"{path}[{i}]")
    elif kind == "map":
        for k, v in value.items():
            _check_required(parsed["value"], v, f"{path}[{k!r}]")
    elif kind == "struct":
        for name, ftype, required in parsed["fields"]:
            fval = value.get(name) if isinstance(value, dict) else None
            if required and fval is None:
                raise ValueError(
                    f"Required field {path}.{name} is null in a present struct "
                    f"(schema marks it NOT NULL); Iceberg would reject this write.")
            _check_required(ftype, fval, f"{path}.{name}")


def _generate(parsed: ParsedType, row_id: int) -> Any:
    if not _is_complex(parsed):
        return _handler(parsed).generate(row_id)
    kind = parsed["kind"]
    if kind == "list":
        num_elements = (row_id % 3) + 1
        return [_generate(parsed["element"], row_id * 10 + i)
                for i in range(num_elements)]
    if kind == "map":
        num_entries = (row_id % 2) + 1
        result = {}
        for i in range(num_entries):
            k = _generate(parsed["key"], row_id * 10 + i)
            v = _generate(parsed["value"], row_id * 10 + i + 100)
            result[k] = v
        return result
    if kind == "struct":
        # required flag doesn't affect value generation — generate() is
        # deterministic and never returns None, so required fields are satisfied.
        return {name: _generate(ftype, row_id * 10 + idx)
                for idx, (name, ftype, _required) in enumerate(parsed["fields"])}
    return None


def physical_value(iceberg_type: str, value: Any) -> Any:
    """Narrow a generated (float64) value to the type's physical storage width.

    generate_value returns Python floats (float64). A `float` column physically
    stores float32, so a correct read-back is the float32-narrowed value, not
    the original float64. Comparing a read-back against the raw generated value
    would spuriously fail for float columns; compare against physical_value(...)
    instead. Recurses into complex types so list<float>/struct<...:float> narrow
    element-wise. Non-float types (and None) pass through unchanged.
    """
    return _physical(parse_type(iceberg_type), value)


def _physical(parsed: ParsedType, value: Any) -> Any:
    if value is None:
        return None
    if not _is_complex(parsed):
        # Only float32 needs narrowing; everything else round-trips exactly.
        if _handler(parsed).arrow_type == pa.float32():
            return float(struct.unpack("f", struct.pack("f", value))[0])
        return value
    kind = parsed["kind"]
    if kind == "list":
        return [_physical(parsed["element"], v) for v in value]
    if kind == "map":
        return {k: _physical(parsed["value"], v) for k, v in value.items()}
    if kind == "struct":
        return {name: _physical(ftype, value[name])
                for name, ftype, _required in parsed["fields"]}
    return value


@dataclass
class DataTypeChoice:
    """Schema definition: a single (column_name, type_string) column.

    Implements the SchemaProvider protocol, providing:
    - Spark DDL for CREATE TABLE
    - PyArrow schema with PARQUET:field_id metadata (via the field-id seam)
    - Type-appropriate value generation for the column

    Single-column only, enforced at construction: a table is always exactly one
    column (which may be a composite type — list/map/struct). value_strategy is
    per-row, so a single column keeps its meaning unambiguous.

    Args:
        columns: List holding exactly one (column_name, iceberg_type) tuple.
        nullable: If True, the (top-level) column is nullable (DDL omits NOT
                  NULL, Arrow field has nullable=True). This flag applies ONLY to
                  the top-level column — it does NOT propagate to nested fields.
                  Nested-field nullability is independent: map keys are always
                  required, list elements / map values always optional, and
                  struct fields optional unless marked NOT NULL in the type
                  string (e.g. 'struct<a:int NOT NULL,b:string>'). See
                  _child_specs. Default: False.
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
        _validate_type(parse_type(col_type))  # raises if any leaf is unsupported

    # -- SchemaProvider protocol -------------------------------------------

    def get_spark_ddl(self) -> str:
        """Return Spark SQL DDL for the CREATE TABLE column definition."""
        col_name, col_type = self.columns[0]
        ddl = f"{col_name} {_parsed_to_spark_ddl(parse_type(col_type))}"
        return ddl if self.nullable else f"{ddl} NOT NULL"

    def get_field_ids(self) -> FieldIdMap:
        """Return this schema's computed field-id map (the 'compute' strategy).

        Callers may ignore it and supply their own map (e.g. harvested from a
        created Iceberg table) to get_arrow_schema — that substitution is the
        seam.
        """
        return compute_field_ids(self.columns)

    def get_arrow_schema(self, field_ids: FieldIdMap) -> pa.Schema:
        """Return a PyArrow schema, stamping the supplied field ids.

        The provider does NOT assign field ids — it stamps whatever the caller
        supplies (keyed by field path), recursing into nested children.
        """
        col_name, col_type = self.columns[0]
        return pa.schema([self._to_field((col_name,), parse_type(col_type),
                                         self.nullable, field_ids)])

    def _to_field(self, path: FieldPath, parsed: ParsedType, nullable: bool,
                  field_ids: FieldIdMap) -> pa.Field:
        """Build one PyArrow field (recursively for complex types), stamping its
        field id from field_ids[path]. KeyError if the caller under-supplied."""
        fid = field_ids[path]
        arrow_type = self._arrow_type(path, parsed, field_ids)
        return (pa.field(path[-1], arrow_type, nullable=nullable)
                .with_metadata({"PARQUET:field_id": str(fid)}))

    def _arrow_type(self, path: FieldPath, parsed: ParsedType,
                    field_ids: FieldIdMap) -> pa.DataType:
        """PyArrow type for a parsed node; recurses via _child_specs so nested
        nullability stays defined in exactly one place."""
        if not _is_complex(parsed):
            return _handler(parsed).arrow_type

        children = _child_specs(parsed)
        child_fields = {
            name: self._to_field(path + (name,), cparsed, cnullable, field_ids)
            for name, cparsed, cnullable in children
        }
        kind = parsed["kind"]
        if kind == "list":
            return pa.list_(child_fields["element"])
        if kind == "map":
            return pa.map_(child_fields["key"], child_fields["value"])
        if kind == "struct":
            return pa.struct([child_fields[name] for name, _, _ in children])
        raise ValueError(f"Unknown complex kind: {kind!r}")

    def generate_value(self, column_name: str, row_id: int) -> Any:
        """Generate a typed value for a given column and row."""
        for col_name, col_type in self.columns:
            if col_name == column_name:
                return generate_value(col_type, row_id)
        raise ValueError(
            f"Column {column_name!r} not found in schema. "
            f"Available: {[c[0] for c in self.columns]}")


def _validate_type(parsed: ParsedType) -> None:
    """Recursively validate that every leaf resolves to a supported primitive."""
    if not _is_complex(parsed):
        _handler(parsed)  # raises if unsupported
        return
    for _name, child, _nullable in _child_specs(parsed):
        _validate_type(child)


def _parsed_to_spark_ddl(parsed: ParsedType) -> str:
    """Recursive Spark DDL for a parsed type (ARRAY<..>/MAP<..>/STRUCT<..>)."""
    if not _is_complex(parsed):
        return _handler(parsed).spark_ddl
    kind = parsed["kind"]
    if kind == "list":
        return f"ARRAY<{_parsed_to_spark_ddl(parsed['element'])}>"
    if kind == "map":
        return (f"MAP<{_parsed_to_spark_ddl(parsed['key'])}, "
                f"{_parsed_to_spark_ddl(parsed['value'])}>")
    if kind == "struct":
        # Emit NOT NULL for required fields — Spark DDL accepts it inside
        # STRUCT<...> and Iceberg records the field as required.
        fields = ", ".join(
            f"{name}: {_parsed_to_spark_ddl(ftype)}"
            + (" NOT NULL" if required else "")
            for name, ftype, required in parsed["fields"])
        return f"STRUCT<{fields}>"
    raise ValueError(f"Unknown complex kind: {kind!r}")


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
        _col_name, col_type = type_choice.columns[0]
        for data_file in result.get("data_files", []):
            for row in data_file.get("rows", []):
                tv = value_map.get(row["row_id"])
                row["typed_value"] = tv
                # Lock the invariant: a present value must honor required
                # (NOT NULL) nested fields. Whole-value nulls are fine (that's
                # the top-level nullable case). Fails here rather than letting
                # Iceberg reject the commit with an opaque error.
                check_required_fields(col_type, tv)
        result["schema"] = type_choice.columns
        return result

    def _generate_typed_value(self, seed: int, type_choice: DataTypeChoice) -> Any:
        _col_name, col_type = type_choice.columns[0]
        return generate_value(col_type, seed)


class DataTypeDimension(Dimension):
    """Enumerates/samples single-column data-type schemas (primitive + complex)."""

    SUPPORTED_TYPES = [
        "boolean", "int", "long", "float", "double", "string", "binary",
        "date", "timestamp", "timestamptz", "decimal(10,2)", "fixed(16)",
    ]

    COMPLEX_TYPES = [
        "list<int>",
        "list<string>",
        "map<string,int>",
        "map<int,string>",
        "struct<a:int,b:string>",
        # Required struct field (NOT NULL) — exercises Iceberg's required-field
        # path. Spark DDL supports NOT NULL inside STRUCT<...>; list-element /
        # map-value required-ness is not expressible in DDL (see _child_specs).
        "struct<a:int NOT NULL,b:string>",
        "struct<inner:struct<x:int,y:string>,items:list<int>>",
    ]

    def enumerate(self) -> Iterator[DataTypeChoice]:
        """Yield each supported type (primitive and complex) as a single-column
        schema, in both non-nullable and nullable variants.

        The nullable variant lets the composer pair the 'nulls' value_strategy
        with a nullable type.
        """
        for type_str in self.SUPPORTED_TYPES + self.COMPLEX_TYPES:
            yield DataTypeChoice(columns=[("value", type_str)], nullable=False)
            yield DataTypeChoice(columns=[("value", type_str)], nullable=True)

    def sample(self, rng: random.Random) -> DataTypeChoice:
        """Sample one single-column schema of a random supported type (primitive
        or complex) with random nullability."""
        type_str = rng.choice(self.SUPPORTED_TYPES + self.COMPLEX_TYPES)
        return DataTypeChoice(columns=[("value", type_str)],
                              nullable=rng.choice([False, True]))
