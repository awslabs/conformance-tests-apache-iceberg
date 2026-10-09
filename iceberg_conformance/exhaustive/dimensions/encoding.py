"""Encoding dimension for unified generator."""

from dataclasses import dataclass
from typing import Dict, List, Iterator
import random
import logging
from . import Dimension

logger = logging.getLogger(__name__)

# Mapping from Iceberg type to valid Parquet encodings.
#
# An encoding is listed based on two criteria:
#   - It appears in the Parquet spec's supported encodings for the type:
#     https://parquet.apache.org/docs/file-format/data-pages/encodings/#supported-encodings
#   - Parquet versioning is not well defined today. Forward-incompatible
#     features, such as newer encodings, were introduced later without a clear
#     indicator of breaking changes, so using them with older readers may fail.
#     We therefore list an encoding only if PyArrow WRITES it AND our readers
#     (DuckDB, Spark) READ it back — a file that writes but can't be read is
#     worse than useless.
#
# Specific callouts:
# - RLE is only valid for BOOLEAN. PyArrow's writer rejects it at write time for
#   other types, e.g. int/long ("Encoding RLE is not supported for type int32"),
#   so no file is ever produced — the modern integer equivalent is
#   DELTA_BINARY_PACKED.
# - BYTE_STREAM_SPLIT is restricted to FLOAT/DOUBLE. Parquet format 2.11
#   extended it to int/long/date/decimal/fixed/timestamp, and PyArrow writes
#   that newer variant, but it is forward-incompatible: older readers (DuckDB
#   here) cannot read it and reject with "BYTE_STREAM_SPLIT encoding is only
#   supported for FLOAT or DOUBLE data". Listing it for those types just
#   produced unreadable files, so it is float/double-only here. (Could be made
#   opt-in later — generate the files but skip DuckDB read-back validation — to
#   fuzz writer-side handling of the newer encoding.)
# - DICTIONARY is universally available (applied via use_dictionary).
ENCODING_COMPATIBILITY = {
    "int": ["PLAIN", "DICTIONARY", "DELTA_BINARY_PACKED"],
    "integer": ["PLAIN", "DICTIONARY", "DELTA_BINARY_PACKED"],
    "long": ["PLAIN", "DICTIONARY", "DELTA_BINARY_PACKED"],
    "string": ["PLAIN", "DICTIONARY", "DELTA_LENGTH_BYTE_ARRAY"],
    "binary": ["PLAIN", "DICTIONARY", "DELTA_LENGTH_BYTE_ARRAY"],
    "float": ["PLAIN", "DICTIONARY", "BYTE_STREAM_SPLIT"],
    "double": ["PLAIN", "DICTIONARY", "BYTE_STREAM_SPLIT"],
    "boolean": ["PLAIN", "DICTIONARY", "RLE"],
    "timestamp": ["PLAIN", "DICTIONARY", "DELTA_BINARY_PACKED"],
    "timestamp_ntz": ["PLAIN", "DICTIONARY", "DELTA_BINARY_PACKED"],
    "timestamptz": ["PLAIN", "DICTIONARY", "DELTA_BINARY_PACKED"],
    "date": ["PLAIN", "DICTIONARY", "DELTA_BINARY_PACKED"],
    "decimal": ["PLAIN", "DICTIONARY"],
    "fixed": ["PLAIN", "DICTIONARY"],
    "time": ["PLAIN", "DICTIONARY", "DELTA_BINARY_PACKED"],
}


@dataclass
class EncodingChoice:
    """Maps column names to encoding names."""
    column_encodings: Dict[str, str]  # column_name -> encoding_name

    def to_dict(self) -> Dict[str, str]:
        return self.column_encodings


def _base_primitive(type_str: str) -> str:
    """Normalize a primitive type string to its ENCODING_COMPATIBILITY key.

    Strips parameters: fixed(16)->fixed, decimal(10,2)->decimal.
    """
    return type_str.strip().lower().split("(")[0].strip()


def is_compatible(column_type: str, encoding: str) -> bool:
    """Check if an encoding is valid for the given column type.

    Parameterized primitives (fixed(L), decimal(P,S)) normalize to their base
    key. For a complex type (list/map/struct), the table-wide encoding is
    applied to each physical leaf column, so it is compatible only if it is
    valid for *every* leaf in the nested tree (the intersection). An empty
    complex type (no leaves) is vacuously compatible.
    """
    # Import here to avoid a circular import at module load (data_types imports
    # from this package's __init__, which does not import encoding eagerly).
    from .data_types import parse_type, _child_specs, _is_complex

    def _leaves(parsed) -> list:
        if not _is_complex(parsed):
            return [parsed]
        out: list = []
        for _name, child, _nullable in _child_specs(parsed):
            out.extend(_leaves(child))
        return out

    parsed = parse_type(column_type)
    return all(
        encoding in ENCODING_COMPATIBILITY.get(_base_primitive(leaf), [])
        for leaf in _leaves(parsed)
    )


class EncodingDimension(Dimension):
    """Dimension for Parquet encoding choices."""

    def __init__(self, schema: Dict[str, str]):
        """
        Args:
            schema: Dict mapping column_name -> type_name (e.g., {"id": "int", "name": "string"})
        """
        self.schema = schema

    def enumerate(self) -> Iterator[EncodingChoice]:
        """Yield all valid encoding combinations for the schema."""
        columns = list(self.schema.keys())
        if not columns:
            yield EncodingChoice({})
            return

        def generate_combinations(idx: int, current: Dict[str, str]):
            if idx == len(columns):
                yield EncodingChoice(current.copy())
                return

            col_name = columns[idx]
            col_type = self.schema[col_name]
            valid_encodings = ENCODING_COMPATIBILITY.get(col_type, ["PLAIN"])

            for encoding in valid_encodings:
                current[col_name] = encoding
                yield from generate_combinations(idx + 1, current)

        yield from generate_combinations(0, {})

    def sample(self, rng: random.Random) -> EncodingChoice:
        """Sample one random valid encoding per column."""
        result = {}
        for col_name, col_type in self.schema.items():
            valid_encodings = ENCODING_COMPATIBILITY.get(col_type, ["PLAIN"])
            result[col_name] = rng.choice(valid_encodings)
        return EncodingChoice(result)
