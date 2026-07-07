"""Encoding dimension for unified generator."""

from dataclasses import dataclass
from typing import Dict, List, Iterator
import random
import logging
from . import Dimension

logger = logging.getLogger(__name__)

# Mapping from Iceberg type to valid Parquet encodings.
#
# Empirically derived from what PyArrow's writer actually accepts for each
# type's physical Parquet representation (verified per type × encoding). Notes:
# - RLE is only valid for BOOLEAN (not int/long, despite Parquet's spec listing
#   RLE for integers — PyArrow's writer rejects it).
# - int/long/timestamp/date/time also accept BYTE_STREAM_SPLIT.
# - uuid maps to a STRING physical column, so it accepts DELTA_LENGTH_BYTE_ARRAY.
# - fixed maps to FIXED_LEN_BYTE_ARRAY, which accepts BYTE_STREAM_SPLIT (not
#   DELTA_LENGTH_BYTE_ARRAY).
# DICTIONARY is universally available (applied via use_dictionary).
ENCODING_COMPATIBILITY = {
    "int": ["PLAIN", "DICTIONARY", "DELTA_BINARY_PACKED", "BYTE_STREAM_SPLIT"],
    "integer": ["PLAIN", "DICTIONARY", "DELTA_BINARY_PACKED", "BYTE_STREAM_SPLIT"],
    "long": ["PLAIN", "DICTIONARY", "DELTA_BINARY_PACKED", "BYTE_STREAM_SPLIT"],
    "string": ["PLAIN", "DICTIONARY", "DELTA_LENGTH_BYTE_ARRAY"],
    "binary": ["PLAIN", "DICTIONARY", "DELTA_LENGTH_BYTE_ARRAY"],
    "float": ["PLAIN", "DICTIONARY", "BYTE_STREAM_SPLIT"],
    "double": ["PLAIN", "DICTIONARY", "BYTE_STREAM_SPLIT"],
    "boolean": ["PLAIN", "DICTIONARY", "RLE"],
    "timestamp": ["PLAIN", "DICTIONARY", "DELTA_BINARY_PACKED", "BYTE_STREAM_SPLIT"],
    "timestamp_ntz": ["PLAIN", "DICTIONARY", "DELTA_BINARY_PACKED", "BYTE_STREAM_SPLIT"],
    "timestamptz": ["PLAIN", "DICTIONARY", "DELTA_BINARY_PACKED", "BYTE_STREAM_SPLIT"],
    "date": ["PLAIN", "DICTIONARY", "DELTA_BINARY_PACKED", "BYTE_STREAM_SPLIT"],
    "decimal": ["PLAIN", "DICTIONARY", "BYTE_STREAM_SPLIT"],
    "uuid": ["PLAIN", "DICTIONARY", "DELTA_LENGTH_BYTE_ARRAY"],
    "fixed": ["PLAIN", "DICTIONARY", "BYTE_STREAM_SPLIT"],
    "time": ["PLAIN", "DICTIONARY", "DELTA_BINARY_PACKED", "BYTE_STREAM_SPLIT"],
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
    """Check if an encoding is valid for the given (primitive) column type.

    Parameterized primitives (fixed(L), decimal(P,S)) normalize to their base
    key. Complex-type leaf-intersection is added when complex types land.
    """
    return encoding in ENCODING_COMPATIBILITY.get(_base_primitive(column_type), [])


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
