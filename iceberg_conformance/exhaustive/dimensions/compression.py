"""Compression dimension for unified generator.

Provides compression codec selection per column for Parquet file writing.
Supports all standard Parquet compression codecs and validates compatibility
with encoding choices.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Iterator, List
import random

from . import Dimension


class CompressionCodec(Enum):
    """Supported Parquet compression codecs."""
    NONE = "NONE"
    SNAPPY = "SNAPPY"
    GZIP = "GZIP"
    LZ4 = "LZ4"
    ZSTD = "ZSTD"
    BROTLI = "BROTLI"


# Compression × Encoding compatibility matrix.
# Some encoding + compression combinations are known-invalid or suboptimal.
# This dict maps encoding → set of compatible compression codecs.
# If an encoding is not listed, all compressions are considered compatible.
COMPRESSION_ENCODING_COMPAT: Dict[str, List[str]] = {
    "PLAIN": ["NONE", "SNAPPY", "GZIP", "LZ4", "ZSTD", "BROTLI"],
    "DICTIONARY": ["NONE", "SNAPPY", "GZIP", "LZ4", "ZSTD", "BROTLI"],
    "RLE": ["NONE", "SNAPPY", "GZIP", "LZ4", "ZSTD", "BROTLI"],
    "DELTA_BINARY_PACKED": ["NONE", "SNAPPY", "GZIP", "LZ4", "ZSTD", "BROTLI"],
    "DELTA_LENGTH_BYTE_ARRAY": ["NONE", "SNAPPY", "GZIP", "LZ4", "ZSTD", "BROTLI"],
    "BYTE_STREAM_SPLIT": ["NONE", "SNAPPY", "GZIP", "LZ4", "ZSTD"],
}


def is_compression_compatible(encoding: str, compression: str) -> bool:
    """Check if a compression codec is compatible with the given encoding.

    Args:
        encoding: Parquet encoding name (e.g., "PLAIN", "DICTIONARY").
        compression: Compression codec name (e.g., "SNAPPY", "ZSTD").

    Returns:
        True if the combination is valid.
    """
    compatible = COMPRESSION_ENCODING_COMPAT.get(encoding)
    if compatible is None:
        # Unknown encoding — allow all codecs
        return True
    return compression in compatible


@dataclass
class CompressionChoice:
    """Per-column compression codec mapping.

    Attributes:
        column_codecs: Dict mapping column_name → CompressionCodec.
                       If a column is absent, the default codec is used.
        default_codec: Codec used for columns not explicitly listed.
    """
    column_codecs: Dict[str, CompressionCodec] = field(default_factory=dict)
    default_codec: CompressionCodec = CompressionCodec.ZSTD

    def get_codec(self, column_name: str) -> CompressionCodec:
        """Get the codec for a specific column."""
        return self.column_codecs.get(column_name, self.default_codec)

    def to_dict(self) -> Dict[str, str]:
        """Return dict mapping column_name → codec string value."""
        return {col: codec.value for col, codec in self.column_codecs.items()}


class CompressionDimension(Dimension):
    """Dimension for Parquet compression codec choices.

    enumerate() yields 8 representative configs:
      - 6 all-same presets (one per codec)
      - 2 mixed presets (different codecs per column)

    sample() picks an independent random codec per column.
    """

    def __init__(self, columns: List[str] = None):
        """
        Args:
            columns: List of column names. If None, presets use default_codec only.
        """
        self.columns = columns or []

    def enumerate(self) -> Iterator[CompressionChoice]:
        """Yield 8 representative compression configurations.

        6 all-same presets (one per codec) + 2 mixed presets.
        """
        # 6 all-same presets
        for codec in CompressionCodec:
            if self.columns:
                column_codecs = {col: codec for col in self.columns}
                yield CompressionChoice(column_codecs=column_codecs, default_codec=codec)
            else:
                yield CompressionChoice(column_codecs={}, default_codec=codec)

        # 2 mixed presets
        yield self._mixed_preset_alternating()
        yield self._mixed_preset_split()

    def _mixed_preset_alternating(self) -> CompressionChoice:
        """Mixed preset 1: alternate between SNAPPY and ZSTD."""
        codecs_cycle = [CompressionCodec.SNAPPY, CompressionCodec.ZSTD]
        column_codecs = {}
        for i, col in enumerate(self.columns):
            column_codecs[col] = codecs_cycle[i % 2]
        return CompressionChoice(column_codecs=column_codecs, default_codec=CompressionCodec.SNAPPY)

    def _mixed_preset_split(self) -> CompressionChoice:
        """Mixed preset 2: first half GZIP, second half LZ4."""
        column_codecs = {}
        mid = max(1, len(self.columns) // 2)
        for i, col in enumerate(self.columns):
            if i < mid:
                column_codecs[col] = CompressionCodec.GZIP
            else:
                column_codecs[col] = CompressionCodec.LZ4
        return CompressionChoice(column_codecs=column_codecs, default_codec=CompressionCodec.GZIP)

    def sample(self, rng: random.Random) -> CompressionChoice:
        """Sample a random compression choice — independent codec per column.

        Args:
            rng: Random number generator for reproducibility.

        Returns:
            CompressionChoice with randomly selected codecs per column.
        """
        all_codecs = list(CompressionCodec)
        column_codecs = {}
        for col in self.columns:
            column_codecs[col] = rng.choice(all_codecs)
        default = rng.choice(all_codecs)
        return CompressionChoice(column_codecs=column_codecs, default_codec=default)
