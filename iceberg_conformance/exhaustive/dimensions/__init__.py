"""Dimensions package — data types, encoding, encryption, compression."""

from abc import ABC, abstractmethod
from typing import Any, Iterator
import random


class Dimension(ABC):
    """Base class for generation dimensions."""

    @abstractmethod
    def enumerate(self) -> Iterator[Any]:
        """Yield all possible values for this dimension."""
        pass

    @abstractmethod
    def sample(self, rng: random.Random) -> Any:
        """Sample one value for this dimension using the provided RNG."""
        pass


# Re-export protocol interfaces for convenient access
from .interfaces import SchemaProvider, WriteOptionsProvider, WriteOptions  # noqa: E402

# Re-export compression types for convenient access
from .compression import (  # noqa: E402
    CompressionCodec,
    CompressionChoice,
    CompressionDimension,
    is_compression_compatible,
    COMPRESSION_ENCODING_COMPAT,
)
