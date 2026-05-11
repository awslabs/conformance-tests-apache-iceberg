"""ConfigProducer registry.

Each ConfigProducer is a function: (spec: dict, minimal: bool) -> list[dict]
"""

from iceberg_fuzzer.config.simple import produce_configs as _simple
from iceberg_fuzzer.config.write_pattern import produce_configs as _write_pattern
from iceberg_fuzzer.config.partitioned import produce_configs as _partitioned
from iceberg_fuzzer.config.evolution import produce_configs as _evolution
from iceberg_fuzzer.config.all import produce_configs as _all

CONFIG_PRODUCERS = {
    'simple': _simple,
    'write_pattern': _write_pattern,
    'partitioned': _partitioned,
    'evolution': _evolution,
    'all': _all,
}
