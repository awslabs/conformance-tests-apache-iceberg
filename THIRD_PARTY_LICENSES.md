# Third-Party Package Licenses

This document lists all third-party packages used in the Iceberg Table Generator project and their respective licenses.

## Core Dependencies

| Package | Version | License | Description |
|---------|---------|---------|-------------|
| **PySpark** | 3.4.4 | Apache-2.0 | Apache Spark Python API for distributed data processing |
| **PyIceberg** | 0.10.0 | Apache-2.0 | Python implementation of Apache Iceberg table format |
| **PyArrow** | 21.0.0 | Apache-2.0 | Columnar in-memory analytics library |
| **DuckDB** | 1.4.1 | MIT | In-process SQL OLAP database management system |
| **Polars** | 1.35.2 | MIT | Fast DataFrame library written in Rust |
| **Boto3** | 1.40.69 | Apache-2.0 | AWS SDK for Python |
| **PyYAML** | 5.4.1 | MIT | YAML parser and emitter for Python |

## Standard Library Modules

The following Python standard library modules are used (no additional licenses required):
- `argparse` - Command-line argument parsing
- `concurrent.futures` - High-level interface for asynchronously executing callables
- `datetime` - Date and time handling
- `decimal` - Decimal fixed point and floating point arithmetic
- `os` - Operating system interface
- `pathlib` - Object-oriented filesystem paths
- `sys` - System-specific parameters and functions
- `typing` - Type hints support
- `uuid` - UUID objects generation
- `abc` - Abstract base classes

## License Summary

- **Apache-2.0**: PySpark, PyIceberg, PyArrow, Boto3 (4 packages)
- **MIT**: DuckDB, Polars, PyYAML (3 packages)

## License Compatibility

All licenses used in this project are permissive open-source licenses that are compatible with each other and allow for commercial use, modification, and distribution.

### Apache License 2.0
- Allows commercial use, modification, distribution, and private use
- Requires preservation of copyright and license notices
- Provides patent grant from contributors

### MIT License  
- Allows commercial use, modification, distribution, and private use
- Requires preservation of copyright and license notices
- Simple and permissive license

## Full License Texts

For complete license texts, refer to:
- Apache-2.0: https://www.apache.org/licenses/LICENSE-2.0
- MIT: https://opensource.org/licenses/MIT

## Package Homepages

- **PySpark**: https://spark.apache.org/
- **PyIceberg**: https://py.iceberg.apache.org/
- **PyArrow**: https://arrow.apache.org/docs/python/
- **DuckDB**: https://duckdb.org/
- **Polars**: https://pola.rs/
- **Boto3**: https://boto3.amazonaws.com/
- **PyYAML**: https://pyyaml.org/
