# Test Configurations

This directory contains test-specific configuration files for the Apache Iceberg Table Fuzzer.

## Files

- **`ci_specs.yaml`** - Minimal table specifications for CI/CD testing
  - Contains a small subset of tables for fast validation
  - Used by GitHub Actions workflows
  - Focuses on basic data types and simple write patterns

- **`test_specs.yaml`** - Legacy test specifications
  - Original test configuration file
  - Contains basic table definitions for manual testing

## Usage

### CI Testing
```bash
# Run CI tests with minimal table set
python -m iceberg_fuzzer.cli generate --backend pyiceberg --warehouse-path ./test-warehouse --specs tests/ci_specs.yaml --random 5
```

### Manual Testing
```bash
# Run with legacy test specs
python -m iceberg_fuzzer.cli generate --backend pyspark --warehouse-path ./test-warehouse --specs tests/test_specs.yaml
```

## Configuration

The CI specifications are designed to:
- Complete within 20 minutes
- Test core functionality without exhaustive coverage
- Validate both PyIceberg and Polars backends
- Generate artifacts for debugging failures
