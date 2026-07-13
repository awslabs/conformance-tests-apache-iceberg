#!/usr/bin/env python3
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
"""
Apache Iceberg Table Fuzzer
Unified CLI for generating Apache Iceberg tables with PySpark, PyIceberg, or Polars backends
"""

import argparse
import logging
import os
import subprocess
import sys

logger = logging.getLogger(__name__)

# Resolve validator script path relative to this file
_CLI_DIR = os.path.dirname(os.path.abspath(__file__))
_VALIDATOR_SCRIPT = os.path.join(
    _CLI_DIR, 'validator', 'local_duckdb', 'validator_duckdb.py'
)


def _incompatibility_reason(col_type, encoding, compression, nullable, value_strategy):
    """Return a reason string if the (type, encoding, compression, nullability,
    value_strategy) combination cannot produce a valid table, else None.

    The single compatibility gate build-shapes applies before building:
      1. value_strategy 'nulls' requires a nullable column.
      2. encoding × type: the encoding must be valid for the column type.
      3. encoding × compression: the codec must be compatible with the encoding.
    """
    from iceberg_fuzzer.exhaustive.dimensions.encoding import is_compatible as _enc_type_ok
    from iceberg_fuzzer.exhaustive.dimensions.compression import (
        is_compression_compatible as _enc_comp_ok)

    if value_strategy == "nulls" and not nullable:
        return "nulls value_strategy requires a nullable type"
    if encoding is not None and not _enc_type_ok(col_type, encoding.upper()):
        return f"encoding {encoding.upper()} incompatible with type {col_type}"
    if encoding is not None and compression is not None:
        if not _enc_comp_ok(encoding.upper(), compression.upper()):
            return (f"compression {compression.upper()} incompatible with "
                    f"encoding {encoding.upper()}")
    return None

def _validate_backend_constraints(backend, warehouse_path):
    """Validate backend-specific constraints for warehouse path."""
    if backend in ['pyiceberg', 'polars']:
        if warehouse_path.startswith('arn:aws:s3tables:'):
            print(f"Error: {backend} backend does not support S3 Tables ARNs")
            sys.exit(1)

def _generate_tables_for_specs(generator, args):
    """Generate tables based on specs file type."""
    specs_file = os.path.basename(args.specs).lower()

    if 'simple' in specs_file:
        generator.generate_simple_tables(
            args.num_parallel,
            minimal=args.minimal,
            random_count=args.random,
            delete_existing=args.delete_existing
        )
    elif 'write_pattern' in specs_file:
        generator.generate_write_pattern_tables(
            args.num_parallel,
            random_count=args.random,
            delete_existing=args.delete_existing
        )
    elif 'partitioned' in specs_file:
        generator.generate_partitioned_tables(
            args.num_parallel,
            random_count=args.random,
            minimal=args.minimal,
            delete_existing=args.delete_existing
        )
    elif 'evolution' in specs_file:
        generator.generate_evolution_tables(
            args.num_parallel,
            random_count=args.random,
            delete_existing=args.delete_existing
        )
    elif 'table_specs.yaml' in specs_file:
        _generate_all_table_types(generator, args)
    else:
        # Default to simple tables for backward compatibility
        generator.generate_simple_tables(
            args.num_parallel,
            minimal=args.minimal,
            random_count=args.random,
            delete_existing=args.delete_existing
        )

def _generate_all_table_types(generator, args):
    """Generate all table types when using main table_specs.yaml."""
    print("Generating all table types (simple + write-pattern + partitioned)...")

    # Generate simple tables
    print("\n" + "="*60)
    print("GENERATING SIMPLE TABLES")
    print("="*60)
    generator.generate_simple_tables(
        args.num_parallel,
        minimal=args.minimal,
        random_count=args.random,
        delete_existing=args.delete_existing
    )

    # Generate write-pattern tables
    print("\n" + "="*60)
    print("GENERATING WRITE-PATTERN TABLES")
    print("="*60)
    generator.generate_write_pattern_tables(
        args.num_parallel,
        random_count=args.random,
        delete_existing=False  # Don't delete again
    )

    # Generate partitioned tables (PySpark only)
    if args.backend == 'pyspark':
        print("\n" + "="*60)
        print("GENERATING PARTITIONED TABLES")
        print("="*60)
        generator.generate_partitioned_tables(
            args.num_parallel,
            random_count=args.random,
            minimal=args.minimal,
            delete_existing=False  # Don't delete again
        )
    else:
        print(
            f"\nSkipping partitioned tables - {args.backend} backend "
            "does not support partitioned tables"
        )

def main():
    parser = argparse.ArgumentParser(
        description='Apache Iceberg Table Fuzzer: Generate and validate Apache Iceberg tables',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    subparsers = parser.add_subparsers(dest='command', required=True, help='Command to execute')

    # Generate subcommand
    generate_parser = subparsers.add_parser('generate',
        help='Generate Iceberg tables',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate simple tables with PySpark
  python3 cli.py generate --backend pyspark --warehouse-path /tmp/warehouse --specs specs/simple.yaml

  # Generate write-pattern tables with PySpark (parallel)
  python3 cli.py generate --backend pyspark --warehouse-path /tmp/warehouse \
      --specs specs/write_patterns.yaml --num-parallel 4

  # Generate partitioned tables with PySpark
  python3 cli.py generate --backend pyspark --warehouse-path /tmp/warehouse \
      --specs specs/partitioned.yaml --num-parallel 8

  # Generate minimal set with any backend
  python3 cli.py generate --backend polars --warehouse-path /tmp/warehouse --specs specs/simple.yaml --minimal

  # Generate random subset
  python3 cli.py generate --backend pyspark --warehouse-path /tmp/warehouse --specs specs/simple.yaml --random 50
        """)

    generate_parser.add_argument('--backend',
                       choices=['pyspark', 'pyiceberg', 'polars'],
                       required=True,
                       help='Backend to use for table generation')
    generate_parser.add_argument('--warehouse-path',
                       required=True,
                       help='Warehouse path (local path or S3 Tables ARN for pyspark)')
    generate_parser.add_argument('--specs',
                       required=True,
                       help='Path to table specs YAML file')
    generate_parser.add_argument('--num-parallel',
                       type=int,
                       default=1,
                       help='Number of parallel workers (default: 1)')
    generate_parser.add_argument('--minimal',
                       action='store_true',
                       help='Generate minimal set (none compression, plain encoding only)')
    generate_parser.add_argument('--random',
                       type=int,
                       metavar='N',
                       help='Generate N randomly chosen tables from the available set')
    generate_parser.add_argument('--validate',
                       action='store_true',
                       help='Validate tables after generation')
    generate_parser.add_argument('--delete-existing',
                       action='store_true',
                       help='Delete existing tables before generating new ones')
    generate_parser.add_argument('--table-name',
                       help='Generate only the specified table by name')

    # Validate subcommand
    validate_parser = subparsers.add_parser('validate',
        help='Validate existing Iceberg tables',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Validate tables with parallel workers
  python3 cli.py validate --warehouse-path /tmp/warehouse --specs specs/simple.yaml --num-parallel 16

  # Full content validation (slower)
  python3 cli.py validate --warehouse-path /tmp/warehouse --specs specs/simple.yaml --full-content-validation
        """)

    validate_parser.add_argument('--warehouse-path',
                       required=True,
                       help='Warehouse path')
    validate_parser.add_argument('--specs',
                       required=True,
                       help='Path to table specs YAML file')
    validate_parser.add_argument('--num-parallel',
                       type=int,
                       default=1,
                       help='Number of parallel workers (default: 1)')
    validate_parser.add_argument('--full-content-validation',
                       action='store_true',
                       help='Validate all rows (slower)')
    validate_parser.add_argument('--minimal',
                       action='store_true',
                       help='Validate minimal set (none compression, plain encoding only)')
    validate_parser.add_argument('--primitives-only',
                       action='store_true',
                       help='Validate only primitive types (skip lists, maps, structs)')

    # Delete subcommand
    delete_parser = subparsers.add_parser('delete',
        help='Delete all tables in warehouse',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Delete all tables in local warehouse (with confirmation)
  python3 cli.py delete --backend pyspark --warehouse-path /tmp/warehouse

  # Delete all tables in S3 Tables bucket (with confirmation)
  python3 cli.py delete --backend pyspark --warehouse-path "arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket"

  # Delete without interactive confirmation
  python3 cli.py delete --backend pyspark --warehouse-path /tmp/warehouse --confirm
        """)

    delete_parser.add_argument('--backend',
                       choices=['pyspark', 'pyiceberg'],
                       required=True,
                       help='Backend to use for table deletion')
    delete_parser.add_argument('--warehouse-path',
                       required=True,
                       help='Warehouse path (local path or S3 Tables ARN for pyspark)')
    delete_parser.add_argument('--confirm',
                       action='store_true',
                       help='Confirm deletion without interactive prompt')
    delete_parser.add_argument('--num-parallel',
                       type=int,
                       default=1,
                       help='Number of parallel workers for deletion (default: 1)')

    # Build-shapes subcommand
    shapes_parser = subparsers.add_parser('build-shapes',
        help='Build Iceberg tables from exhaustive shape configs',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Build a single config by ID
  iceberg-fuzzer build-shapes --warehouse-path /tmp/warehouse --config-id 84823 --target-size-mb 0.01

  # Build first 10 configs with validation
  iceberg-fuzzer build-shapes --warehouse-path /tmp/warehouse --limit 10 --validate

  # Build all configs with custom namespace
  iceberg-fuzzer build-shapes --warehouse-path /tmp/warehouse --namespace my_shapes --target-size-mb 0.1
        """)

    shapes_parser.add_argument('--warehouse-path',
                       required=True,
                       help='Local warehouse path for Iceberg tables')
    shapes_parser.add_argument('--max-row-count',
                       type=int,
                       default=6,
                       help='Max row count for config generation (default: 6)')
    shapes_parser.add_argument('--seed',
                       type=int,
                       default=42,
                       help='Random seed (default: 42)')
    shapes_parser.add_argument('--target-size-mb',
                       type=float,
                       default=0.1,
                       help='Target sentinel file size in MB (default: 0.1)')
    shapes_parser.add_argument('--namespace',
                       default='exhaustive_shapes',
                       help='Iceberg namespace (default: exhaustive_shapes)')
    shapes_parser.add_argument('--config-id',
                       type=int,
                       help='Build only the config with this ID')
    shapes_parser.add_argument('--limit',
                       type=int,
                       help='Limit number of configs to build')
    shapes_parser.add_argument('--validate',
                       action='store_true',
                       help='Run validations after building each table')
    # Optional dimension knobs — a single 'value' column of the given type,
    # with a table-wide encoding/compression. Each has a default so any subset
    # may be given. (Only 'int' is supported for now; nested types like
    # 'list<int>' / 'struct<a:int,b:string>' land in a follow-up.)
    shapes_parser.add_argument('--data-type',
                       default='int',
                       help="Column Iceberg type (default: %(default)s). Only 'int' supported for now.")
    shapes_parser.add_argument('--nullable',
                       action='store_true',
                       help='Make the column nullable (required for nulls value_strategy)')
    shapes_parser.add_argument('--encoding',
                       default='PLAIN',
                       help='Parquet encoding for the column, e.g. PLAIN, DICTIONARY (default: %(default)s)')
    shapes_parser.add_argument('--compression',
                       default='zstd',
                       help='Parquet compression codec, e.g. snappy, gzip, zstd, none (default: %(default)s)')

    # Workflow subcommand
    workflow_parser = subparsers.add_parser('workflow',
        help='Run correctness workflows against existing tables',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run a compaction workflow
  python3 cli.py workflow run --workflow specs/workflows/compaction.yaml \
      --warehouse-path /tmp/warehouse --table-name my_table --namespace simple

  # Dry-run to see what would execute
  python3 cli.py workflow run --workflow specs/workflows/compaction.yaml \
      --warehouse-path /tmp/warehouse --table-name my_table --namespace simple --dry-run
        """)

    workflow_subparsers = workflow_parser.add_subparsers(dest='workflow_action', required=True)
    workflow_run_parser = workflow_subparsers.add_parser('run', help='Run a workflow')
    workflow_run_parser.add_argument('--workflow',
                       required=True,
                       help='Path to workflow YAML file')
    workflow_run_parser.add_argument('--warehouse-path',
                       required=True,
                       help='Warehouse path')
    workflow_run_parser.add_argument('--output-dir',
                       default='output/workflow-logs',
                       help='Directory for operation stdout/stderr (default: output/workflow-logs/)')
    workflow_run_parser.add_argument('--table-name',
                       required=True,
                       help='Table name to run the workflow against')
    workflow_run_parser.add_argument('--namespace',
                       required=True,
                       help='Table namespace')
    workflow_run_parser.add_argument('--dry-run',
                       action='store_true',
                       help='Print operations that would be executed without running them')
    workflow_run_parser.add_argument('--seed',
                       type=int,
                       default=None,
                       help='Random seed for non-deterministic operations (e.g., delete row selection)')
    workflow_run_parser.add_argument('--point-queries', action=argparse.BooleanOptionalAction,
                       default=True,
                       help='Run point query validation after each step (default: %(default)s)')

    args = parser.parse_args()

    # Handle build-shapes command
    if args.command == 'build-shapes':
        from iceberg_fuzzer.exhaustive import TableConfigGenerator, UnifiedTableBuilder
        from iceberg_fuzzer.exhaustive.dimensions.data_types import (
            DataTypeChoice, TypedValueFiller)
        from iceberg_fuzzer.exhaustive.dimensions.interfaces import WriteOptions
        from iceberg_fuzzer.exhaustive.dimensions.encoding import EncodingChoice
        from iceberg_fuzzer.exhaustive.dimensions.compression import (
            CompressionChoice, CompressionCodec)

        # Build the dimension inputs from the CLI flags. Defaults are set on the
        # arguments (type=int, encoding=PLAIN, compression=zstd, non-nullable),
        # so any subset may be given and we can read them directly.
        col_type = args.data_type
        encoding = args.encoding.upper()
        compression = args.compression.upper()
        nullable = args.nullable

        schema_provider = DataTypeChoice([("value", col_type)], nullable=nullable)
        filler = TypedValueFiller()
        write_options = WriteOptions(
            encoding=EncodingChoice({"value": encoding}),
            compression=CompressionChoice(default_codec=CompressionCodec(compression)),
            schema={"value": col_type})

        gen = TableConfigGenerator(max_row_count=args.max_row_count, seed=args.seed)

        built = 0
        failed = 0
        skipped = 0
        with UnifiedTableBuilder(
            args.warehouse_path,
            target_size_mb=args.target_size_mb,
            seed=args.seed,
        ) as builder:
            for config in gen.generate_all():
                if args.config_id is not None and config['config_id'] != args.config_id:
                    continue

                table_name = f"table_{config['config_id']}"

                # Compatibility gate: skip (with a logged reason) any config whose
                # value_strategy/type/encoding/compression combination can't
                # produce a valid table.
                reason = _incompatibility_reason(
                    col_type, encoding, compression, nullable,
                    config.get('value_strategy'))
                if reason is not None:
                    skipped += 1
                    logger.info("skip %s: %s", table_name, reason)

                    if args.config_id is not None:
                        break
                    continue

                try:
                    # Fill typed_value (respecting the config's value_strategy)
                    # before building.
                    build_config = filler.fill(config, schema_provider)

                    stats = builder.build_table(
                        build_config, args.namespace, table_name,
                        schema_provider=schema_provider, write_options=write_options)
                    built += 1

                    status = "OK"
                    if args.validate and stats.validation_messages:
                        status = "WARN"

                    print(f"[{built}] {table_name}: {stats.snapshot_count} snaps, "
                          f"{stats.data_file_count} data, {stats.delete_file_count} del "
                          f"— {status}")

                    if args.validate and stats.validation_messages:
                        for msg in stats.validation_messages:
                            print(f"      {msg}")

                except Exception as e:
                    failed += 1
                    print(f"[ERR] {table_name}: {e}")

                if args.config_id is not None:
                    break
                if args.limit is not None and built >= args.limit:
                    break

        print(f"\nDone: {built} built, {failed} failed, {skipped} skipped")
        sys.exit(1 if failed > 0 else 0)

    # Handle workflow command
    if args.command == 'workflow':
        from iceberg_fuzzer.workflow import run_workflow
        result = run_workflow(
            workflow_file=args.workflow,
            warehouse_path=args.warehouse_path,
            output_dir=args.output_dir,
            table_name=args.table_name,
            namespace=args.namespace,
            dry_run=args.dry_run,
            seed=args.seed,
            point_queries=args.point_queries,
        )
        sys.exit(0 if result is None or result.passed else 1)

    # Handle delete command
    if args.command == 'delete':
        if not args.confirm:
            response = input(f"Are you sure you want to delete ALL tables in {args.warehouse_path}? (yes/no): ")
            if response.lower() != 'yes':
                print("Deletion cancelled.")
                sys.exit(0)

        # Import and instantiate the appropriate generator for deletion
        if args.backend == 'pyspark':
            from iceberg_fuzzer.backend.pyspark.generator import PySparkGenerator
            generator = PySparkGenerator(args.warehouse_path, None)
        elif args.backend == 'pyiceberg':
            from iceberg_fuzzer.backend.pyiceberg.generator import PyIcebergGenerator
            generator = PyIcebergGenerator(args.warehouse_path, None)
        else:
            print(f"Error: Backend '{args.backend}' does not support delete operation")
            sys.exit(1)

        try:
            generator.delete_all_tables(args.num_parallel)
            print("All tables deleted successfully.")
        except Exception as e:
            print(f"Error deleting tables: {e}")
            sys.exit(1)

        sys.exit(0)

    # Handle validate command
    if args.command == 'validate':
        validator_cmd = [
            sys.executable, _VALIDATOR_SCRIPT,
            '--warehouse-path', args.warehouse_path,
            '--specs', args.specs,
            '--num-parallel', str(args.num_parallel)
        ]
        if args.full_content_validation:
            validator_cmd.append('--full-content-validation')
        if args.minimal:
            validator_cmd.append('--minimal')
        if args.primitives_only:
            validator_cmd.append('--primitives-only')

        try:
            result = subprocess.run(validator_cmd)
            sys.exit(result.returncode)
        except FileNotFoundError:
            print(f"Error: Validator script not found at {_VALIDATOR_SCRIPT}")
            sys.exit(1)
        except Exception as e:
            print(f"Error running validator: {e}")
            sys.exit(1)

    # Handle generate command
    # Validate backend-specific constraints
    _validate_backend_constraints(args.backend, args.warehouse_path)

    # Import and instantiate the appropriate generator
    if args.backend == 'pyspark':
        from iceberg_fuzzer.backend.pyspark.generator import PySparkGenerator
        generator = PySparkGenerator(args.warehouse_path, args.specs)
    elif args.backend == 'pyiceberg':
        from iceberg_fuzzer.backend.pyiceberg.generator import PyIcebergGenerator
        generator = PyIcebergGenerator(args.warehouse_path, args.specs)
    else:  # polars
        from iceberg_fuzzer.backend.polars.generator import PolarsGenerator
        generator = PolarsGenerator(args.warehouse_path, args.specs)

    try:
        # Handle single table generation
        if args.table_name:
            generator.generate_single_table(args.table_name, delete_existing=args.delete_existing)
        else:
            _generate_tables_for_specs(generator, args)

        # Validate tables if requested
        if args.validate:
            print("\n" + "="*80)
            print("Starting table validation...")
            print("="*80 + "\n")

            validator_cmd = [
                sys.executable, _VALIDATOR_SCRIPT,
                '--warehouse-path', args.warehouse_path,
                '--specs', args.specs,
                '--num-parallel', str(args.num_parallel)
            ]
            if args.minimal:
                validator_cmd.append('--minimal')
            # PyIceberg only supports primitives
            if args.backend == 'pyiceberg':
                validator_cmd.append('--primitives-only')

            try:
                result = subprocess.run(
                    validator_cmd, capture_output=False
                )
                if result.returncode != 0:
                    print(f"\n⚠️  Validation completed with errors (exit code: {result.returncode})")
                else:
                    print("\n✅ Validation completed successfully")
            except FileNotFoundError:
                print(
                    f"\n❌ Error: Validator script not found at {_VALIDATOR_SCRIPT}"
                )
            except Exception as e:
                print(f"\n❌ Error running validator: {e}")

    except Exception as e:
        print(f"Error generating tables: {e}")
        sys.exit(1)

    finally:
        generator.cleanup()

if __name__ == '__main__':
    main()
