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
import os
import subprocess
import sys

import yaml

from iceberg_fuzzer.config import CONFIG_PRODUCERS

# Resolve validator script path relative to this file
_CLI_DIR = os.path.dirname(os.path.abspath(__file__))
_VALIDATOR_SCRIPT = os.path.join(
    _CLI_DIR, 'validator', 'local_duckdb', 'validator_duckdb.py'
)

def _validate_backend_constraints(backend, warehouse_path):
    """Validate backend-specific constraints for warehouse path."""
    if backend in ['pyiceberg', 'polars']:
        if warehouse_path.startswith('arn:aws:s3tables:'):
            print(f"Error: {backend} backend does not support S3 Tables ARNs")
            sys.exit(1)


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
  python3 cli.py generate --backend pyspark --specs specs/simple.yaml

  # Generate write-pattern tables with PySpark (parallel)
  python3 cli.py generate --backend pyspark \
      --specs specs/write_patterns.yaml --num-parallel 4

  # Generate partitioned tables with PySpark
  python3 cli.py generate --backend pyspark \
      --specs specs/partitioned.yaml --num-parallel 8

  # Generate minimal set with any backend
  python3 cli.py generate --backend polars --specs specs/simple.yaml --minimal

  # Generate random subset
  python3 cli.py generate --backend pyspark --specs specs/simple.yaml --random 50

  # Use a specific catalog
  python3 cli.py generate --backend pyspark --catalog glue --specs specs/simple.yaml
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
  python3 cli.py validate --specs specs/simple.yaml --num-parallel 16

  # Full content validation (slower)
  python3 cli.py validate --specs specs/simple.yaml --full-content-validation
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

    args = parser.parse_args()

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

    # Load spec and dispatch on type
    with open(args.specs, 'r') as f:
        spec = yaml.safe_load(f)

    spec_type = spec.get('type')
    if not spec_type:
        print(f"Error: spec file '{args.specs}' is missing required 'type' key", file=sys.stderr)
        sys.exit(1)

    if spec_type not in CONFIG_PRODUCERS:
        print(f"Error: unknown spec type '{spec_type}'", file=sys.stderr)
        sys.exit(1)

    # Produce table configs
    produce_configs = CONFIG_PRODUCERS[spec_type]
    table_configs = produce_configs(spec, minimal=args.minimal)

    # Filter to single table if requested
    if args.table_name:
        table_configs = [c for c in table_configs if c['name'] == args.table_name]
        if not table_configs:
            print(f"Error: table '{args.table_name}' not found in configs for type '{spec_type}'")
            sys.exit(1)

    # Apply random sampling
    if args.random and args.random < len(table_configs):
        import random
        table_configs = random.sample(table_configs, args.random)

    # Instantiate backend
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
        success_count, total_count = generator.generate(
            table_configs, spec,
            num_parallel=args.num_parallel,
            delete_existing=args.delete_existing,
        )

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
            if args.backend == 'pyiceberg':
                # PyIceberg only supports primitives
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

    except NotImplementedError as e:
        print(f"Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error generating tables: {e}")
        sys.exit(1)

    finally:
        generator.cleanup()


if __name__ == '__main__':
    main()
