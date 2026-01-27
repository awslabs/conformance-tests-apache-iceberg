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
import sys
from pathlib import Path

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
  python3 cli.py generate --backend pyspark --warehouse-path /tmp/warehouse --specs specs/write_patterns.yaml --num-parallel 4
  
  # Generate partitioned tables with PySpark
  python3 cli.py generate --backend pyspark --warehouse-path /tmp/warehouse --specs specs/partitioned.yaml --num-parallel 8
  
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
    
    args = parser.parse_args()
    
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
        
        try:
            generator.delete_all_tables(args.num_parallel)
            print("All tables deleted successfully.")
        except Exception as e:
            print(f"Error deleting tables: {e}")
            sys.exit(1)
        
        sys.exit(0)
    
    # Handle validate command
    if args.command == 'validate':
        import subprocess
        validator_cmd = [
            'python3', 'validator/local_duckdb/validator_duckdb.py',
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
        
        result = subprocess.run(validator_cmd)
        sys.exit(result.returncode)
    
    # Handle generate command
    # Validate backend-specific constraints
    if args.backend in ['pyiceberg', 'polars']:
        if args.warehouse_path.startswith('arn:aws:s3tables:'):
            print(f"Error: {args.backend} backend does not support S3 Tables ARNs")
            sys.exit(1)
    
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
            # Determine table type from specs file name
            specs_file = args.specs.lower()
            if 'simple' in specs_file:
                generator.generate_simple_tables(args.num_parallel, minimal=args.minimal, random_count=args.random, delete_existing=args.delete_existing)
            elif 'write_pattern' in specs_file:
                generator.generate_write_pattern_tables(args.num_parallel, random_count=args.random, delete_existing=args.delete_existing)
            elif 'partitioned' in specs_file:
                generator.generate_partitioned_tables(args.num_parallel, random_count=args.random, minimal=args.minimal, delete_existing=args.delete_existing)
            elif 'evolution' in specs_file:
                generator.generate_evolution_tables(args.num_parallel, random_count=args.random, delete_existing=args.delete_existing)
            elif 'table_specs.yaml' in specs_file:
                # Generate all table types when using main table_specs.yaml
                print("Generating all table types (simple + write-pattern + partitioned)...")
                
                # Generate simple tables
                print("\n" + "="*60)
                print("GENERATING SIMPLE TABLES")
                print("="*60)
                generator.generate_simple_tables(args.num_parallel, minimal=args.minimal, random_count=args.random, delete_existing=args.delete_existing)
                
                # Generate write-pattern tables  
                print("\n" + "="*60)
                print("GENERATING WRITE-PATTERN TABLES")
                print("="*60)
                generator.generate_write_pattern_tables(args.num_parallel, random_count=args.random, delete_existing=False)  # Don't delete again
                
                # Generate partitioned tables (PySpark only)
                if args.backend == 'pyspark':
                    print("\n" + "="*60)
                    print("GENERATING PARTITIONED TABLES")
                    print("="*60)
                    generator.generate_partitioned_tables(args.num_parallel, random_count=args.random, minimal=args.minimal, delete_existing=False)  # Don't delete again
                else:
                    print(f"\nSkipping partitioned tables - {args.backend} backend does not support partitioned tables")
            else:
                # Default to simple tables for backward compatibility
                generator.generate_simple_tables(args.num_parallel, minimal=args.minimal, random_count=args.random, delete_existing=args.delete_existing)
        
        # Validate tables if requested
        if args.validate:
            print("\n" + "="*80)
            print("Starting table validation...")
            print("="*80 + "\n")
            
            import subprocess
            validator_cmd = [
                'python3', 'validator/local_duckdb/validator_duckdb.py',
                '--warehouse-path', args.warehouse_path,
                '--specs', args.specs,
                '--num-parallel', str(args.num_parallel)
            ]
            if args.minimal:
                validator_cmd.append('--minimal')
            # PyIceberg only supports primitives
            if args.backend == 'pyiceberg':
                validator_cmd.append('--primitives-only')
            
            result = subprocess.run(validator_cmd, capture_output=False)
            if result.returncode != 0:
                print(f"\n⚠️  Validation completed with errors (exit code: {result.returncode})")
            else:
                print("\n✅ Validation completed successfully")
    
    finally:
        generator.cleanup()

if __name__ == '__main__':
    main()
