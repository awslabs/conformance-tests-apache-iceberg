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
PySpark implementation of TableGenerator
"""

import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))

import boto3

from backend.pyspark.builders import (
    create_spark_session,
    is_s3_table_warehouse,
    create_single_type_table,
    create_comprehensive_table,
    create_partitioned_table,
    create_table_worker,
    create_single_table,
    create_evolution_table,
    generate_evolution_tables
)
from iceberg_fuzzer.generator import TableGenerator


class PySparkGenerator(TableGenerator):
    """PySpark implementation of table generator"""

    def __init__(self, warehouse_path: str, specs_path: str):
        super().__init__(warehouse_path, specs_path)
        self.spark = None
        self._setup_warehouse()

    def _setup_warehouse(self):
        """Setup warehouse path and handle S3 Tables cleanup"""
        if not is_s3_table_warehouse(self.warehouse_path):
            path = Path(self.warehouse_path)
            path.mkdir(parents=True, exist_ok=True)
            self.warehouse_path = f"file://{path}"

    def generate(
        self,
        table_configs: list[dict],
        spec: dict,
        num_parallel: int = 1,
        delete_existing: bool = False,
    ) -> tuple[int, int]:
        """Generate tables from configs. Dispatches on spec['type']."""
        spec_type = spec['type']
        namespace = spec.get('metadata', {}).get('namespace', spec_type)

        if delete_existing:
            print(f"Deleting existing {spec_type} tables before generation...")
            self.delete_tables_by_namespace(namespace)

        total = len(table_configs)
        if total == 0:
            return (0, 0)

        self._ensure_spark()

        if spec_type == 'simple':
            return self._generate_simple(table_configs, num_parallel, delete_existing)
        elif spec_type == 'write_pattern':
            return self._generate_write_pattern(table_configs, num_parallel, delete_existing)
        elif spec_type == 'partitioned':
            return self._generate_partitioned(table_configs, num_parallel, delete_existing)
        elif spec_type == 'evolution':
            return self._generate_evolution(table_configs, num_parallel)
        elif spec_type == 'all':
            return self._generate_all(table_configs, num_parallel, delete_existing)
        else:
            raise NotImplementedError(f"PySpark backend does not support type '{spec_type}'")

    def _generate_simple(self, table_configs, num_parallel, delete_existing):
        total = len(table_configs)
        success_count = 0

        print(f"Generating {total} single-type tables with {num_parallel} parallel workers...")

        if num_parallel > 1:
            batch_size = 256
            with ThreadPoolExecutor(max_workers=num_parallel) as executor:
                for batch_start in range(0, total, batch_size):
                    batch = table_configs[batch_start:batch_start + batch_size]
                    futures = {
                        executor.submit(
                            create_table_worker, self.spark, self.warehouse_path, spec, delete_existing
                        ): spec
                        for spec in batch
                    }
                    for future in as_completed(futures):
                        table_name, success = future.result()
                        if success:
                            success_count += 1
                            print(f"✓ Created table {table_name} ({success_count}/{total})")
                        else:
                            print(f"✗ Failed to create table {table_name}")
        else:
            for i, spec in enumerate(table_configs, 1):
                success = create_single_type_table(
                    self.spark, self.warehouse_path, spec['name'], spec,
                    f"{i}/{total}", delete_existing
                )
                if success:
                    success_count += 1
                    print(f"✓ Created table {spec['name']} ({success_count}/{total})")
                else:
                    print(f"✗ Failed to create table {spec['name']}")

        print(f"\nCompleted single tables: {success_count}/{total} tables created successfully")
        return (success_count, total)

    def _generate_write_pattern(self, table_configs, num_parallel, delete_existing):
        total = len(table_configs)
        success_count = 0

        print(f"Generating {total} write-pattern tables with {num_parallel} parallel workers...")

        if num_parallel > 1:
            with ThreadPoolExecutor(max_workers=num_parallel) as executor:
                futures = {
                    executor.submit(
                        create_comprehensive_table, self.spark, self.warehouse_path,
                        cfg['name'], cfg['pattern'], delete_existing=delete_existing
                    ): cfg
                    for cfg in table_configs
                }
                for future in as_completed(futures):
                    cfg = futures[future]
                    try:
                        success = future.result()
                        if success:
                            success_count += 1
                            print(f"✓ Created {cfg['name']} ({success_count}/{total})")
                        else:
                            print(f"✗ Failed to create {cfg['name']}")
                    except Exception as e:
                        print(f"✗ Failed to create {cfg['name']}: {e}")
        else:
            for i, cfg in enumerate(table_configs, 1):
                try:
                    success = create_comprehensive_table(
                        self.spark, self.warehouse_path,
                        cfg['name'], cfg['pattern'], delete_existing=delete_existing
                    )
                    if success:
                        success_count += 1
                        print(f"✓ Created {cfg['name']} ({success_count}/{total})")
                    else:
                        print(f"✗ Failed to create {cfg['name']}")
                except Exception as e:
                    print(f"✗ Failed to create {cfg['name']}: {e}")

        print(f"\nCompleted write-pattern tables: {success_count}/{total} tables created successfully")
        return (success_count, total)

    def _generate_partitioned(self, table_configs, num_parallel, delete_existing):
        total = len(table_configs)
        success_count = 0

        print(f"Generating {total} partitioned tables with {num_parallel} parallel workers...")

        if num_parallel > 1:
            with ThreadPoolExecutor(max_workers=num_parallel) as executor:
                futures = {
                    executor.submit(
                        create_partitioned_table, self.spark, self.warehouse_path,
                        cfg['name'], cfg['pattern'], ",".join(cfg['transform']),
                        delete_existing=delete_existing
                    ): cfg
                    for cfg in table_configs
                }
                for future in as_completed(futures):
                    cfg = futures[future]
                    try:
                        success = future.result()
                        if success:
                            success_count += 1
                            print(f"✓ Created {cfg['name']} ({success_count}/{total})")
                        else:
                            print(f"✗ Failed {cfg['name']}")
                    except Exception as e:
                        print(f"✗ Failed {cfg['name']}: {e}")
        else:
            for i, cfg in enumerate(table_configs, 1):
                try:
                    success = create_partitioned_table(
                        self.spark, self.warehouse_path,
                        cfg['name'], cfg['pattern'], ",".join(cfg['transform']),
                        delete_existing=delete_existing
                    )
                    if success:
                        success_count += 1
                        print(f"✓ Created {cfg['name']} ({success_count}/{total})")
                    else:
                        print(f"✗ Failed {cfg['name']}")
                except Exception as e:
                    print(f"✗ Failed {cfg['name']}: {e}")

        print(f"\nCompleted: {success_count}/{total} partitioned tables created")
        return (success_count, total)

    def _generate_evolution(self, table_configs, num_parallel):
        total = len(table_configs)
        success_count = 0

        print(f"Generating {total} evolution tables...")

        for i, cfg in enumerate(table_configs, 1):
            table_name = cfg['name']
            print(f"Creating evolution table {i}/{total}: {table_name}")
            if create_evolution_table(self.spark, self.warehouse_path, table_name, cfg, delete_existing=True):
                success_count += 1

        print(f"\nCompleted evolution tables: {success_count}/{total} tables created successfully")
        return (success_count, total)

    def _generate_all(self, table_configs, num_parallel, delete_existing):
        """Generate mixed configs from the 'all' spec by grouping by _spec_type."""
        # Group configs by their source type
        groups = {}
        for cfg in table_configs:
            t = cfg.get('_spec_type', 'simple')
            groups.setdefault(t, []).append(cfg)

        total_success = 0
        total_count = 0

        for spec_type, cfgs in groups.items():
            print(f"\n{'='*60}")
            print(f"GENERATING {spec_type.upper()} TABLES")
            print(f"{'='*60}")

            if spec_type == 'simple':
                s, t = self._generate_simple(cfgs, num_parallel, delete_existing)
            elif spec_type == 'write_pattern':
                s, t = self._generate_write_pattern(cfgs, num_parallel, delete_existing)
            elif spec_type == 'partitioned':
                s, t = self._generate_partitioned(cfgs, num_parallel, delete_existing)
            else:
                print(f"Skipping unknown type: {spec_type}")
                continue

            total_success += s
            total_count += t

        return (total_success, total_count)

    def delete_all_tables(self, num_parallel: int = 1):
        """Delete all tables in the warehouse"""
        # For S3 Tables, try AWS SDK approach for faster deletion
        if self.warehouse_path.startswith("arn:aws:s3tables:"):
            return self._delete_s3_tables_fast(num_parallel)

        # For local warehouses, use Spark approach
        return self._delete_spark_tables(num_parallel)

    def delete_tables_by_namespace(self, namespace: str, num_parallel: int = 1):
        """Delete tables in specific namespace only"""
        if self.warehouse_path.startswith("arn:aws:s3tables:"):
            return self._delete_s3_tables_by_namespace(namespace, num_parallel)
        else:
            return self._delete_spark_tables_by_namespace(namespace, num_parallel)

    def _delete_s3_tables_fast(self, num_parallel: int):
        """Fast deletion for S3 Tables using AWS SDK"""
        try:

            # Parse S3 Tables ARN
            # arn:aws:s3tables:us-west-2:438336019674:bucket/test-bucket
            arn_parts = self.warehouse_path.split(':')
            region = arn_parts[3]

            # Create S3 Tables client
            print(f"Creating S3 Tables client for region {region}...")
            start_time = time.time()
            s3tables = boto3.client('s3tables', region_name=region)

            # List all tables
            print("Listing tables in S3 Tables bucket...")
            list_start = time.time()
            tables_to_delete = []

            try:
                paginator = s3tables.get_paginator('list_tables')
                for page in paginator.paginate(tableBucketARN=self.warehouse_path):
                    for table in page.get('tables', []):
                        tables_to_delete.append({
                            'name': table['name'],
                            'namespace': table['namespace'][0] if table.get('namespace') else 'default'
                        })

                list_time = time.time() - list_start
                print(f"Listed {len(tables_to_delete)} tables in {list_time:.2f}s")

            except Exception as e:
                print(f"Failed to list S3 Tables: {e}")
                return self._delete_spark_tables(num_parallel)

            if not tables_to_delete:
                print("No tables found to delete.")
                return None

            def delete_s3_table(table_info):
                delete_start = time.time()
                try:
                    s3tables.delete_table(
                        tableBucketARN=self.warehouse_path,
                        namespace=table_info['namespace'],
                        name=table_info['name']
                    )
                    delete_time = time.time() - delete_start
                    return f"✓ Deleted: {table_info['namespace']}.{table_info['name']} ({delete_time:.2f}s)"
                except Exception as e:
                    delete_time = time.time() - delete_start
                    return (
                        f"✗ Failed to delete {table_info['namespace']}.{table_info['name']} "
                        f"({delete_time:.2f}s): {e}"
                    )

            # Delete in parallel using AWS SDK
            print(f"Starting parallel deletion with {min(num_parallel, 20)} workers...")
            delete_start = time.time()
            success_count = 0

            with ThreadPoolExecutor(max_workers=min(num_parallel, 20)) as executor:  # Reduce to 20 for AWS limits
                future_to_table = {executor.submit(delete_s3_table, table): table for table in tables_to_delete}

                for future in as_completed(future_to_table):
                    result = future.result()
                    print(result)
                    if result.startswith("✓"):
                        success_count += 1

            total_time = time.time() - start_time
            delete_time = time.time() - delete_start
            print(f"\nCompleted: {success_count}/{len(tables_to_delete)} tables deleted successfully")
            print(f"Total time: {total_time:.2f}s, Deletion time: {delete_time:.2f}s")
            print(f"Average per table: {delete_time/len(tables_to_delete):.2f}s")
            return None

        except ImportError:
            print("boto3 not available, falling back to Spark deletion...")
            return self._delete_spark_tables(num_parallel)
        except Exception as e:
            print(f"S3 Tables deletion failed: {e}, falling back to Spark deletion...")
            return self._delete_spark_tables(num_parallel)

    def _delete_s3_tables_by_namespace(self, namespace: str, num_parallel: int):
        """Delete tables in specific namespace using AWS SDK"""
        try:

            arn_parts = self.warehouse_path.split(':')
            region = arn_parts[3]
            s3tables = boto3.client('s3tables', region_name=region)

            # List tables in specific namespace
            tables_to_delete = []
            try:
                paginator = s3tables.get_paginator('list_tables')
                for page in paginator.paginate(tableBucketARN=self.warehouse_path):
                    for table in page.get('tables', []):
                        table_namespace = table['namespace'][0] if table.get('namespace') else 'default'
                        if table_namespace == namespace:
                            tables_to_delete.append({
                                'name': table['name'],
                                'namespace': table_namespace
                            })

                print(f"Found {len(tables_to_delete)} tables in namespace '{namespace}' to delete")

            except Exception as e:
                print(f"Failed to list S3 Tables in namespace {namespace}: {e}")
                return self._delete_spark_tables_by_namespace(namespace, num_parallel)

            if not tables_to_delete:
                print(f"No tables found in namespace '{namespace}' to delete.")
                return None

            def delete_s3_table(table_info):
                try:
                    s3tables.delete_table(
                        tableBucketARN=self.warehouse_path,
                        namespace=table_info['namespace'],
                        name=table_info['name']
                    )
                    return f"✓ Deleted: {table_info['namespace']}.{table_info['name']}"
                except Exception as e:
                    return f"✗ Failed to delete {table_info['namespace']}.{table_info['name']}: {e}"

            success_count = 0
            with ThreadPoolExecutor(max_workers=min(num_parallel, 20)) as executor:
                future_to_table = {executor.submit(delete_s3_table, table): table for table in tables_to_delete}

                for future in as_completed(future_to_table):
                    result = future.result()
                    print(result)
                    if result.startswith("✓"):
                        success_count += 1

            print(f"Deleted {success_count}/{len(tables_to_delete)} tables from namespace '{namespace}'")
            return None

        except Exception as e:
            print(f"S3 Tables namespace deletion failed: {e}, falling back to Spark...")
            return self._delete_spark_tables_by_namespace(namespace, num_parallel)

    def _delete_spark_tables(self, num_parallel: int):
        """Original Spark-based deletion method"""
        self._ensure_spark()

        # Collect all table names first
        tables_to_delete = []

        try:
            # Get all catalogs and namespaces
            catalogs = self.spark.sql("SHOW CATALOGS").collect()

            for catalog_row in catalogs:
                catalog_name = catalog_row['catalog']
                if catalog_name in ['spark_catalog', 'system']:
                    continue  # Skip system catalogs

                try:
                    # Get all namespaces in this catalog
                    namespaces = self.spark.sql(f"SHOW NAMESPACES IN {catalog_name}").collect()

                    for namespace_row in namespaces:
                        namespace = namespace_row['namespace']

                        try:
                            # Get all tables in this namespace
                            tables = self.spark.sql(f"SHOW TABLES IN {catalog_name}.{namespace}").collect()

                            for table_row in tables:
                                table_name = table_row['tableName']
                                full_table_name = f"{catalog_name}.{namespace}.{table_name}"
                                tables_to_delete.append(full_table_name)

                        except Exception as e:
                            print(f"Warning: Could not list tables in {catalog_name}.{namespace}: {e}")

                except Exception as e:
                    print(f"Warning: Could not list namespaces in {catalog_name}: {e}")

        except Exception as e:
            print(f"Warning: Could not list catalogs: {e}")
            return

        if not tables_to_delete:
            print("No tables found to delete.")
            return

        print(f"Found {len(tables_to_delete)} tables to delete...")

        # Delete tables in parallel
        if num_parallel > 1:

            # Batch tables for more efficient deletion
            batch_size = max(1, len(tables_to_delete) // num_parallel)
            table_batches = [tables_to_delete[i:i + batch_size] for i in range(0, len(tables_to_delete), batch_size)]

            def delete_table_batch(table_batch):
                # Create separate Spark session for each worker
                worker_spark = None
                deleted_count = 0
                try:
                    worker_spark = create_spark_session(self.warehouse_path)

                    for table_name in table_batch:
                        try:
                            # Use PURGE for S3 Tables
                            if self.warehouse_path.startswith("arn:aws:s3tables:"):
                                worker_spark.sql(f"DROP TABLE IF EXISTS {table_name} PURGE")
                            else:
                                worker_spark.sql(f"DROP TABLE IF EXISTS {table_name}")
                            deleted_count += 1
                            print(f"✓ Deleted: {table_name}")
                        except Exception as e:
                            print(f"✗ Failed to delete {table_name}: {e}")

                    return deleted_count
                except Exception as e:
                    print(f"✗ Batch deletion failed: {e}")
                    return deleted_count
                finally:
                    if worker_spark:
                        worker_spark.stop()

            success_count = 0
            with ThreadPoolExecutor(max_workers=num_parallel) as executor:
                future_to_batch = {executor.submit(delete_table_batch, batch): batch for batch in table_batches}

                for future in as_completed(future_to_batch):
                    batch_success = future.result()
                    success_count += batch_success

            print(f"\nCompleted: {success_count}/{len(tables_to_delete)} tables deleted successfully")

        else:
            # Sequential deletion
            success_count = 0
            for table_name in tables_to_delete:
                try:
                    print(f"Deleting table: {table_name}")
                    # Use PURGE for S3 Tables
                    if self.warehouse_path.startswith("arn:aws:s3tables:"):
                        self.spark.sql(f"DROP TABLE IF EXISTS {table_name} PURGE")
                    else:
                        self.spark.sql(f"DROP TABLE IF EXISTS {table_name}")
                    success_count += 1
                except Exception as e:
                    print(f"Warning: Could not delete table {table_name}: {e}")

            print(f"\nCompleted: {success_count}/{len(tables_to_delete)} tables deleted successfully")

        # Clean up Spark session
        if self.spark:
            self.spark.stop()
            self.spark = None

    def _delete_spark_tables_by_namespace(self, namespace: str, num_parallel: int):
        """Delete tables in specific namespace using Spark"""
        self._ensure_spark()

        tables_to_delete = []

        try:
            catalogs = self.spark.sql("SHOW CATALOGS").collect()

            for catalog_row in catalogs:
                catalog_name = catalog_row['catalog']
                if catalog_name in ['spark_catalog', 'system']:
                    continue

                try:
                    # Check if namespace exists in this catalog
                    namespaces = self.spark.sql(f"SHOW NAMESPACES IN {catalog_name}").collect()
                    namespace_exists = any(ns['namespace'] == namespace for ns in namespaces)

                    if namespace_exists:
                        # Get tables in this specific namespace
                        tables = self.spark.sql(f"SHOW TABLES IN {catalog_name}.{namespace}").collect()

                        for table_row in tables:
                            table_name = table_row['tableName']
                            full_table_name = f"{catalog_name}.{namespace}.{table_name}"
                            tables_to_delete.append(full_table_name)

                except Exception as e:
                    print(f"Warning: Could not check namespace {namespace} in {catalog_name}: {e}")

        except Exception as e:
            print(f"Warning: Could not list catalogs: {e}")
            return

        if not tables_to_delete:
            print(f"No tables found in namespace '{namespace}' to delete.")
            return

        print(f"Found {len(tables_to_delete)} tables in namespace '{namespace}' to delete...")

        # Delete tables (reuse existing parallel logic)
        if num_parallel > 1:

            batch_size = max(1, len(tables_to_delete) // num_parallel)
            table_batches = [tables_to_delete[i:i + batch_size] for i in range(0, len(tables_to_delete), batch_size)]

            def delete_table_batch(table_batch):
                worker_spark = None
                deleted_count = 0
                try:
                    worker_spark = create_spark_session(self.warehouse_path)

                    for table_name in table_batch:
                        try:
                            if self.warehouse_path.startswith("arn:aws:s3tables:"):
                                worker_spark.sql(f"DROP TABLE IF EXISTS {table_name} PURGE")
                            else:
                                worker_spark.sql(f"DROP TABLE IF EXISTS {table_name}")
                            deleted_count += 1
                            print(f"✓ Deleted: {table_name}")
                        except Exception as e:
                            print(f"✗ Failed to delete {table_name}: {e}")

                    return deleted_count
                except Exception as e:
                    print(f"✗ Batch deletion failed: {e}")
                    return deleted_count
                finally:
                    if worker_spark:
                        worker_spark.stop()

            success_count = 0
            with ThreadPoolExecutor(max_workers=num_parallel) as executor:
                future_to_batch = {executor.submit(delete_table_batch, batch): batch for batch in table_batches}

                for future in as_completed(future_to_batch):
                    batch_success = future.result()
                    success_count += batch_success

            print(f"Deleted {success_count}/{len(tables_to_delete)} tables from namespace '{namespace}'")

        else:
            # Sequential deletion
            success_count = 0
            for table_name in tables_to_delete:
                try:
                    if self.warehouse_path.startswith("arn:aws:s3tables:"):
                        self.spark.sql(f"DROP TABLE IF EXISTS {table_name} PURGE")
                    else:
                        self.spark.sql(f"DROP TABLE IF EXISTS {table_name}")
                    success_count += 1
                    print(f"✓ Deleted: {table_name}")
                except Exception as e:
                    print(f"✗ Failed to delete {table_name}: {e}")

            print(f"Deleted {success_count}/{len(tables_to_delete)} tables from namespace '{namespace}'")

    def _ensure_spark(self):
        """Create Spark session if not already created"""
        if self.spark is None:
            self.spark = create_spark_session(self.warehouse_path)

    def cleanup(self):
        """Cleanup Spark session"""
        if self.spark is not None:
            self.spark.stop()
            self.spark = None
