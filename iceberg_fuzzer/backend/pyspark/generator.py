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
import yaml

from backend.pyspark.builders import (
    create_spark_session,
    is_s3_table_warehouse,
    get_catalog_name,
    create_single_type_table,
    create_comprehensive_table,
    create_partitioned_table,
    create_table_worker,
    create_single_table,
    generate_evolution_tables
)
from iceberg_fuzzer.common.partition_transforms import partitioned_table_specs
from iceberg_fuzzer.generator import TableGenerator
from iceberg_fuzzer.table_spec import load_table_specs


class PySparkGenerator(TableGenerator):
    """PySpark implementation of table generator"""

    def __init__(self, warehouse_path: str, specs_path: str, seed: int | None = None):
        super().__init__(warehouse_path, specs_path, seed=seed)
        self.spark = None
        self._setup_warehouse()

    def _setup_warehouse(self):
        """Setup warehouse path and handle S3 Tables cleanup"""
        if not is_s3_table_warehouse(self.warehouse_path):
            path = Path(self.warehouse_path)
            path.mkdir(parents=True, exist_ok=True)
            self.warehouse_path = f"file://{path}"

    def supports_s3_tables(self) -> bool:
        return True

    def supports_partitioned_tables(self) -> bool:
        return True

    def supports_minimal_mode(self) -> bool:
        return True

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
            # arn:aws:s3tables:us-west-2:123456789012:bucket/test-bucket
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

    def _configured_catalog(self) -> str:
        """Return the Iceberg catalog name this generator reads and writes.

        `SHOW CATALOGS` cannot be used to discover it. Spark only lists catalogs
        it has already initialised, and the Iceberg catalog is configured
        lazily, so it is absent from that listing until some query touches it.
        """
        return get_catalog_name(self.warehouse_path)

    def _delete_spark_tables(self, num_parallel: int):
        """Original Spark-based deletion method"""
        self._ensure_spark()

        # Collect all table names first
        tables_to_delete = []
        catalog_name = self._configured_catalog()

        try:
            # Get all namespaces in the configured catalog
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
        catalog_name = self._configured_catalog()

        try:
            # Check if namespace exists in the configured catalog
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

    def generate_simple_tables(
        self,
        num_parallel: int = 1,
        minimal: bool = False,
        random_count: int = None,
        delete_existing: bool = False
    ) -> bool:
        """Generate simple single-type tables.

        Returns:
            True if every table in the set was created, False if any failed.
        """
        if delete_existing:
            print("Deleting existing simple tables before generation...")
            self.delete_tables_by_namespace("simple")

        table_specs = load_table_specs(self.specs_path, minimal=minimal)
        mode_str = "minimal (uncompressed, plain encoding)" if minimal else "full"

        # Apply random selection if specified
        table_specs = self._random_select_tables(table_specs, random_count)

        print(f"Generating {len(table_specs)} single-type tables ({mode_str}) with {num_parallel} parallel workers...")
        print("\n📊 PySpark Backend Support Status:")
        print("  ✅ Supported: All data types (primitives, lists, maps, structs)")
        print("  ✅ Supported: All compressions and encodings")
        print("  ✅ Supported: Both Iceberg v1 and v2")
        print(f"  📈 Expected success rate: 100% ({len(table_specs)}/{len(table_specs)} tables)\n")

        success_count = 0
        total_count = len(table_specs)

        if num_parallel > 1:
            self._ensure_spark()
            batch_size = 256
            with ThreadPoolExecutor(max_workers=num_parallel) as executor:
                for batch_start in range(0, len(table_specs), batch_size):
                    batch_end = min(batch_start + batch_size, len(table_specs))
                    batch = table_specs[batch_start:batch_end]

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
                            print(f"✓ Created table {table_name} ({success_count}/{total_count})")
                        else:
                            print(f"✗ Failed to create table {table_name}")
        else:
            self._ensure_spark()
            for i, spec in enumerate(table_specs, 1):
                success = create_single_type_table(
                    self.spark, self.warehouse_path, spec['name'], spec,
                    f"{i}/{total_count}", delete_existing
                )
                if success:
                    success_count += 1
                    print(f"✓ Created table {spec['name']} ({success_count}/{total_count})")
                else:
                    print(f"✗ Failed to create table {spec['name']}")

        print(f"\nCompleted single tables: {success_count}/{total_count} tables created successfully")
        return success_count == total_count

    def generate_single_table(self, table_name: str, delete_existing: bool = False) -> bool:
        """Generate a single table by name.

        Returns:
            True if the table was created, False if the name matched no spec
            or creation failed.
        """

        # Ensure Spark session is initialized
        if not self.spark:
            self.spark = create_spark_session(self.warehouse_path)

        if delete_existing:
            print("Deleting existing tables before generation...")
            self.delete_all_tables()

        print(f"Generating single table: {table_name}")
        success = create_single_table(self.spark, self.warehouse_path, table_name, delete_existing=delete_existing)

        if success:
            print(f"✓ Successfully created table: {table_name}")
        else:
            print(f"✗ Failed to create table: {table_name}")

        return success


    def generate_write_pattern_tables(
        self,
        num_parallel: int = 1,
        random_count: int = None,
        delete_existing: bool = False
    ) -> bool:
        """Generate write-pattern tables.

        Returns:
            True if every table in the set was created, False if any failed.
        """
        if delete_existing:
            print("Deleting existing write-pattern tables before generation...")
            self.delete_tables_by_namespace("write_pattern")

        all_patterns = ["insert_heavy", "update_heavy", "delete_heavy", "mixed_chaos",
                       "batch_replace", "gradual_growth", "burst_ops", "truncate_cycles"]
        versions = ["v1", "v2"]
        delete_modes = ["cow", "posdelete"]  # Remove eqdelete - PySpark cannot create equality deletes

        # Build table specs
        table_specs = []
        for pattern in all_patterns:
            for version in versions:
                for delete_mode in delete_modes:
                    # Skip invalid combinations: v1 only supports cow
                    if version == "v1" and delete_mode != "cow":
                        continue

                    table_name = f"unpart_{pattern}_zstd_plain_dictionary_{version}_{delete_mode}"
                    table_specs.append({
                        'name': table_name,
                        'pattern': pattern,
                        'version': version,
                        'delete_mode': delete_mode
                    })

        # Apply random selection if specified
        table_specs = self._random_select_tables(table_specs, random_count)

        print(f"Generating {len(table_specs)} write-pattern tables with {num_parallel} parallel workers...")

        success_count = 0
        total_count = len(table_specs)

        self._ensure_spark()

        if num_parallel > 1:
            with ThreadPoolExecutor(max_workers=num_parallel) as executor:
                futures = {
                    executor.submit(create_comprehensive_table, self.spark, self.warehouse_path,
                                  spec['name'], spec['pattern'], delete_existing=delete_existing): spec
                    for spec in table_specs
                }
                for future in as_completed(futures):
                    spec = futures[future]
                    try:
                        success = future.result()
                        if success:
                            success_count += 1
                            print(f"✓ Created {spec['name']} ({success_count}/{total_count})")
                        else:
                            print(f"✗ Failed to create {spec['name']}")
                    except Exception as e:
                        print(f"✗ Failed to create {spec['name']}: {e}")
        else:
            for i, spec in enumerate(table_specs, 1):
                try:
                    success = create_comprehensive_table(self.spark, self.warehouse_path,
                                                       spec['name'], spec['pattern'], delete_existing=delete_existing)
                    if success:
                        success_count += 1
                        print(f"✓ Created {spec['name']} ({success_count}/{total_count})")
                    else:
                        print(f"✗ Failed to create {spec['name']}")
                except Exception as e:
                    print(f"✗ Failed to create {spec['name']}: {e}")

        print(f"\nCompleted write-pattern tables: {success_count}/{total_count} tables created successfully")
        return success_count == total_count

    def generate_partitioned_tables(
        self,
        num_parallel: int = 1,
        random_count: int = None,
        minimal: bool = False,
        delete_existing: bool = False
    ) -> bool:
        """Generate partitioned tables.

        Returns:
            True if every table in the set was created, False if any failed.
        """
        if delete_existing:
            print("Deleting existing partitioned tables before generation...")
            self.delete_tables_by_namespace("partitioned")

        table_specs = partitioned_table_specs(minimal=minimal)

        # Apply random selection if specified
        table_specs = self._random_select_tables(table_specs, random_count)

        print(
            f"Generating {len(table_specs)} partitioned tables "
            f"with {num_parallel} parallel workers..."
        )

        success_count = 0
        total_count = len(table_specs)

        if num_parallel > 1:
            self._ensure_spark()
            with ThreadPoolExecutor(max_workers=num_parallel) as executor:
                futures = {
                    executor.submit(
                        create_partitioned_table, self.spark, self.warehouse_path,
                        spec['name'], spec['pattern'], ",".join(spec['transform']),
                        delete_existing=delete_existing
                    ): spec
                    for spec in table_specs
                }
                for future in as_completed(futures):
                    spec = futures[future]
                    try:
                        success = future.result()
                        if success:
                            success_count += 1
                            print(f"✓ Created {spec['name']} ({success_count}/{total_count})")
                        else:
                            print(f"✗ Failed {spec['name']}")
                    except Exception as e:
                        print(f"✗ Failed {spec['name']}: {e}")
        else:
            self._ensure_spark()
            for i, spec in enumerate(table_specs, 1):
                try:
                    success = create_partitioned_table(
                        self.spark, self.warehouse_path,
                        spec['name'], spec['pattern'], ",".join(spec['transform']),
                        delete_existing=delete_existing
                    )
                    if success:
                        success_count += 1
                        print(f"✓ Created {spec['name']} ({success_count}/{total_count})")
                    else:
                        print(f"✗ Failed {spec['name']}")
                except Exception as e:
                    print(f"✗ Failed {spec['name']}: {e}")

        print(f"\nCompleted: {success_count}/{total_count} partitioned tables created")
        return success_count == total_count

    def generate_minimal_tables(self, num_parallel: int = 1, random_count: int = None) -> bool:
        """Generate minimal set of tables.

        Returns:
            True if every table in the set was created, False if any failed.
        """
        table_specs = load_table_specs(self.specs_path, minimal=True)

        # Apply random selection if specified
        table_specs = self._random_select_tables(table_specs, random_count)
        print(f"Generating {len(table_specs)} minimal tables with {num_parallel} parallel workers...")

        success_count = 0
        total_count = len(table_specs)

        if num_parallel > 1:
            self._ensure_spark()
            with ThreadPoolExecutor(max_workers=num_parallel) as executor:
                futures = {
                    executor.submit(create_table_worker, self.spark, self.warehouse_path, spec): spec
                    for spec in table_specs
                }
                for future in as_completed(futures):
                    table_name, success = future.result()
                    if success:
                        success_count += 1
                        print(f"✓ Created table {table_name} ({success_count}/{total_count})")
                    else:
                        print(f"✗ Failed to create table {table_name}")
        else:
            self._ensure_spark()
            for i, spec in enumerate(table_specs, 1):
                success = create_single_type_table(
                    self.spark, self.warehouse_path, spec['name'], spec, f"{i}/{total_count}"
                )
                if success:
                    success_count += 1
                    print(f"✓ Created table {spec['name']} ({success_count}/{total_count})")
                else:
                    print(f"✗ Failed to create table {spec['name']}")

        print(f"\nCompleted minimal tables: {success_count}/{total_count} tables created successfully")
        return success_count == total_count

    def cleanup(self):
        """Cleanup Spark session"""
        if self.spark is not None:
            self.spark.stop()
            self.spark = None

    def generate_evolution_tables(
        self, num_parallel: int = 1, random_count: int = None, delete_existing: bool = False
    ) -> bool:
        """Generate evolution tables for testing Iceberg evolution features.

        Returns:
            True if every table in the set was created, False if any failed.
        """
        if delete_existing:
            print("Deleting existing evolution tables before generation...")
            self.delete_tables_by_namespace("evolution")

        with open(self.specs_path, 'r') as f:
            specs = yaml.safe_load(f)
        table_specs = self._random_select_tables(
            specs.get('evolution_tables', []), random_count
        )

        self._ensure_spark()
        return generate_evolution_tables(self.spark, self.warehouse_path, table_specs)
