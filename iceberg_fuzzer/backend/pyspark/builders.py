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
KONA - Iceberg Table Fuzzer
PySpark Iceberg table generation implementation
"""

import heapq
import logging
import os
import random
import time
from contextlib import contextmanager
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date
from decimal import Decimal
import boto3
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, date_add
from pyspark.sql.types import (
    StructType, StructField, BooleanType, IntegerType, LongType,
    FloatType, DoubleType, DecimalType, DateType, TimestampType,
    StringType, BinaryType, ArrayType, MapType, TimestampNTZType
)

from iceberg_fuzzer.table_spec import load_table_specs
from iceberg_fuzzer.common import iceberg_compression_codec, row_id_from_index


@contextmanager
def _suppress_jvm_output():
    """Temporarily redirect OS-level stdout/stderr to /dev/null.

    Spark's JVM (Ivy dependency resolution, Hadoop filesystem init, log4j)
    writes directly to file descriptors 1 and 2, bypassing Python's
    sys.stdout/sys.stderr. Python-level logging configuration cannot silence
    this output. The only reliable way to suppress it is to redirect the
    underlying OS file descriptors for the duration of the noisy operation.
    """
    devnull = os.open(os.devnull, os.O_WRONLY)
    saved_stdout = os.dup(1)
    saved_stderr = os.dup(2)
    os.dup2(devnull, 1)
    os.dup2(devnull, 2)
    try:
        yield
    finally:
        os.dup2(saved_stdout, 1)
        os.dup2(saved_stderr, 2)
        os.close(saved_stdout)
        os.close(saved_stderr)
        os.close(devnull)


def create_spark_session(warehouse_path, app_name=None):
    """Create Spark session with Iceberg support and performance optimizations"""
    if app_name is None:
        app_name = f"IcebergTableGenerator-{uuid.uuid4()}"

    # Detect if warehouse_path is an S3 table ARN
    is_s3_table = warehouse_path.startswith('arn:aws:s3tables:')

    builder = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.ui.enabled", "false") \
        .config("spark.ui.showConsoleProgress", "false") \
        .config("spark.eventLog.enabled", "false") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1") \
        .config("spark.jars.ivy", "/tmp/.ivy2") \
        .config("spark.driver.host", "localhost") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryo.unsafe", "true") \
        .config("spark.kryo.registrationRequired", "false") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
        .config("spark.sql.cbo.enabled", "false") \
        .config("spark.driver.memory", "16g") \
        .config("spark.driver.memoryFraction", "0.8") \
        .config("spark.driver.maxResultSize", "8g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.executor.memoryFraction", "0.8") \
        .config(
            "spark.driver.extraJavaOptions",
            "-XX:+UseG1GC -XX:MaxGCPauseMillis=200 "
            "-XX:+UnlockExperimentalVMOptions -XX:+UseJVMCICompiler -Xmx16g"
        ) \
        .config(
            "spark.executor.extraJavaOptions",
            "-XX:+UseG1GC -XX:MaxGCPauseMillis=200 "
            "-XX:+UnlockExperimentalVMOptions -XX:+UseJVMCICompiler"
        ) \
        .config("spark.cleaner.periodicGC.interval", "30s") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.parquet.enableVectorizedReader", "true") \
        .config("spark.sql.parquet.enableVectorizedWriter", "true") \
        .config("spark.sql.files.maxPartitionBytes", "128MB") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.default.parallelism", "8") \
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "10000") \
        .config("spark.sql.codegen.wholeStage", "true") \
        .config("spark.sql.codegen.factoryMode", "CODEGEN_ONLY") \
        .config("spark.sql.iceberg.delete.rewrite-data-files-threshold", "1") \
        .config("spark.sql.iceberg.delete.rewrite-delete-files-threshold", "1000000") \
        .config("spark.sql.iceberg.merge.rewrite-data-files-threshold", "1") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .config("spark.sql.hive.metastore.warehouse.dir", "/tmp/spark-warehouse") \
        .config("spark.driver.extraJavaOptions",
                "-XX:+UseG1GC -XX:+UnlockExperimentalVMOptions -XX:+UseCompressedOops "
                "-XX:+UseCompressedClassPointers -XX:+TieredCompilation -XX:TieredStopAtLevel=4 "
                "-XX:+AggressiveOpts -XX:+OptimizeStringConcat -XX:+UseStringDeduplication")

    if is_s3_table:
        # Configure for S3 Tables using REST catalog (based on official docs)
        # Extract region from ARN: arn:aws:s3tables:region:account:bucket/bucket-name
        arn_parts = warehouse_path.split(':')
        region = arn_parts[3]

        builder = builder \
            .config("spark.sql.catalog.s3tables", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.s3tables.type", "rest") \
            .config("spark.sql.catalog.s3tables.uri", f"https://s3tables.{region}.amazonaws.com/iceberg") \
            .config("spark.sql.catalog.s3tables.warehouse", warehouse_path) \
            .config("spark.sql.catalog.s3tables.rest.sigv4-enabled", "true") \
            .config("spark.sql.catalog.s3tables.rest.signing-name", "s3tables") \
            .config("spark.sql.catalog.s3tables.rest.signing-region", region) \
            .config("spark.sql.catalog.s3tables.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
            .config("spark.sql.catalog.s3tables.client.region", region) \
            .config("spark.sql.catalog.s3tables.rest-metrics-reporting-enabled", "false") \
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialProvider"
            ) \
            .config(
                "spark.jars.packages",
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1,"
                "software.amazon.awssdk:bundle:2.28.11,"
                "software.amazon.awssdk:url-connection-client:2.28.11"
            )
    else:
        # Configure for local/HDFS with comprehensive file system settings
        # Ensure warehouse directory exists with proper permissions
        os.makedirs(warehouse_path, mode=0o755, exist_ok=True)

        builder = builder \
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.local.type", "hadoop") \
            .config("spark.sql.catalog.local.warehouse", warehouse_path) \
            .config("spark.sql.iceberg.distribution-mode", "hash") \
            .config("spark.hadoop.fs.defaultFS", "file:///") \
            .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
            .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
            .config("spark.hadoop.dfs.permissions.enabled", "false") \
            .config("spark.hadoop.fs.permissions.umask-mode", "000") \
            .config("spark.hadoop.hadoop.tmp.dir", "/tmp/hadoop-tmp") \
            .config("spark.hadoop.fs.automatic.close", "false")

    # Silence py4j's Python-level logger (the Python↔JVM bridge)
    logging.getLogger("py4j").setLevel(logging.ERROR)
    # Env var read by Spark when configuring log4j inside the JVM at boot
    os.environ.setdefault("SPARK_LOG_LEVEL", "ERROR")

    # During getOrCreate(), Ivy/Hadoop/log4j write directly to OS fds 1 & 2
    # before any runtime config can take effect — the context manager hides that burst.
    with _suppress_jvm_output():
        spark = builder.getOrCreate()

    return spark

def delete_all_s3_tables(warehouse_path):
    """Delete all tables from S3 table bucket before proceeding"""
    if not is_s3_table_warehouse(warehouse_path):
        return

    print("🗑️  Deleting all existing tables from S3 table bucket...")

    try:

        # Extract region from ARN
        # Format: arn:aws:s3tables:region:account:bucket/bucket-name
        arn_parts = warehouse_path.split(':')
        region = arn_parts[3]

        # Use AWS credentials from environment
        session = boto3.Session(
            aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
            aws_session_token=os.environ.get('AWS_SESSION_TOKEN'),
            region_name=region
        )

        s3tables_client = session.client('s3tables')

        # List all namespaces
        namespaces_response = s3tables_client.list_namespaces(tableBucketARN=warehouse_path)

        total_deleted = 0
        for namespace in namespaces_response.get('namespaces', []):
            # Extract namespace name from the response structure
            namespace_name = namespace['namespace'][0]  # namespace is a list with one element
            print(f"  → Checking namespace: {namespace_name}")

            # List all tables in this namespace
            try:
                tables_response = s3tables_client.list_tables(
                    tableBucketARN=warehouse_path,
                    namespace=namespace_name
                )

                # Delete each table
                for table in tables_response.get('tables', []):
                    table_name = table['name']
                    try:
                        s3tables_client.delete_table(
                            tableBucketARN=warehouse_path,
                            namespace=namespace_name,
                            name=table_name
                        )
                        print(f"    ✓ Deleted table: {namespace_name}.{table_name}")
                        total_deleted += 1
                    except Exception as e:
                        print(f"    ✗ Failed to delete {namespace_name}.{table_name}: {e}")

            except Exception as e:
                print(f"    ✗ Failed to list tables in namespace {namespace_name}: {e}")

        print(f"🗑️  Deleted {total_deleted} tables from S3 table bucket")

    except Exception:
        pass  # Ignore errors in S3 Tables deletion

def is_s3_table_warehouse(warehouse_path):
    """Check if warehouse path is an S3 table ARN"""
    return warehouse_path.startswith('arn:aws:s3tables:')

def get_catalog_name(warehouse_path):
    """Get the Spark catalog name configured for this warehouse type.

    Single source of truth for the catalog name, so that code reading tables
    addresses the same catalog the writers created them in.
    """
    return "s3tables" if is_s3_table_warehouse(warehouse_path) else "local"

def get_catalog_and_namespace(warehouse_path, table_type):
    """Get catalog name and namespace based on warehouse type and table type"""
    # The namespace is the table type verbatim (simple, write_pattern,
    # partitioned, evolution) for every warehouse type.
    return get_catalog_name(warehouse_path), table_type

def create_table_safe(spark, catalog, namespace, table_name, schema_fields, props_sql, partition_clause=""):
    """Create table with IF NOT EXISTS to handle race conditions in S3 Tables"""
    try:
        create_sql = (
            f"CREATE TABLE IF NOT EXISTS {catalog}.{namespace}.{table_name} "
            f"({schema_fields}) USING iceberg {partition_clause} "
            f"TBLPROPERTIES ({props_sql})"
        )
        spark.sql(create_sql)
        return True
    except Exception as e:
        # If IF NOT EXISTS fails, try regular CREATE (for older Spark versions)
        try:
            create_sql = (
                f"CREATE TABLE {catalog}.{namespace}.{table_name} "
                f"({schema_fields}) USING iceberg {partition_clause} "
                f"TBLPROPERTIES ({props_sql})"
            )
            spark.sql(create_sql)
            return True
        except Exception as e2:
            print(f"    → Failed to create table: {e2}")
            return False

def table_exists(spark, catalog, namespace, table_name):
    """Check if table exists"""
    try:
        spark.sql(f"DESCRIBE TABLE {catalog}.{namespace}.{table_name}")
        return True
    except Exception:
        return False

def drop_table_if_exists(spark, catalog, namespace, table_name):
    """Drop table if it exists"""
    try:
        # Check if this is an S3 Tables warehouse
        warehouse_path = spark.conf.get("spark.sql.catalog.s3tables.warehouse", "")
        if catalog == "s3tables" or warehouse_path.startswith("arn:aws:s3tables:"):
            spark.sql(f"DROP TABLE IF EXISTS {catalog}.{namespace}.{table_name} PURGE")
        else:
            spark.sql(f"DROP TABLE IF EXISTS {catalog}.{namespace}.{table_name}")
        print(f"    → Dropped existing table {catalog}.{namespace}.{table_name}")
    except Exception as e:
        print(f"    → Could not drop table (may not exist): {e}")

def disable_table_compaction(spark, catalog, namespace, table_name, warehouse_path=None):
    """Disable compaction on the table"""
    print(f"    → disable_table_compaction called: catalog={catalog}, warehouse_path={warehouse_path}")
    try:
        # For S3 Tables, use AWS SDK to disable maintenance operations
        if catalog == "s3tables" and warehouse_path and is_s3_table_warehouse(warehouse_path):
            print(f"    → Using S3 Tables SDK for {catalog}.{namespace}.{table_name}")
            disable_s3_table_maintenance(warehouse_path, namespace, table_name)
        else:
            print(f"    → Using table properties for {catalog}.{namespace}.{table_name}")
            # For local tables, use table properties
            spark.sql(
                f"ALTER TABLE {catalog}.{namespace}.{table_name} "
                f"SET TBLPROPERTIES ('write.target-file-size-bytes'='268435456')"
            )
            spark.sql(
                f"ALTER TABLE {catalog}.{namespace}.{table_name} "
                f"SET TBLPROPERTIES ('write.delete.target-file-size-bytes'='268435456')"
            )
            spark.sql(
                f"ALTER TABLE {catalog}.{namespace}.{table_name} "
                f"SET TBLPROPERTIES ('write.merge.target-file-size-bytes'='268435456')"
            )
        print(f"    → Disabled compaction for {catalog}.{namespace}.{table_name}")
    except Exception as e:
        print(f"    → Failed to disable compaction: {e}")

def disable_s3_table_maintenance(warehouse_path, namespace, table_name):
    """Disable maintenance operations for S3 Tables using AWS SDK"""
    try:

        # Parse S3 Tables ARN
        arn_parts = warehouse_path.split(':')
        region = arn_parts[3]

        print(f"    → Calling S3 Tables SDK to disable maintenance for {namespace}.{table_name} in region {region}")

        # Create S3 Tables client
        s3tables = boto3.client('s3tables', region_name=region)

        # Wait a moment for table to be fully created
        time.sleep(2)

        # Disable iceberg compaction
        try:
            s3tables.put_table_maintenance_configuration(
                tableBucketARN=warehouse_path,
                namespace=namespace,
                name=table_name,
                type='icebergCompaction',
                value={
                    'status': 'disabled',
                    'settings': {
                        'icebergCompaction': {
                            'targetFileSizeMB': 512,
                            'strategy': 'auto'
                        }
                    }
                }
            )
            print(f"    → Disabled iceberg compaction")
        except Exception as e:
            print(f"    → Failed to disable compaction: {e}")

        # Disable iceberg snapshot management
        try:
            s3tables.put_table_maintenance_configuration(
                tableBucketARN=warehouse_path,
                namespace=namespace,
                name=table_name,
                type='icebergSnapshotManagement',
                value={
                    'status': 'disabled',
                    'settings': {
                        'icebergSnapshotManagement': {
                            'minSnapshotsToKeep': 1,
                            'maxSnapshotAgeHours': 120
                        }
                    }
                }
            )
            print(f"    → Disabled iceberg snapshot management")
        except Exception as e:
            print(f"    → Failed to disable snapshot management: {e}")

        print(f"    → S3 Tables maintenance configuration completed")

    except Exception as e:
        print(f"    → S3 Tables SDK call failed: {e}")

def create_namespace_if_not_exists(spark, catalog, namespace):
    """Create namespace if it doesn't exist"""
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")
        print(f"  → Created namespace {catalog}.{namespace}")
    except Exception:
        pass  # Namespace already exists

def create_spark_schema(table_spec):
    """Convert table spec to Spark schema"""
    fields = []

    for field_spec in table_spec['schema']:
        field_name = field_spec['name']
        field_type_str = field_spec['type']

        # Map type strings to Spark types
        if field_type_str == 'boolean':
            spark_type = BooleanType()
        elif field_type_str == 'int':
            spark_type = IntegerType()
        elif field_type_str == 'long':
            spark_type = LongType()
        elif field_type_str == 'float':
            spark_type = FloatType()
        elif field_type_str == 'double':
            spark_type = DoubleType()
        elif field_type_str == 'string':
            spark_type = StringType()
        elif field_type_str == 'binary':
            spark_type = BinaryType()
        elif field_type_str == 'date':
            spark_type = DateType()
        elif field_type_str == 'timestamp':
            # Spark's TimestampType is zone-aware and Iceberg records it as
            # timestamptz; the zone-free type is TimestampNTZType.
            spark_type = TimestampNTZType()
        elif field_type_str == 'timestamptz':
            spark_type = TimestampType()
        elif field_type_str.startswith('decimal('):
            parts = field_type_str[8:-1].split(',')
            precision = int(parts[0])
            scale = int(parts[1]) if len(parts) > 1 else 0
            spark_type = DecimalType(precision, scale)
        elif field_type_str in ('uuid', 'time') or field_type_str.startswith('fixed('):
            raise ValueError(
                f"Spark cannot express Iceberg type '{field_type_str}' "
                f"(column '{field_name}')"
            )
        elif field_type_str.startswith('list<'):
            element_type_str = field_type_str[5:-1]
            if element_type_str == 'boolean':
                element_type = BooleanType()
            elif element_type_str == 'int':
                element_type = IntegerType()
            elif element_type_str == 'long':
                element_type = LongType()
            elif element_type_str == 'float':
                element_type = FloatType()
            elif element_type_str == 'double':
                element_type = DoubleType()
            elif element_type_str == 'string':
                element_type = StringType()
            elif element_type_str == 'binary':
                element_type = BinaryType()
            elif element_type_str == 'date':
                element_type = DateType()
            elif element_type_str == 'timestamp':
                element_type = TimestampNTZType()
            elif element_type_str == 'timestamptz':
                element_type = TimestampType()
            elif element_type_str.startswith('decimal('):
                parts = element_type_str[8:-1].split(',')
                precision = int(parts[0])
                scale = int(parts[1]) if len(parts) > 1 else 0
                element_type = DecimalType(precision, scale)
            elif element_type_str in ('uuid', 'time') or element_type_str.startswith('fixed('):
                raise ValueError(
                    f"Spark cannot express Iceberg type '{element_type_str}' as a "
                    f"list element (column '{field_name}')"
                )
            else:
                element_type = StringType()
            spark_type = ArrayType(element_type)
        elif field_type_str.startswith('map<'):
            # Parse map<key_type, value_type>
            inner = field_type_str[4:-1]
            key_type_str, value_type_str = [t.strip() for t in inner.split(',', 1)]

            # Map key type
            if key_type_str == 'int':
                key_type = IntegerType()
            elif key_type_str == 'string':
                key_type = StringType()
            else:
                key_type = StringType()

            # Map value type
            if value_type_str == 'string':
                value_type = StringType()
            elif value_type_str == 'int':
                value_type = IntegerType()
            elif value_type_str == 'long':
                value_type = LongType()
            elif value_type_str == 'float':
                value_type = FloatType()
            elif value_type_str == 'double':
                value_type = DoubleType()
            elif value_type_str == 'boolean':
                value_type = BooleanType()
            elif value_type_str == 'binary':
                value_type = BinaryType()
            elif value_type_str == 'date':
                value_type = DateType()
            elif value_type_str == 'timestamp':
                value_type = TimestampNTZType()
            elif value_type_str == 'timestamptz':
                value_type = TimestampType()
            elif value_type_str.startswith('decimal('):
                parts = value_type_str[8:-1].split(',')
                precision = int(parts[0])
                scale = int(parts[1]) if len(parts) > 1 else 0
                value_type = DecimalType(precision, scale)
            elif value_type_str in ('uuid', 'time') or value_type_str.startswith('fixed('):
                raise ValueError(
                    f"Spark cannot express Iceberg type '{value_type_str}' as a "
                    f"map value (column '{field_name}')"
                )
            else:
                value_type = StringType()

            spark_type = MapType(key_type, value_type)
        else:
            raise ValueError(
                f"Unknown Iceberg type '{field_type_str}' (column '{field_name}')"
            )

        fields.append(StructField(field_name, spark_type, True))

    return StructType(fields)

def generate_spark_data(spark, table_spec, num_rows=100, row_offset=0):
    """Generate deterministic sample data using Spark to match PyIceberg patterns"""
    data = []

    for i in range(num_rows):
        row_id = row_id_from_index(i, row_offset)
        row_data = {}

        # Generate data for each field in the schema
        for field_spec in table_spec['schema']:
            field_name = field_spec['name']
            field_type = field_spec['type']

            if field_name == 'id':
                row_data[field_name] = row_id
            # Handle specific column name patterns first
            elif field_name == 'string_small_col':
                row_data[field_name] = f"s{row_id}"
            elif field_name == 'string_medium_col':
                row_data[field_name] = f"medium_value_{row_id}"
            elif field_name == 'string_long_col':
                row_data[field_name] = f"long_value_{row_id}_" + "x" * 50
            elif field_name == 'binary_small_col':
                row_data[field_name] = f"b{row_id}".encode()
            elif field_name == 'binary_medium_col':
                row_data[field_name] = f"medium_binary_{row_id}".encode()
            elif field_name == 'binary_long_col':
                row_data[field_name] = f"long_binary_{row_id}".encode() + b"x" * 50
            elif 'uuid' in field_name:
                # UUID must be exactly 16 bytes
                row_data[field_name] = uuid.UUID(int=row_id).bytes
            elif 'fixed' in field_name:
                row_data[field_name] = f"fixed_{row_id}".ljust(16, '0')[:16].encode()
            elif 'time' in field_name and field_type not in ['timestamp', 'timestamptz']:
                row_data[field_name] = f"{(row_id % 24):02d}:{(row_id % 60):02d}:00"
            # Then handle generic types
            elif field_type == 'boolean':
                row_data[field_name] = (row_id % 2) == 0
            elif field_type == 'int':
                row_data[field_name] = row_id
            elif field_type == 'long':
                row_data[field_name] = row_id * 1000
            elif field_type == 'float':
                row_data[field_name] = float(row_id) + 0.5
            elif field_type == 'double':
                row_data[field_name] = float(row_id) * 1.5
            elif field_type == 'string':
                row_data[field_name] = f"value_{row_id}"
            elif field_type == 'binary':
                row_data[field_name] = f"value_{row_id}".encode()
            elif field_type == 'date':
                row_data[field_name] = date(2023, 1, 1).replace(day=min(28, (row_id % 28) + 1))
            elif field_type == 'timestamp' or field_type == 'timestamptz':
                row_data[field_name] = datetime(2023, 1, 1).replace(hour=(row_id % 24))
            elif field_type.startswith('decimal('):
                row_data[field_name] = Decimal(f"{row_id}.{row_id % 100:02d}")
            elif field_type.startswith('list<'):
                element_type = field_type[5:-1]
                if element_type == 'boolean':
                    row_data[field_name] = [True, False, (row_id % 2) == 0]
                elif element_type == 'int':
                    row_data[field_name] = [row_id, row_id + 1, row_id + 2]
                elif element_type == 'long':
                    row_data[field_name] = [row_id * 1000, (row_id + 1) * 1000, (row_id + 2) * 1000]
                elif element_type == 'float':
                    row_data[field_name] = [float(row_id) + 0.5, float(row_id + 1) + 0.5, float(row_id + 2) + 0.5]
                elif element_type == 'double':
                    row_data[field_name] = [float(row_id) * 1.5, float(row_id + 1) * 1.5, float(row_id + 2) * 1.5]
                elif element_type == 'string':
                    row_data[field_name] = [f"item_{row_id}_{j}" for j in range(3)]
                elif element_type == 'date':
                    row_data[field_name] = [date(2023, 1, min(28, ((row_id + j) % 28) + 1)) for j in range(3)]
                elif element_type == 'timestamp' or element_type == 'timestamptz':
                    row_data[field_name] = [datetime(2023, 1, 1, hour=((row_id + j) % 24)) for j in range(3)]
                elif element_type.startswith('decimal('):
                    row_data[field_name] = [Decimal(f"{row_id + j}.{(row_id + j) % 100:02d}") for j in range(3)]
                elif element_type.startswith('fixed('):
                    length = int(element_type[6:-1])
                    row_data[field_name] = [
                        f"fixed_{row_id + j}".ljust(length, '0')[:length].encode()
                        for j in range(3)
                    ]
                elif element_type.startswith('binary'):
                    row_data[field_name] = [f"item_{row_id}_{j}".encode() for j in range(3)]
                else:
                    row_data[field_name] = [f"item_{row_id}_{j}" for j in range(3)]
            elif field_type.startswith('map<'):
                # Parse map type
                inner = field_type[4:-1]
                key_type_str, value_type_str = [t.strip() for t in inner.split(',', 1)]

                # Generate map data based on value type
                if value_type_str == 'string':
                    row_data[field_name] = {row_id: f"value_{row_id}", row_id + 1: f"value_{row_id + 1}"}
                elif value_type_str == 'int':
                    row_data[field_name] = {row_id: row_id * 10, row_id + 1: (row_id + 1) * 10}
                elif value_type_str == 'long':
                    row_data[field_name] = {row_id: row_id * 1000, row_id + 1: (row_id + 1) * 1000}
                elif value_type_str == 'float':
                    row_data[field_name] = {row_id: float(row_id) + 0.5, row_id + 1: float(row_id + 1) + 0.5}
                elif value_type_str == 'double':
                    row_data[field_name] = {row_id: float(row_id) * 1.5, row_id + 1: float(row_id + 1) * 1.5}
                elif value_type_str == 'boolean':
                    row_data[field_name] = {row_id: (row_id % 2) == 0, row_id + 1: ((row_id + 1) % 2) == 0}
                elif value_type_str == 'binary':
                    row_data[field_name] = {
                        row_id: f"value_{row_id}".encode(),
                        row_id + 1: f"value_{row_id + 1}".encode()
                    }
                elif value_type_str == 'date':
                    row_data[field_name] = {
                        row_id: date(2023, 1, min(28, (row_id % 28) + 1)),
                        row_id + 1: date(2023, 1, min(28, ((row_id + 1) % 28) + 1))
                    }
                elif value_type_str == 'timestamp' or value_type_str == 'timestamptz':
                    row_data[field_name] = {
                        row_id: datetime(2023, 1, 1, hour=(row_id % 24)),
                        row_id + 1: datetime(2023, 1, 1, hour=((row_id + 1) % 24))
                    }
                elif value_type_str.startswith('decimal('):
                    row_data[field_name] = {
                        row_id: Decimal(f"{row_id}.{row_id % 100:02d}"),
                        row_id + 1: Decimal(f"{row_id + 1}.{(row_id + 1) % 100:02d}")
                    }
                elif value_type_str.startswith('fixed('):
                    length = int(value_type_str[6:-1])
                    row_data[field_name] = {
                        row_id: f"fixed_{row_id}".ljust(length, '0')[:length].encode(),
                        row_id + 1: f"fixed_{row_id + 1}".ljust(length, '0')[:length].encode()
                    }
                else:
                    row_data[field_name] = {row_id: f"value_{row_id}", row_id + 1: f"value_{row_id + 1}"}
            else:
                # Default fallback
                row_data[field_name] = f"value_{row_id}"

        data.append(row_data)

    schema = create_spark_schema(table_spec)
    return spark.createDataFrame(data, schema)

def get_all_possible_table_names(specs_path='specs'):
    """Generate all possible table names from all spec files"""

    all_table_names = []

    # Get simple table names
    try:
        simple_specs = load_table_specs(f'{specs_path}/simple.yaml')
        all_table_names.extend([spec['name'] for spec in simple_specs])
    except Exception:
        pass  # Ignore errors

    # Get write pattern table names
    try:
        write_pattern_specs = load_table_specs(f'{specs_path}/write_patterns.yaml')
        all_table_names.extend([spec['name'] for spec in write_pattern_specs])
    except Exception:
        pass  # Ignore errors

    # Get evolution table names
    try:
        evolution_specs = load_table_specs(f'{specs_path}/evolution.yaml')
        all_table_names.extend([spec['name'] for spec in evolution_specs])
    except Exception:
        pass  # Ignore errors

    # Generate partitioned table names programmatically
    try:
        # Load partitioned spec to get transforms and patterns
        with open(f'{specs_path}/partitioned.yaml', 'r') as f:
            partitioned_spec = yaml.safe_load(f)

        transforms = partitioned_spec.get('partition_transforms', [])
        patterns = partitioned_spec.get('patterns', [])

        # Extract transform names and pattern names
        transform_names = [t['name'] for t in transforms]
        pattern_names = [p['name'] for p in patterns]
        delete_modes = ['cow', 'posdelete']

        for transform in transform_names:
            for pattern in pattern_names:
                for mode in delete_modes:
                    table_name = f"part_{transform}_{pattern}_{mode}"
                    all_table_names.append(table_name)
    except Exception:
        pass  # Ignore errors

    return sorted(list(set(all_table_names)))

def suggest_table_names(invalid_name, specs_path='specs'):
    """Suggest closest matching table names using weighted edit distance"""

    def weighted_levenshtein_distance(s1, s2):
        if len(s1) < len(s2):
            return weighted_levenshtein_distance(s2, s1)

        if len(s2) == 0:
            return len(s1)

        previous_row = list(range(len(s2) + 1))
        for i, c1 in enumerate(s1):
            current_row = [i + 1]
            for j, c2 in enumerate(s2):
                # Weight differences more heavily at the beginning of the string
                position_weight = 1.0 / (1.0 + i * 0.1)  # Higher weight for earlier positions

                insertions = previous_row[j + 1] + position_weight
                deletions = current_row[j] + position_weight
                substitutions = previous_row[j] + (position_weight if c1 != c2 else 0)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row

        return previous_row[-1]

    all_names = get_all_possible_table_names(specs_path)

    # Calculate weighted edit distance for each name and keep top 5
    distances = []
    for name in all_names:
        distance = weighted_levenshtein_distance(invalid_name.lower(), name.lower())
        heapq.heappush(distances, (distance, name))

    # Return top 5 closest matches
    suggestions = []
    for _ in range(min(5, len(distances))):
        if distances:
            _, name = heapq.heappop(distances)
            suggestions.append(name)

    return suggestions

def create_single_table(spark, warehouse_path, table_name, delete_existing=True):
    """Create a single table by name from all available specs"""

    # Try to find the table in simple specs
    try:
        simple_specs = load_table_specs('specs/simple.yaml')
        for spec in simple_specs:
            if spec['name'] == table_name:
                return create_single_type_table(
                    spark, warehouse_path, table_name, spec, delete_existing=delete_existing
                )
    except Exception:
        pass  # Ignore errors

    # Try to find the table in write pattern specs
    try:
        write_pattern_specs = load_table_specs('specs/write_patterns.yaml')
        for spec in write_pattern_specs:
            if spec['name'] == table_name:
                return create_comprehensive_table(
                    spark, warehouse_path, table_name, spec['pattern'],
                    delete_existing=delete_existing
                )
    except Exception:
        pass  # Ignore errors

    # Try to find the table in partitioned specs by generating all possible combinations
    try:
        with open('specs/partitioned.yaml', 'r') as f:
            partitioned_spec = yaml.safe_load(f)

        transforms = partitioned_spec.get('partition_transforms', [])
        patterns = partitioned_spec.get('patterns', [])

        # Generate all possible table names and find exact match
        for transform in transforms:
            for pattern in patterns:
                for mode in ['cow', 'posdelete']:
                    generated_name = f"part_{transform['name']}_{pattern['name']}_{mode}"
                    if generated_name == table_name:
                        # Found exact match - create the table using YAML data
                        partition_spec_list = transform['spec']  # This is already a list from YAML
                        partition_spec_str = ','.join(partition_spec_list)
                        return create_partitioned_table(
                            spark, warehouse_path, table_name, pattern['name'],
                            partition_spec_str, delete_existing=delete_existing
                        )
    except Exception as e:
        print(f"Error checking partitioned specs: {e}")

    # Try to find the table in evolution specs
    try:
        evolution_specs = load_table_specs('specs/evolution.yaml')
        for spec in evolution_specs:
            if spec['name'] == table_name:
                return create_evolution_table(spark, warehouse_path, table_name, spec, delete_existing=delete_existing)
    except Exception:
        pass  # Ignore errors

    # Table not found - suggest alternatives
    print(f"❌ Table '{table_name}' not found in any specs")

    suggestions = suggest_table_names(table_name)
    if suggestions:
        print(f"\n💡 Did you mean one of these?")
        for i, suggestion in enumerate(suggestions, 1):
            print(f"   {i}. {suggestion}")
    else:
        print(f"\n💡 Run without --table-name to see all available tables")

    return False

def create_single_type_table(spark, warehouse_path, table_name, table_spec, counter=None, delete_existing=True):
    """Create a single-type table with PySpark"""
    counter_str = f" ({counter})" if counter else ""

    # Get catalog and namespace
    catalog, namespace = get_catalog_and_namespace(warehouse_path, "simple")

    # Check if table exists and skip if delete_existing is False
    if not delete_existing and table_exists(spark, catalog, namespace, table_name):
        print(f"Skipping existing table: {table_name}{counter_str}")
        return True

    print(f"Creating table: {table_name}{counter_str}")

    try:
        # Create namespace if needed
        create_namespace_if_not_exists(spark, catalog, namespace)

        # Drop existing table only if delete_existing is True
        if delete_existing:
            drop_table_if_exists(spark, catalog, namespace, table_name)

        compression = table_spec.get('compression', {})
        codec = iceberg_compression_codec(compression.get('codec', 'snappy'))
        if codec == 'lz4':
            # Spark's Parquet writer only produces the LZ4_RAW framing
            codec = 'lz4_raw'
        level = compression.get('level')

        # Write 10 commits of 100 rows each
        for i in range(10):
            batch_df = generate_spark_data(spark, table_spec, 100, row_offset=i * 100)

            if i == 0:
                props = {
                    # format-version lives here; Iceberg defaults to 2, so a v1
                    # table needs it set explicitly.
                    **table_spec.get('properties', {}),
                    'write.parquet.compression-codec': codec,
                }
                if level is not None:
                    props['write.parquet.compression-level'] = str(level)
                # Create table with SQL first
                batch_df.createOrReplaceTempView("temp_create_data")
                schema_fields = ", ".join([f"{f.name} {f.dataType.simpleString()}" for f in batch_df.schema.fields])
                props_sql = ", ".join([f"'{k}' = '{v}'" for k, v in props.items()])
                spark.sql(
                    f"CREATE TABLE {catalog}.{namespace}.{table_name} "
                    f"({schema_fields}) USING iceberg "
                    f"TBLPROPERTIES ({props_sql})"
                )
                # Insert data
                batch_df.write.format("iceberg").mode("append").insertInto(f"{catalog}.{namespace}.{table_name}")
            else:
                batch_df.writeTo(f"{catalog}.{namespace}.{table_name}").append()

        # Disable compaction
        disable_table_compaction(spark, catalog, namespace, table_name, warehouse_path)

        return True
    except Exception as e:
        print(f"Error creating table {table_name}: {e}")
        return False

def create_table_with_mode(df, table_name):
    """Create table with appropriate mode based on table name"""
    if "cow" in table_name:
        # COW mode - use copy-on-write (default behavior)
        return df.writeTo(f"local.simple.{table_name}").create()
    elif "eqdelete" in table_name:
        # Equality delete mode - configure for equality deletes
        return df.writeTo(f"local.simple.{table_name}") \
            .tableProperty('write.delete.mode', 'merge-on-read') \
            .tableProperty('write.update.mode', 'merge-on-read') \
            .tableProperty('write.merge.mode', 'merge-on-read') \
            .tableProperty('write.delete.equality-field-columns', 'id') \
            .create()
    else:
        # Position delete mode (posdelete)
        return df.writeTo(f"local.simple.{table_name}") \
            .tableProperty('write.delete.mode', 'merge-on-read') \
            .tableProperty('write.update.mode', 'merge-on-read') \
            .tableProperty('write.merge.mode', 'merge-on-read') \
            .create()

def create_equality_deletes(spark, table_name, delete_condition):
    """
    Create equality delete files by using MERGE with equality delete configuration.
    This is a workaround since Spark's DELETE creates position deletes.
    """
    try:
        # Set session-level config to prefer equality deletes
        spark.conf.set("spark.sql.iceberg.delete.mode", "merge-on-read")

        # Use MERGE with DELETE to try to create equality deletes
        # The key is to match on the equality field (id)
        spark.sql(f"""
            MERGE INTO local.default.{table_name} AS target
            USING (
                SELECT DISTINCT id
                FROM local.default.{table_name}
                WHERE {delete_condition}
            ) AS source
            ON target.id = source.id
            WHEN MATCHED THEN DELETE
        """)
        return True
    except Exception as e:
        return False

def get_table_properties(table_name):
    """Get table properties dict based on delete mode and version"""
    props = {
        'write.distribution-mode': 'none'  # Disable distribution requirements (fanout mode)
    }

    # Set format version based on table name
    if "_v1_" in table_name:
        props['format-version'] = '1'
    elif "_v2_" in table_name:
        props['format-version'] = '2'
    else:
        props['format-version'] = '2'  # Default to v2

    # Only posdelete mode needs special properties (cow is default)
    if "posdelete" in table_name:
        props.update({
            'write.delete.mode': 'merge-on-read',
            'write.update.mode': 'merge-on-read',
            'write.merge.mode': 'merge-on-read',
            'write.spark.fanout.enabled': 'true',
            'write.delete.rewrite-data-files-threshold': '1',  # Force delete files for any delete
            'write.delete.rewrite-delete-files-threshold': '1000000',  # Never rewrite delete files
            'write.merge.rewrite-data-files-threshold': '1',  # Force delete files for merges
            'write.parquet.row-group-size-bytes': '1048576'  # Smaller row groups
        })

    return props

def create_comprehensive_table(spark, warehouse_path, table_name, pattern_name, counter=None, delete_existing=True):
    """Create a comprehensive write-pattern table"""
    counter_str = f" ({counter})" if counter else ""

    # Get catalog and namespace
    catalog, namespace = get_catalog_and_namespace(warehouse_path, "write_pattern")

    # Check if table exists and skip if delete_existing is False
    if not delete_existing and table_exists(spark, catalog, namespace, table_name):
        print(f"Skipping existing table: {table_name} (pattern: {pattern_name}){counter_str}")
        return True

    print(f"Creating table: {table_name} (pattern: {pattern_name}){counter_str}")

    try:
        # Create namespace if needed
        create_namespace_if_not_exists(spark, catalog, namespace)

        # Drop existing table only if delete_existing is True
        if delete_existing:
            drop_table_if_exists(spark, catalog, namespace, table_name)
        # Create comprehensive schema - 21 columns to match PyIceberg
        schema_spec = {
            'schema': [
                {'name': 'id', 'type': 'int'},
                {'name': 'boolean_col', 'type': 'boolean'},
                {'name': 'int_col', 'type': 'int'},
                {'name': 'long_col', 'type': 'long'},
                {'name': 'float_col', 'type': 'float'},
                {'name': 'double_col', 'type': 'double'},
                {'name': 'string_col', 'type': 'string'},
                {'name': 'binary_col', 'type': 'binary'},
                {'name': 'date_col', 'type': 'date'},
                {'name': 'time_col', 'type': 'string'},  # Time as string in Spark
                {'name': 'timestamp_col', 'type': 'timestamp'},
                {'name': 'timestamptz_col', 'type': 'timestamptz'},
                {'name': 'decimal_col', 'type': 'decimal(10,2)'},
                {'name': 'uuid_col', 'type': 'binary'},
                {'name': 'fixed_col', 'type': 'binary'},
                {'name': 'string_small_col', 'type': 'string'},
                {'name': 'string_medium_col', 'type': 'string'},
                {'name': 'string_long_col', 'type': 'string'},
                {'name': 'binary_small_col', 'type': 'binary'},
                {'name': 'binary_medium_col', 'type': 'binary'},
                {'name': 'binary_long_col', 'type': 'binary'},
                {'name': 'list_boolean_col', 'type': 'list<boolean>'},
                {'name': 'list_int_col', 'type': 'list<int>'},
                {'name': 'list_string_col', 'type': 'list<string>'}
            ]
        }

        # Execute pattern-specific operations with 10K rows
        # Use repartition to create multiple files per commit for compaction testing
        num_files_per_commit = 3  # Create 3 files per commit

        if pattern_name == "insert_heavy":
            # 10 commits of 1K rows each = 10K total, 30 files total
            for i in range(10):
                batch_df = generate_spark_data(spark, schema_spec, 1000, row_offset=i * 1000)
                batch_df = batch_df.repartition(num_files_per_commit)

                if i == 0:
                    props = get_table_properties(table_name)

                    # Create table with SQL first

                    batch_df.createOrReplaceTempView("temp_create_data")

                    schema_fields = ", ".join([f"{f.name} {f.dataType.simpleString()}" for f in batch_df.schema.fields])

                    props_sql = ", ".join([f"'{k}' = '{v}'" for k, v in props.items()])

                    spark.sql(
                        f"CREATE TABLE {catalog}.{namespace}.{table_name} "
                        f"({schema_fields}) USING iceberg "
                        f"TBLPROPERTIES ({props_sql})"
                    )

                    # Insert data

                    batch_df.write.format("iceberg").mode("append").insertInto(f"{catalog}.{namespace}.{table_name}")
                else:
                    batch_df.writeTo(f"{catalog}.{namespace}.{table_name}").append()

        elif pattern_name == "batch_replace":
            # Create initial batch then replace with full dataset
            df = generate_spark_data(spark, schema_spec, 5000)
            df = df.repartition(num_files_per_commit)

            props = get_table_properties(table_name)
            # Create table with SQL first
            df.createOrReplaceTempView("temp_create_data")
            schema_fields = ", ".join([f"{f.name} {f.dataType.simpleString()}" for f in df.schema.fields])
            props_sql = ", ".join([f"'{k}' = '{v}'" for k, v in props.items()])
            spark.sql(
                f"CREATE TABLE {catalog}.{namespace}.{table_name} "
                f"({schema_fields}) USING iceberg "
                f"TBLPROPERTIES ({props_sql})"
            )
            # Insert data
            df.write.format("iceberg").mode("append").insertInto(f"{catalog}.{namespace}.{table_name}")

            # Replace with full dataset
            if "cow" in table_name:
                # COW mode: use overwrite
                df_full = generate_spark_data(spark, schema_spec, 10000)
                df_full = df_full.repartition(num_files_per_commit)
                df_full.writeTo(f"{catalog}.{namespace}.{table_name}").overwritePartitions()
            else:
                # Posdelete mode: DELETE all + INSERT new data
                try:
                    spark.sql(f"DELETE FROM {catalog}.{namespace}.{table_name}")
                    df_full = generate_spark_data(spark, schema_spec, 10000)
                    df_full = df_full.repartition(num_files_per_commit)
                    df_full.writeTo(f"{catalog}.{namespace}.{table_name}").append()
                except Exception as e:

                    df_full = generate_spark_data(spark, schema_spec, 10000)
                    df_full = df_full.repartition(num_files_per_commit)
                    df_full.writeTo(f"{catalog}.{namespace}.{table_name}").overwritePartitions()

        elif pattern_name == "gradual_growth":
            # 10 commits of 1K rows each, 30 files total
            for i in range(10):
                batch_df = generate_spark_data(spark, schema_spec, 1000, row_offset=i * 1000)
                batch_df = batch_df.repartition(num_files_per_commit)

                if i == 0:
                    props = get_table_properties(table_name)

                    # Create table with SQL first

                    batch_df.createOrReplaceTempView("temp_create_data")

                    schema_fields = ", ".join([f"{f.name} {f.dataType.simpleString()}" for f in batch_df.schema.fields])

                    props_sql = ", ".join([f"'{k}' = '{v}'" for k, v in props.items()])

                    spark.sql(
                        f"CREATE TABLE {catalog}.{namespace}.{table_name} "
                        f"({schema_fields}) USING iceberg "
                        f"TBLPROPERTIES ({props_sql})"
                    )

                    # Insert data

                    batch_df.write.format("iceberg").mode("append").insertInto(f"{catalog}.{namespace}.{table_name}")
                else:
                    batch_df.writeTo(f"{catalog}.{namespace}.{table_name}").append()

        elif pattern_name == "burst_ops":
            # Small commits then large operation
            for i in range(3):
                batch_df = generate_spark_data(spark, schema_spec, 500, row_offset=i * 500)
                batch_df = batch_df.repartition(num_files_per_commit)

                if i == 0:
                    props = get_table_properties(table_name)
                    # Create table with SQL first
                    batch_df.createOrReplaceTempView("temp_create_data")
                    schema_fields = ", ".join([f"{f.name} {f.dataType.simpleString()}" for f in batch_df.schema.fields])
                    props_sql = ", ".join([f"'{k}' = '{v}'" for k, v in props.items()])
                    spark.sql(
                        f"CREATE TABLE {catalog}.{namespace}.{table_name} "
                        f"({schema_fields}) USING iceberg "
                        f"TBLPROPERTIES ({props_sql})"
                    )
                    # Insert data
                    batch_df.write.format("iceberg").mode("append").insertInto(f"{catalog}.{namespace}.{table_name}")
                else:
                    batch_df.writeTo(f"{catalog}.{namespace}.{table_name}").append()

            # Large operation
            if "cow" in table_name:
                # COW mode: use overwrite
                df_full = generate_spark_data(spark, schema_spec, 10000)
                df_full = df_full.repartition(num_files_per_commit)
                df_full.writeTo(f"{catalog}.{namespace}.{table_name}").overwritePartitions()
            else:
                # Posdelete mode: DELETE all + INSERT new data
                try:
                    spark.sql(f"DELETE FROM {catalog}.{namespace}.{table_name}")
                    df_full = generate_spark_data(spark, schema_spec, 10000)
                    df_full = df_full.repartition(num_files_per_commit)
                    df_full.writeTo(f"{catalog}.{namespace}.{table_name}").append()
                except Exception as e:

                    df_full = generate_spark_data(spark, schema_spec, 10000)
                    df_full = df_full.repartition(num_files_per_commit)
                    df_full.writeTo(f"{catalog}.{namespace}.{table_name}").overwritePartitions()

        # PySpark can also handle the limited patterns with actual operations
        elif pattern_name == "update_heavy":
            # Create initial data then simulate updates
            df = generate_spark_data(spark, schema_spec, 10000)
            df = df.repartition(num_files_per_commit)
            props = get_table_properties(table_name)
            props.update({
            'write.delete.mode': 'merge-on-read',
            'write.update.mode': 'merge-on-read',
            'write.merge.mode': 'merge-on-read'
        })
            # Create table with SQL first
            df.createOrReplaceTempView("temp_create_data")
            schema_fields = ", ".join([f"{f.name} {f.dataType.simpleString()}" for f in df.schema.fields])
            props_sql = ", ".join([f"'{k}' = '{v}'" for k, v in props.items()])
            spark.sql(
                f"CREATE TABLE {catalog}.{namespace}.{table_name} "
                f"({schema_fields}) USING iceberg "
                f"TBLPROPERTIES ({props_sql})"
            )
            # Insert data
            df.write.format("iceberg").mode("append").insertInto(f"{catalog}.{namespace}.{table_name}")

            # Simulate multiple update operations
            if "cow" in table_name:
                # COW mode: use overwrites
                for i in range(5):
                    df_updated = generate_spark_data(spark, schema_spec, 10000)
                    df_updated = df_updated.repartition(num_files_per_commit)
                    df_updated.writeTo(f"{catalog}.{namespace}.{table_name}").overwritePartitions()
            else:
                # Posdelete mode: use DELETE + INSERT to create position deletes
                for i in range(5):
                    # Delete a range of rows
                    start_id = i * 2000 + 1
                    end_id = (i + 1) * 2000
                    try:
                        spark.sql(
                            f"DELETE FROM {catalog}.{namespace}.{table_name} "
                            f"WHERE id >= {start_id} AND id < {end_id}"
                        )
                        # Insert updated rows
                        df_updated = generate_spark_data(spark, schema_spec, 2000, row_offset=delete_start-1)
                        df_updated = df_updated.repartition(num_files_per_commit)
                        df_updated.writeTo(f"{catalog}.{namespace}.{table_name}").append()
                    except Exception as e:

                        df_updated = generate_spark_data(spark, schema_spec, 10000)
                        df_updated = df_updated.repartition(num_files_per_commit)
                        df_updated.writeTo(f"{catalog}.{namespace}.{table_name}").overwritePartitions()
                        break

        elif pattern_name == "delete_heavy":
            # Create initial data with 20K rows
            df = generate_spark_data(spark, schema_spec, 20000)
            df = df.repartition(num_files_per_commit * 2)  # More files for larger dataset

            props = get_table_properties(table_name)


            # Create table with SQL first


            df.createOrReplaceTempView("temp_create_data")


            schema_fields = ", ".join([f"{f.name} {f.dataType.simpleString()}" for f in df.schema.fields])


            props_sql = ", ".join([f"'{k}' = '{v}'" for k, v in props.items()])


            spark.sql(
                f"CREATE TABLE {catalog}.{namespace}.{table_name} "
                f"({schema_fields}) USING iceberg "
                f"TBLPROPERTIES ({props_sql})"
            )


            # Insert data


            df.write.format("iceberg").mode("append").insertInto(f"{catalog}.{namespace}.{table_name}")

            # For COW mode, use overwrite to avoid delete files
            if "cow" in table_name:
                final_df = generate_spark_data(spark, schema_spec, 10000)
                final_df = final_df.repartition(num_files_per_commit)
                final_df.writeTo(f"{catalog}.{namespace}.{table_name}").overwritePartitions()
            else:
                # For posdelete mode, use regular DELETE for position deletes
                try:
                    spark.sql(f"DELETE FROM {catalog}.{namespace}.{table_name} WHERE id > 10000")
                    print(f"✓ Used DELETE operation for {table_name}")
                except Exception as e:

                    final_df = generate_spark_data(spark, schema_spec, 10000)
                    final_df = final_df.repartition(num_files_per_commit)
                    final_df.writeTo(f"{catalog}.{namespace}.{table_name}").overwritePartitions()

        elif pattern_name == "mixed_chaos":
            # Mixed operations - inserts, updates, deletes
            df = generate_spark_data(spark, schema_spec, 5000)
            df = df.repartition(num_files_per_commit)

            props = get_table_properties(table_name)
            # Create table with SQL first
            df.createOrReplaceTempView("temp_create_data")
            schema_fields = ", ".join([f"{f.name} {f.dataType.simpleString()}" for f in df.schema.fields])
            props_sql = ", ".join([f"'{k}' = '{v}'" for k, v in props.items()])
            spark.sql(
                f"CREATE TABLE {catalog}.{namespace}.{table_name} "
                f"({schema_fields}) USING iceberg "
                f"TBLPROPERTIES ({props_sql})"
            )
            # Insert data
            df.write.format("iceberg").mode("append").insertInto(f"{catalog}.{namespace}.{table_name}")

            # Add more data
            append_df = generate_spark_data(spark, schema_spec, 3000, row_offset=5000)
            append_df = append_df.repartition(num_files_per_commit)
            append_df.writeTo(f"{catalog}.{namespace}.{table_name}").append()

            # For COW mode, use overwrite to avoid delete files
            if "cow" in table_name:
                final_df = generate_spark_data(spark, schema_spec, 10000)
                final_df = final_df.repartition(num_files_per_commit)
                final_df.writeTo(f"{catalog}.{namespace}.{table_name}").overwritePartitions()
            else:
                # For posdelete mode, use DELETE
                try:
                    spark.sql(f"DELETE FROM {catalog}.{namespace}.{table_name} WHERE id > 6000")
                    # Add final data to reach 10K rows
                    final_append = generate_spark_data(spark, schema_spec, 4000, row_offset=6000)
                    final_append = final_append.repartition(num_files_per_commit)
                    final_append.writeTo(f"{catalog}.{namespace}.{table_name}").append()
                except Exception as e:

                    final_df = generate_spark_data(spark, schema_spec, 10000)
                    final_df = final_df.repartition(num_files_per_commit)
                    final_df.writeTo(f"{catalog}.{namespace}.{table_name}").overwritePartitions()

        elif pattern_name == "truncate_cycles":
            # Create, truncate, recreate cycles using TRUNCATE or overwrite
            df = generate_spark_data(spark, schema_spec, 4000)
            df = df.repartition(num_files_per_commit)
            props = get_table_properties(table_name)
            props.update({
            'write.delete.mode': 'merge-on-read',
            'write.update.mode': 'merge-on-read',
            'write.merge.mode': 'merge-on-read'
        })
            # Create table with SQL first
            df.createOrReplaceTempView("temp_create_data")
            schema_fields = ", ".join([f"{f.name} {f.dataType.simpleString()}" for f in df.schema.fields])
            props_sql = ", ".join([f"'{k}' = '{v}'" for k, v in props.items()])
            spark.sql(
                f"CREATE TABLE {catalog}.{namespace}.{table_name} "
                f"({schema_fields}) USING iceberg "
                f"TBLPROPERTIES ({props_sql})"
            )
            # Insert data
            df.write.format("iceberg").mode("append").insertInto(f"{catalog}.{namespace}.{table_name}")

            # Truncate and refill cycles
            for cycle in range(2):
                try:
                    spark.sql(f"TRUNCATE TABLE {catalog}.{namespace}.{table_name}")
                except Exception:
                    # If TRUNCATE not supported, use DELETE
                    spark.sql(f"DELETE FROM {catalog}.{namespace}.{table_name}")

                df_refill = generate_spark_data(spark, schema_spec, 4000)
                df_refill = df_refill.repartition(num_files_per_commit)
                df_refill.writeTo(f"{catalog}.{namespace}.{table_name}").append()

            # Final truncate and full dataset
            try:
                spark.sql(f"TRUNCATE TABLE {catalog}.{namespace}.{table_name}")
            except Exception:
                spark.sql(f"DELETE FROM {catalog}.{namespace}.{table_name}")

            final_df = generate_spark_data(spark, schema_spec, 10000)
            final_df = final_df.repartition(num_files_per_commit)
            final_df.writeTo(f"{catalog}.{namespace}.{table_name}").append()

        # Disable compaction
        disable_table_compaction(spark, catalog, namespace, table_name, warehouse_path)

        return True
    except Exception as e:
        print(f"Error creating table {table_name}: {e}")
        return False

def create_table_worker_with_session(warehouse_path, table_spec):
    """Worker function that creates its own Spark session per thread"""
    spark = None
    try:
        spark = create_spark_session(warehouse_path)
        success = create_single_type_table(spark, warehouse_path, table_spec['name'], table_spec)
        return (table_spec['name'], success)
    except Exception as e:
        return (table_spec['name'], False)
    finally:
        if spark:
            spark.stop()

def create_table_worker(spark, warehouse_path, spec, delete_existing=True):
    """Worker function to create a single table using shared Spark session"""
    try:
        success = create_single_type_table(spark, warehouse_path, spec['name'], spec, delete_existing=delete_existing)
        return (spec['name'], success)
    except Exception as e:
        return (spec['name'], False)

def generate_single_tables(args, spark, warehouse_path):
    """Generate single-type tables"""
    minimal = args.table_type == 'minimal'
    table_specs = load_table_specs(args.specs, minimal=minimal)

    print(f"Generating {len(table_specs)} single-type tables with {args.num_parallel} parallel workers...")

    success_count = 0
    total_count = len(table_specs)

    if args.num_parallel > 1:
        # Process in batches to avoid overwhelming the executor with 11k+ futures
        batch_size = 256
        with ThreadPoolExecutor(max_workers=args.num_parallel) as executor:
            for batch_start in range(0, len(table_specs), batch_size):
                batch_end = min(batch_start + batch_size, len(table_specs))
                batch = table_specs[batch_start:batch_end]

                futures = {
                    executor.submit(create_table_worker_with_session, warehouse_path, spec): spec
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
        for i, spec in enumerate(table_specs, 1):
            success = create_single_type_table(spark, warehouse_path, spec['name'], spec, f"{i}/{total_count}")
            if success:
                success_count += 1
                print(f"✓ Created table {spec['name']} ({success_count}/{total_count})")
            else:
                print(f"✗ Failed to create table {spec['name']}")

    print(f"\nCompleted single tables: {success_count}/{total_count} tables created successfully")

def create_write_pattern_worker_with_session(warehouse_path, table_name, pattern):
    """Worker function that creates its own Spark session per thread"""
    spark = None
    try:
        spark = create_spark_session(warehouse_path)
        success = create_comprehensive_table(spark, warehouse_path, table_name, pattern)
        return (table_name, success)
    except Exception as e:
        return (table_name, False)
    finally:
        if spark:
            spark.stop()

def create_write_pattern_worker(spark, warehouse_path, table_name, pattern, delete_existing=True):
    """Worker function to create a write-pattern table using shared Spark session"""
    try:
        success = create_comprehensive_table(
            spark, warehouse_path, table_name, pattern, delete_existing=delete_existing
        )
        return (table_name, success)
    except Exception as e:
        return (table_name, False)

def generate_write_pattern_tables(args, spark, warehouse_path):
    """Generate comprehensive write-pattern tables"""
    all_patterns = ["insert_heavy", "update_heavy", "delete_heavy", "mixed_chaos",
                    "batch_replace", "gradual_growth", "burst_ops", "truncate_cycles"]
    versions = ["v1", "v2"]
    delete_modes = ["cow", "posdelete"]  # Remove eqdelete - PySpark cannot create equality deletes

    tasks = []
    for pattern in all_patterns:
        for version in versions:
            for delete_mode in delete_modes:
                # Skip invalid combinations: v1 only supports cow
                if version == "v1" and delete_mode != "cow":
                    continue

                table_name = f"unpart_{pattern}_zstd_plain_dictionary_{version}_{delete_mode}"
                tasks.append((table_name, pattern))

    # Calculate actual table count after filtering invalid combinations
    # v1: only cow (8 patterns × 1 delete mode = 8)
    # v2: cow, posdelete (8 patterns × 2 delete modes = 16)
    # Total: 8 + 16 = 24 tables
    total_tables = len(tasks)
    created = 0

    print(f"Generating {total_tables} write-pattern tables (PySpark supports all patterns)...")

    if args.num_parallel > 1:
        with ThreadPoolExecutor(max_workers=args.num_parallel) as executor:
            futures = {
                executor.submit(
                    create_write_pattern_worker_with_session, warehouse_path, name, pattern
                ): (name, pattern)
                for name, pattern in table_specs
            }
            for future in as_completed(futures):
                try:
                    table_name, success = future.result()
                    if success:
                        created += 1
                        print(f"✓ Created {table_name} ({created}/{total_tables})")
                    else:
                        print(f"✗ Failed to create {table_name}")
                except Exception as e:
                    print(f"✗ Failed to create table: {e}")
    else:
        for i, (table_name, pattern) in enumerate(tasks, 1):
            try:
                success = create_comprehensive_table(spark, warehouse_path, table_name, pattern, f"{i}/{total_tables}")
                if success:
                    created += 1
                    print(f"✓ Created {table_name} ({created}/{total_tables})")
                else:
                    print(f"✗ Failed to create {table_name}")
            except Exception as e:
                print(f"✗ Failed to create {table_name}: {e}")

    print(f"\nCompleted write-pattern tables: {created}/{total_tables} tables created successfully")

def generate_partitioned_data(spark, schema_spec, num_rows=100, row_offset=0):
    """Generate data for partitioned tables with partition-specific fields"""

    schema = create_spark_schema(schema_spec)
    data = []

    categories = ['A', 'B', 'C', 'D', 'E']

    for i in range(num_rows):
        row_id = row_id_from_index(i, row_offset)
        row = {
            'id': row_id,
            'category': categories[row_id % 5],
            'name': f'name_{row_id % 100}',
            'value': row_id * 10,
            'timestamp': datetime(2020 + (row_id % 5), 1 + (row_id % 12), 1 + (row_id % 28), row_id % 24, 0, 0),
            'deprecated_field': 'deprecated',
            'single_value_field': 'constant',
            'boolean_col': row_id % 2 == 0,
            'int_col': row_id,
            'long_col': row_id * 1000,
            'float_col': float(row_id) * 1.5,
            'double_col': float(row_id) * 2.5,
            'string_col': f'str_{row_id}',
            'binary_col': f'bin_{row_id}'.encode(),
            'date_col': date(2020, 1, 1),
            'time_col': '12:00:00',
            'timestamp_col': datetime(2020, 1, 1, 12, 0, 0),
            'timestamptz_col': datetime(2020, 1, 1, 12, 0, 0),
            'decimal_col': Decimal(f'{row_id}.50'),
            'uuid_col': f'uuid_{row_id}'.encode()[:16].ljust(16, b'\x00'),
            'fixed_col': f'fix_{row_id}'.encode()[:10].ljust(10, b'\x00'),
            'string_small_col': f's{row_id}',
            'string_medium_col': f'medium_{row_id}' * 5,
            'string_long_col': f'long_{row_id}' * 20,
            'binary_small_col': f'b{row_id}'.encode(),
            'binary_medium_col': f'bm_{row_id}'.encode() * 5,
            'binary_long_col': f'bl_{row_id}'.encode() * 20,
            'list_boolean_col': [True, False, row_id % 2 == 0],
            'list_int_col': [row_id, row_id + 1, row_id + 2],
            'list_string_col': [f'item_{row_id}', f'item_{row_id + 1}']
        }
        data.append(row)

    return spark.createDataFrame(data, schema)

def parse_partition_spec(spec_str):
    """Parse partition spec string into list of (transform, column, param) tuples"""
    # Smart split that respects parentheses
    parts = []
    current_part = ""
    paren_count = 0

    for char in spec_str:
        if char == '(':
            paren_count += 1
            current_part += char
        elif char == ')':
            paren_count -= 1
            current_part += char
        elif char == ',' and paren_count == 0:
            parts.append(current_part.strip())
            current_part = ""
        else:
            current_part += char

    if current_part.strip():
        parts.append(current_part.strip())

    partition_specs = []

    for part in parts:
        if part.startswith('identity('):
            col_name = part[9:-1]
            partition_specs.append(('identity', col_name, None))
        elif part.startswith('bucket['):
            bucket_size = part.split('[')[1].split(']')[0]
            col_name = part.split('(')[1].rstrip(')')
            partition_specs.append(('bucket', col_name, int(bucket_size)))
        elif part.startswith('bucket('):
            # Handle bucket(N, col) format from YAML
            inner = part[7:-1]  # Remove bucket( and )
            bucket_size, col_name = [x.strip() for x in inner.split(',')]
            partition_specs.append(('bucket', col_name, int(bucket_size)))
        elif part.startswith('truncate['):
            width = part.split('[')[1].split(']')[0]
            col_name = part.split('(')[1].rstrip(')')
            partition_specs.append(('truncate', col_name, int(width)))
        elif part.startswith('truncate('):
            # Handle truncate(N, col) format from YAML
            inner = part[9:-1]  # Remove truncate( and )
            width, col_name = [x.strip() for x in inner.split(',')]
            partition_specs.append(('truncate', col_name, int(width)))
        elif part.startswith('date('):
            col_name = part[5:-1]
            partition_specs.append(('date', col_name, None))
        elif part.startswith('year(') or part.startswith('years('):
            col_name = part[5:-1] if part.startswith('year(') else part[6:-1]
            partition_specs.append(('year', col_name, None))
        elif part.startswith('month(') or part.startswith('months('):
            col_name = part[6:-1] if part.startswith('month(') else part[7:-1]
            partition_specs.append(('month', col_name, None))
        elif part.startswith('day(') or part.startswith('days('):
            col_name = part[4:-1] if part.startswith('day(') else part[5:-1]
            partition_specs.append(('day', col_name, None))
        elif part.startswith('hour(') or part.startswith('hours('):
            col_name = part[5:-1] if part.startswith('hour(') else part[6:-1]
            partition_specs.append(('hour', col_name, None))
        elif part.startswith('void('):
            col_name = part[5:-1]
            partition_specs.append(('void', col_name, None))

    return partition_specs

def get_sort_columns(partition_specs):
    """Get columns to sort by - returns None if non-identity transforms present"""
    has_non_identity = any(t != 'identity' for t, _, _ in partition_specs)
    if has_non_identity:
        return None
    return [col for _, col, _ in partition_specs]

def prepare_partitioned_df(df, sort_cols, num_partitions):
    """Prepare DataFrame for partitioned write"""
    if sort_cols is None:
        # For non-identity transforms, just repartition
        return df.repartition(num_partitions)
    # For identity transforms, sort within partitions
    return df.repartition(num_partitions).sortWithinPartitions(*sort_cols)

def create_partitioned_table(
    spark, warehouse_path, table_name, pattern_name, partition_spec,
    delete_existing=False
):
    """Create a partitioned table"""
    # Get catalog and namespace
    catalog, namespace = get_catalog_and_namespace(warehouse_path, "partitioned")

    # Check if table exists and skip if delete_existing is False
    if not delete_existing and table_exists(spark, catalog, namespace, table_name):
        print(
            f"Skipping existing table: {table_name} "
            f"(pattern: {pattern_name}, partition: {partition_spec})"
        )
        return True

    print(
        f"Creating partitioned table: {table_name} "
        f"(pattern: {pattern_name}, partition: {partition_spec})"
    )

    try:
        # Create namespace if needed
        create_namespace_if_not_exists(spark, catalog, namespace)

        # Drop existing table only if delete_existing is True
        if delete_existing:
            drop_table_if_exists(spark, catalog, namespace, table_name)
        schema_spec = {
            'schema': [
                {'name': 'id', 'type': 'int'}, {'name': 'category', 'type': 'string'},
                {'name': 'name', 'type': 'string'}, {'name': 'value', 'type': 'int'},
                {'name': 'timestamp', 'type': 'timestamp'}, {'name': 'deprecated_field', 'type': 'string'},
                {'name': 'single_value_field', 'type': 'string'}, {'name': 'boolean_col', 'type': 'boolean'},
                {'name': 'int_col', 'type': 'int'}, {'name': 'long_col', 'type': 'long'},
                {'name': 'float_col', 'type': 'float'}, {'name': 'double_col', 'type': 'double'},
                {'name': 'string_col', 'type': 'string'}, {'name': 'binary_col', 'type': 'binary'},
                {'name': 'date_col', 'type': 'date'}, {'name': 'time_col', 'type': 'string'},
                {'name': 'timestamp_col', 'type': 'timestamp'}, {'name': 'timestamptz_col', 'type': 'timestamptz'},
                {'name': 'decimal_col', 'type': 'decimal(10,2)'}, {'name': 'uuid_col', 'type': 'binary'},
                {'name': 'fixed_col', 'type': 'binary'}, {'name': 'string_small_col', 'type': 'string'},
                {'name': 'string_medium_col', 'type': 'string'}, {'name': 'string_long_col', 'type': 'string'},
                {'name': 'binary_small_col', 'type': 'binary'}, {'name': 'binary_medium_col', 'type': 'binary'},
                {'name': 'binary_long_col', 'type': 'binary'}, {'name': 'list_boolean_col', 'type': 'list<boolean>'},
                {'name': 'list_int_col', 'type': 'list<int>'}, {'name': 'list_string_col', 'type': 'list<string>'}
            ]
        }

        partition_specs = parse_partition_spec(partition_spec)
        sort_cols = get_sort_columns(partition_specs)

        # Build SQL partition spec
        partition_spec_sql = []
        for transform, col_name, param in partition_specs:
            if transform == 'identity':
                partition_spec_sql.append(col_name)
            elif transform == 'bucket':
                partition_spec_sql.append(f"bucket({param}, {col_name})")
            elif transform == 'truncate':
                partition_spec_sql.append(f"truncate({param}, {col_name})")
            elif transform == 'date':
                partition_spec_sql.append(f"date({col_name})")
            elif transform == 'year':
                partition_spec_sql.append(f"years({col_name})")
            elif transform == 'month':
                partition_spec_sql.append(f"months({col_name})")
            elif transform == 'day':
                partition_spec_sql.append(f"days({col_name})")
            elif transform == 'hour':
                partition_spec_sql.append(f"hours({col_name})")
            elif transform == 'void':
                partition_spec_sql.append(f"void({col_name})")

        # Build table properties - use snappy compression and plain encoding for partitioned tables
        props = get_table_properties(table_name)

        # Override compression and encoding for partitioned tables
        props['write.parquet.compression-codec'] = 'snappy'
        # Note: Encoding is auto-selected by Spark, plain is default for most data types

        # Remove fanout mode - we'll sort data instead
        if 'write.distribution-mode' in props:
            del props['write.distribution-mode']

        props_sql = ", ".join([f"'{k}' = '{v}'" for k, v in props.items()])

        # Create table with schema (no CTAS to avoid staged create)
        schema_df = generate_partitioned_data(spark, schema_spec, 1, 0)
        schema_fields = ", ".join([f"{f.name} {f.dataType.simpleString()}" for f in schema_df.schema.fields])

        # Build CREATE TABLE statement with properties
        create_sql = f"CREATE TABLE {catalog}.{namespace}.{table_name} ({schema_fields}) USING iceberg"
        if props_sql:
            create_sql += f" TBLPROPERTIES ({props_sql})"

        spark.sql(create_sql)

        # Apply pattern-specific logic
        if pattern_name == "insert_heavy":
            # Pure append operations - 10 batches of 10K rows each
            for i in range(10):
                batch_df = generate_partitioned_data(spark, schema_spec, 10000, i * 10000)
                batch_df = prepare_partitioned_df(batch_df, sort_cols, 3)
                batch_df.writeTo(f"{catalog}.{namespace}.{table_name}").append()

        elif pattern_name == "delete_heavy":
            # Create 200K rows, then delete half to end with 100K
            for i in range(20):
                batch_df = generate_partitioned_data(spark, schema_spec, 10000, i * 10000)
                batch_df = prepare_partitioned_df(batch_df, sort_cols, 3)
                batch_df.writeTo(f"{catalog}.{namespace}.{table_name}").append()

            # Delete operations based on mode
            if "cow" in table_name:
                # COW: Use overwrite to avoid delete files
                final_df = generate_partitioned_data(spark, schema_spec, 10000, 0)  # Reduced from 100000
                final_df = prepare_partitioned_df(final_df, sort_cols, 3)
                final_df.writeTo(f"{catalog}.{namespace}.{table_name}").overwritePartitions()
            else:
                # Posdelete: Use DELETE to create position delete files
                spark.sql(f"DELETE FROM {catalog}.{namespace}.{table_name} WHERE id > 100000")

        elif pattern_name == "update_heavy":
            # Create 120K rows, then delete in cycles, end with 100K
            for i in range(12):
                batch_df = generate_partitioned_data(spark, schema_spec, 10000, i * 10000)
                batch_df = prepare_partitioned_df(batch_df, sort_cols, 3)
                batch_df.writeTo(f"{catalog}.{namespace}.{table_name}").append()

            # Update cycles based on mode
            if "cow" in table_name:
                # COW: Use overwrite operations
                for i in range(5):
                    updated_df = generate_partitioned_data(spark, schema_spec, 100000, 0)
                    updated_df = prepare_partitioned_df(updated_df, sort_cols, 3)
                    updated_df.writeTo(f"{catalog}.{namespace}.{table_name}").overwritePartitions()
            else:
                # Posdelete: DELETE then INSERT to maintain 100K final count
                spark.sql(f"DELETE FROM {catalog}.{namespace}.{table_name} WHERE id > 100000")

        elif pattern_name == "mixed_chaos":
            # Create 150K rows, delete 1/3, add back to reach 100K
            for i in range(15):
                batch_df = generate_partitioned_data(spark, schema_spec, 10000, i * 10000)
                batch_df = prepare_partitioned_df(batch_df, sort_cols, 3)
                batch_df.writeTo(f"{catalog}.{namespace}.{table_name}").append()

            if "cow" in table_name:
                # COW: Use overwrite
                final_df = generate_partitioned_data(spark, schema_spec, 100000, 0)
                final_df = prepare_partitioned_df(final_df, sort_cols, 3)
                final_df.writeTo(f"{catalog}.{namespace}.{table_name}").overwritePartitions()
            else:
                # Posdelete: Force position deletes with multiple DELETE operations
                # Delete in smaller chunks to force position delete files
                for chunk in range(5):
                    start_id = 100000 + (chunk * 10000) + 1
                    end_id = 100000 + ((chunk + 1) * 10000)
                    try:
                        spark.sql(
                            f"DELETE FROM {catalog}.{namespace}.{table_name} "
                            f"WHERE id >= {start_id} AND id < {end_id}"
                        )
                        print(f"✓ DELETE chunk {chunk+1}/5 for {table_name}")
                    except Exception as e:
                        print(f"✗ DELETE failed for {table_name}: {e}")
                        # Fallback to overwrite if DELETE fails
                        final_df = generate_partitioned_data(spark, schema_spec, 100000, 0)
                        final_df = prepare_partitioned_df(final_df, sort_cols, 3)
                        final_df.writeTo(f"{catalog}.{namespace}.{table_name}").overwritePartitions()
                        break

        else:
            # Default: append-only pattern (100K rows)
            for i in range(10):
                batch_df = generate_partitioned_data(spark, schema_spec, 10000, i * 10000)
                batch_df = prepare_partitioned_df(batch_df, sort_cols, 3)
                batch_df.writeTo(f"{catalog}.{namespace}.{table_name}").append()

        # Disable compaction
        disable_table_compaction(spark, catalog, namespace, table_name, warehouse_path)

        return True
    except Exception as e:
        print(f"Error creating {table_name}: {e}")
        return False

def create_partitioned_table_worker_with_session(warehouse_path, table_name, pattern, partition_spec):
    """Worker function that creates its own Spark session per thread"""
    spark = None
    try:
        spark = create_spark_session(warehouse_path)
        spark.conf.set("spark.sql.iceberg.fanout-enabled", "true")
        success = create_partitioned_table(spark, warehouse_path, table_name, pattern, partition_spec)
        return (table_name, success)
    except Exception as e:
        return (table_name, False)
    finally:
        if spark:
            spark.stop()

def create_partitioned_table_worker(spark, warehouse_path, table_name, pattern, partition_spec, delete_existing=True):
    """Worker function for parallel partitioned table creation using shared Spark session"""
    try:
        result = create_partitioned_table(
            spark, warehouse_path, table_name, pattern, partition_spec,
            delete_existing=delete_existing
        )
        return (table_name, result)
    except Exception as e:
        return (table_name, False)

def generate_partitioned_tables(args, spark, warehouse_path):
    """Generate partitioned tables - limited to mixed_chaos pattern only"""
    # Minimal set of 4 essential partition transforms
    minimal_transforms = [
        ["identity(category)"],                    # Direct partitioning
        ["bucket(4, id)"],                       # Hash partitioning
        ["months(timestamp)"],                   # Temporal partitioning
        ["identity(category)", "bucket(4, id)"]  # Multi-column partitioning
    ]

    patterns = ['mixed_chaos']  # Only mixed_chaos pattern
    delete_modes = ['cow', 'posdelete']  # Test both modes

    total = len(minimal_transforms) * len(patterns) * len(delete_modes)
    print(f"Generating {total} partitioned tables (minimal set: mixed_chaos pattern only)...")

    # Build list of all tables to create
    tables = []
    for transform in minimal_transforms:
        for pattern in patterns:
            for delete_mode in delete_modes:
                transform_name = (
                    "_".join(transform).replace('(', '_').replace(')', '')
                    .replace('[', '').replace(']', '').replace(',', '_')
                )
                table_name = f"part_{transform_name}_{pattern}_{delete_mode}"
                tables.append((table_name, pattern, transform))

    created = 0
    if args.num_parallel > 1:
        # Parallel execution
        with ThreadPoolExecutor(max_workers=args.num_parallel) as executor:
            futures = {
                executor.submit(create_partitioned_table_worker_with_session, warehouse_path, name, pat, spec): name
                for name, pat, spec in tables
            }

            for future in as_completed(futures):
                table_name = futures[future]
                try:
                    _, success = future.result()
                    if success:
                        created += 1
                        print(f"✓ Created {table_name} ({created}/{total})")
                    else:
                        print(f"✗ Failed {table_name}")
                except Exception as e:
                    print(f"✗ Failed {table_name}: {e}")
    else:
        # Sequential execution
        if spark is None:
            spark = create_spark_session(warehouse_path)
            should_stop = True
        else:
            should_stop = False

        spark.conf.set("spark.sql.iceberg.fanout-enabled", "true")

        try:
            for i, (table_name, pattern, partition_spec) in enumerate(tables, 1):
                try:
                    if create_partitioned_table(
                        spark, warehouse_path, table_name, pattern, partition_spec,
                        delete_existing=delete_existing
                    ):
                        created += 1
                        print(f"✓ Created {table_name} ({created}/{total})")
                except Exception as e:
                    print(f"✗ Failed {table_name}: {e}")
        finally:
            if should_stop:
                spark.stop()

    print(f"\nCompleted: {created}/{total} partitioned tables created")



def generate_evolution_data(spark, table_name, num_rows, row_offset=0):
    """Generate data for evolution tables with schema-aware column mapping"""

    # Get current table schema if table exists
    current_schema = None
    if table_name:
        try:
            current_schema = spark.table(table_name).schema
            print(f"    → Current schema columns: {[f.name for f in current_schema.fields]}")
        except Exception as e:
            print(f"    → Could not read schema: {e}")

    # Generate base data
    categories = ["A", "B", "C", "D", "E"]

    if current_schema:
        # Use current table schema - create data matching evolved schema
        data = []
        for i in range(num_rows):
            row_id = row_id_from_index(i, row_offset)
            row_values = []

            # Only generate data for columns that exist in current schema
            for field in current_schema.fields:
                field_name = field.name
                if field_name == "id":
                    row_values.append(row_id)
                elif field_name == "name":
                    row_values.append(f"name_{row_id}")
                elif field_name == "full_name":  # Renamed from name
                    row_values.append(f"full_name_{row_id}")
                elif field_name == "category":
                    row_values.append(categories[row_id % len(categories)])
                elif field_name == "value":
                    row_values.append((row_id * 10) % 1000)
                elif field_name == "timestamp":
                    row_values.append(None)  # Will be set using SQL functions
                elif field_name == "active":
                    row_values.append(row_id % 2 == 0)
                elif field_name == "description":
                    row_values.append(f"desc_{row_id}")
                else:
                    # Default values for unknown columns
                    if "string" in str(field.dataType).lower():
                        row_values.append(f"default_{row_id}")
                    elif "bigint" in str(field.dataType).lower() or "long" in str(field.dataType).lower():
                        row_values.append(row_id)
                    elif "int" in str(field.dataType).lower():
                        row_values.append(row_id)
                    elif "boolean" in str(field.dataType).lower():
                        row_values.append(row_id % 2 == 0)
                    else:
                        row_values.append(None)

            data.append(tuple(row_values))

        df = spark.createDataFrame(data, current_schema)
    else:
        # Use base schema for initial creation
        base_schema = StructType([
            StructField("id", LongType(), False),
            StructField("name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("value", IntegerType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("active", BooleanType(), True)
        ])

        data = []
        for i in range(num_rows):
            row_id = row_id_from_index(i, row_offset)
            data.append((
                row_id,
                f"name_{row_id}",
                categories[row_id % len(categories)],
                (row_id * 10) % 1000,
                None,  # Will be set using SQL functions
                row_id % 2 == 0
            ))

        df = spark.createDataFrame(data, base_schema)

    # Add timestamp using SQL functions
    if "timestamp" in [f.name for f in df.schema.fields]:
        df = df.withColumn("timestamp",
                          date_add(lit("2020-01-01"), (col("id") % 1460).cast("int")))

    return df

def create_evolution_table(spark, warehouse_path, table_name, table_spec, delete_existing=True):
    """Create a single evolution table with specified evolution operations"""
    # Get catalog and namespace using the same function as other table types
    catalog, namespace = get_catalog_and_namespace(warehouse_path, "evolution")

    try:
        # Check if table exists and skip if delete_existing is False
        if not delete_existing:
            try:
                spark.sql(f"DESCRIBE TABLE {catalog}.{namespace}.{table_name}")
                print(f"Skipping existing evolution table: {table_name}")
                return True
            except Exception:
                pass  # Table doesn't exist, continue with creation

        print(f"Creating evolution table: {table_name}")

        # Create namespace if not exists
        create_namespace_if_not_exists(spark, catalog, namespace)

        # Drop existing table only if delete_existing is True
        if delete_existing:
            drop_table_if_exists(spark, catalog, namespace, table_name)

        # Get table configuration
        initial_format_version = table_spec.get('initial_format_version', 2)
        schema_type = table_spec.get('schema', 'base')
        partition_spec = table_spec.get('partition_spec') or table_spec.get('initial_partition_spec')
        sort_order = table_spec.get('sort_order') or table_spec.get('initial_sort_order')

        # Build table properties
        props = {
            'format-version': str(initial_format_version),
            'write.distribution-mode': 'none'  # Support all partition transforms
        }

        # Add initial properties
        initial_props = table_spec.get('initial_properties', {})
        if 'compression' in initial_props:
            props['write.parquet.compression-codec'] = initial_props['compression']
        if 'encoding' in initial_props:
            props['write.parquet.encoding'] = initial_props['encoding']
        if 'delete_mode' in initial_props:
            if initial_props['delete_mode'] == 'merge-on-read':
                props.update({
                    'write.delete.mode': 'merge-on-read',
                    'write.update.mode': 'merge-on-read'
                })

        # Create base schema
        base_columns = [
            "id BIGINT",
            "name STRING",
            "category STRING",
            "value INT",
            "timestamp TIMESTAMP",
            "active BOOLEAN"
        ]

        # Add extended schema columns if needed
        if schema_type == 'extended':
            base_columns.append("description STRING")

        schema_sql = ", ".join(base_columns)

        # Build partition clause
        partition_clause = ""
        if partition_spec:
            partition_transforms = []
            for spec in partition_spec:
                partition_transforms.append(spec)
            partition_clause = f"PARTITIONED BY ({', '.join(partition_transforms)})"

        # Build sort clause
        sort_clause = ""
        if sort_order:
            sort_clause = f"TBLPROPERTIES ('write.sort-order' = '{', '.join(sort_order)}')"

        # Build properties clause
        props_items = [f"'{k}' = '{v}'" for k, v in props.items()]
        if sort_clause:
            props_clause = f"TBLPROPERTIES ({', '.join(props_items)})"
        else:
            props_clause = f"TBLPROPERTIES ({', '.join(props_items)})"

        # Create table
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{namespace}.{table_name} (
            {schema_sql}
        ) USING ICEBERG
        {partition_clause}
        {props_clause}
        """

        spark.sql(create_sql)
        print(f"  → Created table {catalog}.{namespace}.{table_name}")

        # Execute evolution sequence
        evolution_ops = []

        # Single operation types
        if 'schema_evolution' in table_spec:
            evolution_ops.extend(table_spec['schema_evolution'])
        if 'partition_evolution' in table_spec:
            evolution_ops.extend(table_spec['partition_evolution'])
        if 'sort_evolution' in table_spec:
            evolution_ops.extend(table_spec['sort_evolution'])
        if 'property_evolution' in table_spec:
            evolution_ops.extend(table_spec['property_evolution'])
        if 'metadata_evolution' in table_spec:
            evolution_ops.extend(table_spec['metadata_evolution'])

        # Combined evolution sequence
        if 'evolution_sequence' in table_spec:
            evolution_ops.extend(table_spec['evolution_sequence'])

        # Execute evolution operations with data insertions
        total_rows = 0
        target_rows = 1000
        full_table_name = f"{catalog}.{namespace}.{table_name}"

        # Initial data insertion
        if not evolution_ops:
            # Simple case: just insert 1000 rows
            initial_df = generate_evolution_data(spark, full_table_name, target_rows, 0)
            initial_df.writeTo(full_table_name).append()
        else:
            # Complex case: insert data between evolution operations
            rows_per_step = target_rows // (len(evolution_ops) + 1)

            # Initial data - use table schema for extended tables
            initial_df = generate_evolution_data(spark, full_table_name, rows_per_step, total_rows)
            initial_df.writeTo(full_table_name).append()
            total_rows += rows_per_step

            # Execute evolution operations
            for i, op in enumerate(evolution_ops):
                execute_evolution_operation(spark, catalog, namespace, table_name, op)

                # Insert data after evolution (except last operation)
                if i < len(evolution_ops) - 1:
                    step_df = generate_evolution_data(spark, full_table_name, rows_per_step, total_rows)
                    step_df.writeTo(full_table_name).append()
                    total_rows += rows_per_step

            # Final data to reach exactly 1000 rows
            remaining_rows = target_rows - total_rows
            if remaining_rows > 0:
                final_df = generate_evolution_data(spark, full_table_name, remaining_rows, total_rows)
                final_df.writeTo(full_table_name).append()

        print(f"  ✓ Created evolution table {table_name} with {target_rows} rows")
        return True

    except Exception as e:
        print(f"  ✗ Failed to create evolution table {table_name}: {e}")
        return False

def execute_evolution_operation(spark, catalog, namespace, table_name, operation):
    """Execute a single evolution operation"""
    full_table_name = f"{catalog}.{namespace}.{table_name}"
    op_type = operation['operation']

    try:
        if op_type == 'upgrade_format':
            # Upgrade table format version
            to_version = operation['to_version']
            spark.sql(f"ALTER TABLE {full_table_name} SET TBLPROPERTIES ('format-version' = '{to_version}')")
            print(f"    → Upgraded format to v{to_version}")

        elif op_type == 'add_column':
            # Add new column
            column = operation['column']
            col_type = operation['type']
            spark.sql(f"ALTER TABLE {full_table_name} ADD COLUMN {column} {col_type}")
            print(f"    → Added column {column} {col_type}")

        elif op_type == 'drop_column':
            # Drop column
            column = operation['column']
            spark.sql(f"ALTER TABLE {full_table_name} DROP COLUMN {column}")
            print(f"    → Dropped column {column}")

        elif op_type == 'rename_column':
            # Rename column
            old_name = operation['old_name']
            new_name = operation['new_name']
            spark.sql(f"ALTER TABLE {full_table_name} RENAME COLUMN {old_name} TO {new_name}")
            print(f"    → Renamed column {old_name} to {new_name}")

        elif op_type == 'widen_type':
            # Widen column type (INT to BIGINT)
            column = operation['column']
            to_type = operation['to_type']
            spark.sql(f"ALTER TABLE {full_table_name} ALTER COLUMN {column} TYPE {to_type}")
            print(f"    → Widened {column} to {to_type}")

        elif op_type == 'add_partition':
            # Add partition field
            field = operation['field']
            spark.sql(f"ALTER TABLE {full_table_name} ADD PARTITION FIELD {field}")
            print(f"    → Added partition field {field}")

        elif op_type == 'drop_partition':
            # Drop partition field
            field = operation['field']
            spark.sql(f"ALTER TABLE {full_table_name} DROP PARTITION FIELD {field}")
            print(f"    → Dropped partition field {field}")

        elif op_type == 'replace_partition':
            # Replace partition transform
            old_field = operation['old_field']
            new_field = operation['new_field']
            spark.sql(f"ALTER TABLE {full_table_name} DROP PARTITION FIELD {old_field}")
            spark.sql(f"ALTER TABLE {full_table_name} ADD PARTITION FIELD {new_field}")
            print(f"    → Replaced partition {old_field} with {new_field}")

        elif op_type == 'add_sort':
            # Add sort order
            fields = operation['fields']
            sort_spec = ', '.join(fields)
            spark.sql(f"ALTER TABLE {full_table_name} WRITE ORDERED BY {sort_spec}")
            print(f"    → Added sort order: {sort_spec}")

        elif op_type == 'change_sort':
            # Change sort order
            new_fields = operation['new_fields']
            sort_spec = ', '.join(new_fields)
            spark.sql(f"ALTER TABLE {full_table_name} WRITE ORDERED BY {sort_spec}")
            print(f"    → Changed sort order: {sort_spec}")

        elif op_type == 'remove_sort':
            # Remove sort order
            spark.sql(f"ALTER TABLE {full_table_name} WRITE UNORDERED")
            print(f"    → Removed sort order")

        elif op_type == 'change_compression':
            # Change compression codec
            to_codec = operation['to']
            spark.sql(
                f"ALTER TABLE {full_table_name} "
                f"SET TBLPROPERTIES ('write.parquet.compression-codec' = '{codec}')"
            )
            print(f"    → Changed compression to {to_codec}")

        elif op_type == 'change_encoding':
            # Change encoding
            to_encoding = operation['to']
            spark.sql(f"ALTER TABLE {full_table_name} SET TBLPROPERTIES ('write.parquet.encoding' = '{to_encoding}')")
            print(f"    → Changed encoding to {to_encoding}")

        elif op_type == 'change_delete_mode':
            # Change delete mode
            to_mode = operation['to']
            if to_mode == 'merge-on-read':
                spark.sql(
                    f"ALTER TABLE {full_table_name} "
                    f"SET TBLPROPERTIES ('write.delete.mode' = '{delete_mode}')"
                )
            else:
                spark.sql(f"ALTER TABLE {full_table_name} SET TBLPROPERTIES ('write.delete.mode' = 'copy-on-write')")
            print(f"    → Changed delete mode to {to_mode}")

        elif op_type == 'add_comments':
            # Add table and column comments
            if 'table_comment' in operation:
                comment = operation['table_comment']
                spark.sql(f"ALTER TABLE {full_table_name} SET TBLPROPERTIES ('comment' = '{comment}')")
            if 'column_comments' in operation:
                for col, comment in operation['column_comments'].items():
                    spark.sql(f"ALTER TABLE {full_table_name} ALTER COLUMN {col} COMMENT '{comment}'")
            print(f"    → Added comments")

        elif op_type == 'add_custom_properties':
            # Add custom properties
            properties = operation['properties']
            prop_items = [f"'{k}' = '{v}'" for k, v in properties.items()]
            spark.sql(f"ALTER TABLE {full_table_name} SET TBLPROPERTIES ({', '.join(prop_items)})")
            print(f"    → Added custom properties")

        else:
            print(f"    → Skipped unsupported operation: {op_type}")

    except Exception as e:
        print(f"    ✗ Evolution operation {op_type} failed: {e}")

def generate_evolution_tables(args, spark, warehouse_path):
    """Generate evolution tables for testing Iceberg evolution features"""

    with open(args.specs, 'r') as f:
        specs = yaml.safe_load(f)

    evolution_tables = specs.get('evolution_tables', [])

    # Apply random selection if specified
    if args.random and args.random < len(evolution_tables):
        evolution_tables = random.sample(evolution_tables, args.random)

    total = len(evolution_tables)
    print(f"Generating {total} evolution tables...")

    created = 0
    for i, table_spec in enumerate(evolution_tables, 1):
        table_name = table_spec['name']
        print(f"Creating evolution table {i}/{total}: {table_name}")

        if create_evolution_table(spark, warehouse_path, table_name, table_spec, delete_existing=True):
            created += 1

    print(f"\nCompleted evolution tables: {created}/{total} tables created successfully")
