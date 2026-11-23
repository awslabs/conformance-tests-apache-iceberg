#!/usr/bin/env python3
"""
Example compaction operation for use with iceberg-fuzzer workflows.

Invokes Spark's rewriteDataFiles action against a single Iceberg table.
Works with both local filesystem and S3 Tables warehouses.

Usage:
    python3 scripts/compact.py --warehouse /tmp/warehouse --namespace simple --table-name my_table
    python3 scripts/compact.py --warehouse arn:aws:s3tables:us-west-2:123456:bucket/my-bucket --namespace ns --table-name t
"""

import argparse
import os

from pyspark.sql import SparkSession


def create_spark_session(warehouse_path: str) -> SparkSession:
    is_s3_table = warehouse_path.startswith("arn:aws:s3tables:")

    builder = (
        SparkSession.builder.appName("iceberg-fuzzer-compact")
        .master("local[*]")
        .config("spark.ui.enabled", "false")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
    )

    if is_s3_table:
        arn_parts = warehouse_path.split(":")
        region = arn_parts[3]

        builder = (
            builder.config(
                "spark.sql.catalog.s3tables", "org.apache.iceberg.spark.SparkCatalog"
            )
            .config("spark.sql.catalog.s3tables.type", "rest")
            .config(
                "spark.sql.catalog.s3tables.uri",
                f"https://s3tables.{region}.amazonaws.com/iceberg",
            )
            .config("spark.sql.catalog.s3tables.warehouse", warehouse_path)
            .config("spark.sql.catalog.s3tables.rest.sigv4-enabled", "true")
            .config("spark.sql.catalog.s3tables.rest.signing-name", "s3tables")
            .config("spark.sql.catalog.s3tables.rest.signing-region", region)
            .config(
                "spark.sql.catalog.s3tables.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO",
            )
            .config(
                "spark.jars.packages",
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,"
                "org.apache.iceberg:iceberg-aws-bundle:1.10.0",
            )
        )
    else:
        os.makedirs(warehouse_path, mode=0o755, exist_ok=True)
        builder = (
            builder.config(
                "spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog"
            )
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", warehouse_path)
            .config(
                "spark.jars.packages",
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0",
            )
            .config("spark.jars.ivy", "/tmp/.ivy2")
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def compact(spark: SparkSession, warehouse_path: str, namespace: str, table_name: str):
    is_s3_table = warehouse_path.startswith("arn:aws:s3tables:")
    catalog = "s3tables" if is_s3_table else "local"
    qualified = f"{catalog}.{namespace}.{table_name}"

    print(f"Compacting {qualified}...")
    spark.sql(
        f"CALL {catalog}.system.rewrite_data_files(table => '{namespace}.{table_name}')"
    )
    print(f"Compaction complete: {qualified}")


def main():
    parser = argparse.ArgumentParser(description="Compact an Iceberg table using Spark")
    parser.add_argument(
        "--warehouse", required=True, help="Warehouse path or S3 Tables ARN"
    )
    parser.add_argument("--namespace", required=True, help="Table namespace")
    parser.add_argument("--table-name", required=True, help="Table name")
    args = parser.parse_args()

    spark = create_spark_session(args.warehouse)
    try:
        compact(spark, args.warehouse, args.namespace, args.table_name)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
