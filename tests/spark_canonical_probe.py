"""Renders canonical strings with PySpark and prints them as JSON.

Run as a subprocess by tests/test_canonical_cast.py rather than imported. Spark
allows one SparkContext per process and `getOrCreate` hands back whatever exists
ignoring the config asked for, so a session built here in the test process would
be handed to the modules that need an Iceberg catalog and they would fail. Paying
for a JVM start keeps that out of the suite.

Takes a JSON argument {"zones": [...], "instants": [...], "wall_clocks": [...]}
on stdin and prints {"timestamptz": {zone: [str, ...]}, "timestamp_ntz": [...]}.
"""

import json
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructField,
    StructType,
    TimestampNTZType,
    TimestampType,
)

from iceberg_fuzzer.backend.pyspark.canonical import PySparkCanonicalCast


def render(spark, values, spark_type):
    schema = StructType([StructField('v', spark_type, True)])
    df = spark.createDataFrame([(value,) for value in values], schema)
    expr = PySparkCanonicalCast().cast('v', spark_type)
    return [row[0] for row in df.select(expr).collect()]


def main():
    request = json.loads(sys.stdin.read())
    instants = [datetime.fromisoformat(value) for value in request['instants']]
    wall_clocks = [datetime.fromisoformat(value) for value in request['wall_clocks']]

    spark = (
        SparkSession.builder.appName('canonical-cast-probe')
        .master('local[1]')
        .config('spark.ui.enabled', 'false')
        .config('spark.driver.host', 'localhost')
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel('ERROR')

    result = {'timestamptz': {}}
    for zone in request['zones']:
        spark.conf.set('spark.sql.session.timeZone', zone)
        result['timestamptz'][zone] = render(spark, instants, TimestampType())
    result['timestamp_ntz'] = render(spark, wall_clocks, TimestampNTZType())

    print('CANONICAL_PROBE ' + json.dumps(result))


if __name__ == '__main__':
    main()
