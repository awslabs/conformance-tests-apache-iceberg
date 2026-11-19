"""PySpark backend — validation via SparkSession table reads."""

import decimal

from pyspark.sql.types import ArrayType, DateType, DecimalType, MapType, StructType, TimestampNTZType, TimestampType

from iceberg_fuzzer.backend import (
    ValidationEngine,
    Checkpoint,
    ColumnFilter,
    ColumnName,
    ColTypeStr,
)
from iceberg_fuzzer.backend.canonical import HASH_MODULUS, Epoch, MapPairs, _apply, hash_rows
from iceberg_fuzzer.backend.point_query import format_sql_literal
from iceberg_fuzzer.backend.pyspark.builders import create_spark_session


# Wide enough for a decimal(38, s) value, where the default context rounds to 28 digits.
_DECIMALS = decimal.Context(prec=76)


def _identifier(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _string(text: str) -> str:
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _exported(sql: str, data_type, depth: int = 0) -> str:
    """A Spark SQL expression of the values of sql, of data_type, as count_and_hash hands them to
    Python: a TIMESTAMP as its microseconds since the epoch, a TIMESTAMP_NTZ as the same count
    taken from its wall clock read as UTC, a DATE as its days since the epoch, and a map as its
    entries, at any depth. No session time zone changes any of them."""
    if isinstance(data_type, TimestampType):
        return f"unix_micros({sql})"
    if isinstance(data_type, TimestampNTZType):
        return f"timestampdiff(MICROSECOND, TIMESTAMP_NTZ'1970-01-01 00:00:00', {sql})"
    if isinstance(data_type, DateType):
        return f"unix_date({sql})"
    if isinstance(data_type, ArrayType):
        element = _exported(f"x{depth}", data_type.elementType, depth + 1)
        return sql if element == f"x{depth}" else f"transform({sql}, x{depth} -> {element})"
    if isinstance(data_type, MapType):
        key = _exported(f"e{depth}.key", data_type.keyType, depth + 1)
        value = _exported(f"e{depth}.value", data_type.valueType, depth + 1)
        if (key, value) == (f"e{depth}.key", f"e{depth}.value"):
            return f"map_entries({sql})"
        return f"transform(map_entries({sql}), e{depth} -> named_struct('key', {key}, 'value', {value}))"
    if isinstance(data_type, StructType):
        fields = [(f.name, f"{sql}.{_identifier(f.name)}", f.dataType) for f in data_type.fields]
        exported = [(name, _exported(field, field_type, depth)) for name, field, field_type in fields]
        if all(value == field for (_, value), (_, field, _) in zip(exported, fields)):
            return sql
        return f"if({sql} IS NULL, NULL, named_struct({', '.join(f'{_string(name)}, {value}' for name, value in exported)}))"
    return sql


def _to_canonical(data_type):
    """A function that puts the values of data_type, as _exported hands them over, in the form the
    shared encoding takes, or None if they need none: timestamps and dates tagged, maps as their
    pairs, and decimals at their type's scale, since PySpark hands a decimal whose exponent is
    below -6 in engineering notation, -1E-10 as -100E-12. Structs come back as dicts, which hash
    like Rows."""
    if isinstance(data_type, (TimestampType, TimestampNTZType)):
        return lambda micros: Epoch(b"T", micros)
    if isinstance(data_type, DateType):
        return lambda days: Epoch(b"d", days)
    if isinstance(data_type, DecimalType):
        exponent = decimal.Decimal(1).scaleb(-data_type.scale)
        return lambda value: value.quantize(exponent, context=_DECIMALS)
    if isinstance(data_type, MapType):
        key, value = _to_canonical(data_type.keyType), _to_canonical(data_type.valueType)
        return lambda entries: MapPairs((_apply(key, entry[0]), _apply(value, entry[1])) for entry in entries)
    if isinstance(data_type, StructType):
        fields = [(f.name, _to_canonical(f.dataType)) for f in data_type.fields]
        if not any(convert for _, convert in fields):
            return None
        return lambda row: {name: _apply(convert, row[i]) for i, (name, convert) in enumerate(fields)}
    if isinstance(data_type, ArrayType):
        element = _to_canonical(data_type.elementType)
        return element and (lambda values: [_apply(element, v) for v in values])
    return None


def _canonical_rows(rows, converters: list[tuple[int, object]]):
    for row in rows:
        values = list(row)
        for i, convert in converters:
            values[i] = _apply(convert, values[i])
        yield values


def count_and_hash(frame) -> tuple[int, int]:
    """Row count and content hash of a DataFrame's rows. Each Spark task hashes its rows with the
    shared encoding in canonical.py, and the driver adds the partial results. The rows go through
    the RDD API, because mapInArrow needs pandas. Spark hands them over as _exported gives them, so
    no timestamp or date goes through Python's datetime, whose years end at 9999 and which a
    Python worker would read in its own time zone, and no map key needs to be hashable."""
    converters = [(i, convert) for i, f in enumerate(frame.schema) if (convert := _to_canonical(f.dataType))]
    exported = frame.selectExpr(*(_exported(_identifier(f.name), f.dataType) for f in frame.schema))
    partials = exported.rdd.mapPartitions(lambda rows: [hash_rows(_canonical_rows(rows, converters))]).collect()
    return sum(rows for rows, _ in partials), sum(h for _, h in partials) % HASH_MODULUS


class PySparkValidation(ValidationEngine):
    """PySpark-based validation engine using Spark SQL for table access, hashing rows as
    count_and_hash does."""

    def __init__(self, warehouse_path: str):
        self._warehouse_path = warehouse_path
        self._spark = None

    def _get_spark(self):
        if self._spark is None:
            self._spark = create_spark_session(self._warehouse_path)
        return self._spark

    def _resolve_table_ref(self, namespace: str, table_name: str) -> str:
        is_s3 = self._warehouse_path.startswith('arn:aws:s3tables:')
        catalog = "s3tables" if is_s3 else "local"
        return f"{catalog}.{namespace}.{table_name}"

    def _count_and_hash(self, table_ref: str) -> tuple[int, int]:
        return count_and_hash(self._get_spark().table(table_ref))

    def capture_checkpoint(self, warehouse: str, namespace: str, table_name: str) -> Checkpoint:
        table_ref = self._resolve_table_ref(namespace, table_name)
        row_count, content_hash = self._count_and_hash(table_ref)
        return Checkpoint(row_count=row_count, content_hash=content_hash)

    def filtered_count(
        self,
        warehouse: str,
        namespace: str,
        table_name: str,
        column_filter: ColumnFilter,
    ) -> int:
        table_ref = self._resolve_table_ref(namespace, table_name)
        spark = self._get_spark()
        col = f"`{column_filter.column}`"
        if column_filter.is_null:
            where = f"{col} IS NULL"
        else:
            where = f"{col} = {format_sql_literal(column_filter.value)}"
        query = f"SELECT COUNT(*) FROM {table_ref} WHERE {where}"
        return spark.sql(query).collect()[0][0]

    def get_columns(self, warehouse: str, namespace: str, table_name: str) -> list[tuple[ColumnName, ColTypeStr]]:
        spark = self._get_spark()
        table_ref = self._resolve_table_ref(namespace, table_name)
        schema = spark.table(table_ref).schema
        return [(f.name, f.dataType.simpleString()) for f in schema if f.name != "id"]

    def get_row_ids(self, warehouse: str, namespace: str, table_name: str) -> set[int]:
        table_ref = self._resolve_table_ref(namespace, table_name)
        spark = self._get_spark()
        rows = spark.sql(f"SELECT id FROM {table_ref}").collect()
        return {row.id for row in rows}

    def close(self):
        if self._spark is not None:
            self._spark.stop()
            self._spark = None
