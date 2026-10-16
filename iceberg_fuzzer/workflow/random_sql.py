"""Seeded random workflows that build their own table with SQL.

`generate_workflow` turns a seed into a workflow: a table with a random schema, partition spec
and properties, then appends, deletes, updates, schema changes and compactions in a random order.
Every value is a deterministic function of the row id and the seed, so the same seed and
GENERATOR_VERSION always give the same workflow and the same rows, and a failure replays exactly.

Each dimension, such as the schema, the writers or the deletes, draws from its own random stream,
seeded by the base version, the seed and the dimension's name, and the dimension's own version
once it has changed. A new dimension gets a new stream, and a changed one a new version, so
either leaves every other dimension's choices for every seed unchanged. A dimension is a stream name, a share or bounds in
GeneratorConfig, a method of _Builder that adds its steps, and a feature for pairwise selection.

Compaction steps expect `replace_or_unchanged`, because a compactor may decline a generated
table. Either way every reader must return the rows it returned before the step.
"""

import itertools
import random
from collections.abc import Iterable
from dataclasses import dataclass, field, replace

import yaml

# Seeds every dimension's stream. Changing it changes every seed's workflow.
BASE_VERSION = 5
# Dimensions changed since BASE_VERSION, with their own version, so a change to one dimension
# reseeds only that dimension's stream.
DIMENSION_VERSIONS: dict[str, int] = {"filters": 1}
GENERATOR_VERSION = str(BASE_VERSION) + "".join(f"+{d}{v}" for d, v in sorted(DIMENSION_VERSIONS.items()))

TABLE = "${table}"
# Spark rejects writing expressions it cannot prove non-null into required columns.
ALLOW_REQUIRED = "SET `spark.sql.iceberg.check-nullability`=false"
# Spark's vectorized Iceberg reader cannot read the DELTA_LENGTH_BYTE_ARRAY strings in DuckDB's
# data page v2 files, nor, in Iceberg 1.10.0, DuckDB's int columns once promoted to BIGINT.
SCALAR_SPARK_READS = "SET `spark.sql.iceberg.vectorization.enabled`=false"
# The split range() gets by default from the fuzzer's spark.default.parallelism, written out.
INPUT_PARTITIONS = 8
IN_PROCESS_SPARK_COMPACTION = "CALL ${catalog}.system.rewrite_data_files(table => '${namespace}.${table_name}')"
# Spark rewrites delete files separately; with it, a Spark compaction also merges position deletes,
# as the compactor under test does, so its delete files are a reference for theirs.
IN_PROCESS_SPARK_DELETE_REWRITE = "CALL ${catalog}.system.rewrite_position_delete_files(table => '${namespace}.${table_name}')"
# Rewrites every manifest with the current schema ID, so files written under an earlier schema sit
# in manifests that claim the current one, as Spark's manifest merging also leaves them.
REWRITE_MANIFESTS = "CALL ${catalog}.system.rewrite_manifests(table => '${namespace}.${table_name}')"

PRIMITIVE_TYPES = ("boolean", "int", "long", "float", "double", "decimal", "date", "timestamp", "string", "binary")
NESTED_TYPES = ("struct", "list", "map")
PARTITION_KINDS = ("identity", "bucket", "truncate", "years", "months", "days", "hours")
CODECS = ("zstd", "snappy", "gzip", "uncompressed")
METRICS_MODES = ("truncate(16)", "full", "counts", "none")
DECIMALS = ((10, 2), (18, 6), (38, 10))
SCHEMA_CHANGES = ("add_column", "rename_column", "promote", "drop_column")
TIMESTAMP_VALUE = "timestamp_micros(-315619200000000 + CAST({hash} AS BIGINT) * 1000003)"


@dataclass(frozen=True)
class GeneratorConfig:
    """Bounds for the dimensions a seed picks from."""

    max_columns: int = 8
    max_commits: int = 6
    # Rows per commit are log-uniform between these bounds.
    min_rows_per_commit: int = 1
    max_rows_per_commit: int = 20_000
    max_files_per_commit: int = 6
    nested_share: float = 0.3
    # Nesting two levels deep, which some compactors decline.
    deep_nesting_share: float = 0.1
    partitioned_share: float = 0.6
    copy_on_write_share: float = 0.2
    delete_share: float = 0.5
    update_share: float = 0.15
    edge_value_share: float = 0.3
    mid_compaction_share: float = 0.3
    spark_compaction_share: float = 0.25
    # A schema change after a commit, from the second commit on, and how often the manifests are
    # then rewritten with the new schema ID.
    schema_change_share: float = 0.3
    rewrite_manifests_share: float = 0.5
    # Commits written and committed by PyIceberg instead of Spark, on local warehouses. Partition
    # transforms other than identity need PyIceberg's pyiceberg-core package.
    pyiceberg_writer_share: float = 0.0
    # Commits whose files DuckDB writes, with random Parquet options, registered by PyIceberg.
    duckdb_writer_share: float = 0.0
    codecs: tuple[str, ...] = CODECS
    compaction_timeout: int = 1800


@dataclass(frozen=True)
class Column:
    name: str
    sql_type: str
    expression: str
    nullable: bool = True
    partition_source: bool = False


@dataclass
class GeneratedWorkflow:
    seed: int
    definition: dict
    # Human-readable description, written as the YAML header.
    dimensions: dict = field(default_factory=dict)
    # One categorical value per dimension, for choosing seeds that cover combinations.
    features: dict[str, str] = field(default_factory=dict)
    # Predicates to read the table through as well as in full, since a filter makes readers prune
    # files and manifests by their metadata.
    filters: tuple[str, ...] = ()

    def to_yaml(self) -> str:
        header = [f"# Generated by iceberg_fuzzer.workflow.random_sql, version {GENERATOR_VERSION}, seed {self.seed}."]
        header += [f"# {key}: {value}" for key, value in self.dimensions.items()]
        header += [f"# row filter: {f}" for f in self.filters]
        return "\n".join(header) + "\n" + yaml.safe_dump(self.definition, sort_keys=False, width=200)


def _hash(salt: int, modulo: int) -> str:
    return f"pmod(hash(id, {salt}), {modulo})"


def _primitive(kind: str, salt: int, rng: random.Random) -> tuple[str, str, list[str]]:
    """SQL type, value expression and edge values for one primitive column."""
    if kind == "boolean":
        return "BOOLEAN", f"{_hash(salt, 2)} = 0", []
    if kind == "int":
        return "INT", f"CAST({_hash(salt, 2000)} - 1000 AS INT)", ["2147483647", "-2147483648", "0"]
    if kind == "long":
        return (
            "BIGINT", f"CAST(hash(id, {salt}) AS BIGINT) * 4294967296 + {_hash(salt + 1, 1000)}",
            ["9223372036854775807", "-9223372036854775808"],
        )
    if kind == "float":
        return (
            "FLOAT", f"CAST(hash(id, {salt}) AS FLOAT) / 7",
            ["CAST('NaN' AS FLOAT)", "CAST('-0.0' AS FLOAT)", "CAST('Infinity' AS FLOAT)"],
        )
    if kind == "double":
        return (
            "DOUBLE", f"CAST(hash(id, {salt}) AS DOUBLE) / 13",
            ["CAST('NaN' AS DOUBLE)", "CAST('-0.0' AS DOUBLE)", "CAST('-Infinity' AS DOUBLE)"],
        )
    if kind == "decimal":
        precision, scale = rng.choice(DECIMALS)
        sql_type = f"DECIMAL({precision},{scale})"
        return (
            sql_type, f"CAST(({_hash(salt, 20_000_000)} - 10000000) / 100 AS {sql_type})",
            [f"CAST(0 AS {sql_type})", f"CAST(-1 AS {sql_type})"],
        )
    if kind == "date":
        return (
            "DATE", f"date_add(DATE'1960-01-01', {_hash(salt, 40_000)})",
            ["DATE'2024-02-29'", "DATE'1969-12-31'", "DATE'1970-01-01'"],
        )
    if kind == "timestamp":
        return (
            "TIMESTAMP", TIMESTAMP_VALUE.format(hash=_hash(salt, 2_000_000_000)),
            ["TIMESTAMP'1969-12-31 23:59:59.999999'", "TIMESTAMP'2024-02-29 12:00:00'"],
        )
    if kind == "string":
        cardinality = rng.choice((5, 1000, 1_000_000))
        return (
            "STRING", f"CONCAT('s', CAST({_hash(salt, cardinality)} AS STRING))",
            ["''", "repeat('z', 40)", "'mixed Case and space'"],
        )
    if kind == "binary":
        return "BINARY", f"CAST(CONCAT('b', CAST({_hash(salt, 100_000)} AS STRING)) AS BINARY)", ["X''", "X'00'", "X'00FF00'"]
    raise ValueError(f"Unknown primitive type {kind}")


def _with_edges(expression: str, edges: list[str], nullable: bool, salt: int, share: float, rng: random.Random) -> str:
    """Replace a small share of values with nulls and the type's edge values."""
    choices = (["NULL"] if nullable else []) + (edges if rng.random() < share else [])
    if not choices:
        return expression
    cases = " ".join(f"WHEN {i} THEN {value}" for i, value in enumerate(choices))
    return f"CASE {_hash(salt, 40)} {cases} ELSE {expression} END"


def _nested(kind: str, salt: int, deep: bool) -> tuple[str, str]:
    if kind == "struct":
        if deep:
            return "STRUCT<a: INT, b: STRUCT<x: STRING>>", (
                f"named_struct('a', CAST({_hash(salt, 100)} AS INT), "
                f"'b', named_struct('x', CONCAT('x', CAST({_hash(salt + 1, 50)} AS STRING))))"
            )
        return "STRUCT<a: INT, b: STRING, t: TIMESTAMP>", (
            f"named_struct('a', CAST({_hash(salt, 100)} AS INT), 'b', CONCAT('b', CAST({_hash(salt + 1, 50)} AS STRING)), "
            f"'t', {TIMESTAMP_VALUE.format(hash=_hash(salt + 2, 2_000_000_000))})"
        )
    if kind == "list":
        return "ARRAY<INT>", f"array(CAST({_hash(salt, 100)} AS INT), CAST({_hash(salt + 1, 100)} AS INT))"
    return "MAP<STRING, INT>", f"map(CONCAT('k', CAST({_hash(salt, 5)} AS STRING)), CAST({_hash(salt + 1, 100)} AS INT))"


def _partition_column(kind: str, salt: int, rng: random.Random) -> tuple[Column, str]:
    """A dedicated source column with bounded cardinality, and the PARTITIONED BY clause."""
    if kind == "identity":
        values = rng.randint(2, 8)
        column = Column("p_key", "STRING", f"CONCAT('p', CAST({_hash(salt, values)} AS STRING))", partition_source=True)
        return column, "PARTITIONED BY (p_key)"
    if kind == "bucket":
        column = Column("p_key", "BIGINT", "id * 7919", partition_source=True)
        return column, f"PARTITIONED BY (bucket({rng.choice((2, 4, 16))}, p_key))"
    if kind == "truncate":
        column = Column("p_key", "STRING", f"CONCAT(CAST({_hash(salt, 6)} AS STRING), 'abcdef')", partition_source=True)
        return column, f"PARTITIONED BY (truncate({rng.choice((1, 4))}, p_key))"
    span = {"years": 4 * 365 * 86400, "months": 180 * 86400, "days": 10 * 86400, "hours": 24 * 3600}[kind]
    column = Column(
        "p_ts", "TIMESTAMP", f"timestamp_seconds(1704067200 + {_hash(salt, span)})", nullable=False, partition_source=True,
    )
    return column, f"PARTITIONED BY ({kind}(p_ts))"


class _Streams:
    """One random stream per dimension of a seed's workflow."""

    def __init__(self, seed: int):
        self._seed = seed
        self._streams: dict[str, random.Random] = {}

    def __getitem__(self, dimension: str) -> random.Random:
        if dimension not in self._streams:
            version = f":{DIMENSION_VERSIONS[dimension]}" if dimension in DIMENSION_VERSIONS else ""
            self._streams[dimension] = random.Random(f"{BASE_VERSION}:{self._seed}:{dimension}{version}")
        return self._streams[dimension]


def _columns(rng: random.Random, config: GeneratorConfig) -> list[Column]:
    columns = [Column("id", "BIGINT", "id", nullable=False)]
    nested = rng.random() < config.nested_share
    deep = nested and rng.random() < config.deep_nesting_share
    for i in range(rng.randint(1, config.max_columns)):
        salt = 101 + i * 10
        nullable = rng.random() < 0.7
        if nested and i == 0:
            kind = rng.choice(NESTED_TYPES)
            sql_type, expression = _nested(kind, salt, deep and kind == "struct")
            columns.append(Column(f"c{i}_{kind}", sql_type, expression, nullable))
            continue
        kind = rng.choice(PRIMITIVE_TYPES)
        sql_type, expression, edges = _primitive(kind, salt, rng)
        expression = _with_edges(expression, edges, nullable, salt + 7, config.edge_value_share, rng)
        columns.append(Column(f"c{i}_{kind}", sql_type, expression, nullable))
    return columns


def _partitioning(rng: random.Random, config: GeneratorConfig) -> tuple[str, Column | None, str]:
    """The partition transform, its source column, which goes last, and the PARTITIONED BY clause."""
    if rng.random() >= config.partitioned_share:
        return "none", None, ""
    kind = rng.choice(PARTITION_KINDS)
    source, clause = _partition_column(kind, 991, rng)
    return kind, source, clause


def _properties(rng: random.Random, config: GeneratorConfig) -> dict:
    mode = "copy-on-write" if rng.random() < config.copy_on_write_share else "merge-on-read"
    return {
        "format-version": "2",
        "write.delete.mode": mode,
        "write.update.mode": mode,
        "write.parquet.compression-codec": rng.choice(config.codecs),
        "write.metadata.metrics.default": rng.choice(METRICS_MODES),
    }


def _select(columns: list[Column], start: int, end: int, files: int) -> str:
    values = ", ".join(c.expression for c in columns)
    # A fixed split of the input, so the rows each file gets do not depend on the Spark session.
    return f"SELECT /*+ REPARTITION({files}) */ {values} FROM range({start}, {end}, 1, {INPUT_PARTITIONS})"


def _insert(columns: list[Column], start: int, end: int, files: int) -> str:
    return f"INSERT INTO {TABLE} {_select(columns, start, end, files)}"


def _duckdb_copy(columns: list[Column], start: int, end: int, files: int, rng: random.Random, config: GeneratorConfig) -> str:
    options = (
        f"COMPRESSION {rng.choice(config.codecs)}, PARQUET_VERSION {rng.choice(('v1', 'v2'))}, "
        f"ROW_GROUP_SIZE {rng.choice((1024, 10_000, 122_880))}"
    )
    return f"COPY ({_select(columns, start, end, files)}) TO {TABLE} ({options})"


def _delete(rng: random.Random, commits: list[tuple[int, int]], salt: int) -> tuple[str, str]:
    start, end = rng.choice(commits)
    kind = rng.choice(("one_row", "share", "whole_commit"))
    if kind == "one_row":
        return f"DELETE FROM {TABLE} WHERE id = {rng.randrange(start, end)}", kind
    if kind == "share":
        return f"DELETE FROM {TABLE} WHERE {_hash(salt, 100)} < {rng.choice((1, 10, 50))}", kind
    return f"DELETE FROM {TABLE} WHERE id BETWEEN {start} AND {end - 1}", kind


def _update(columns: list[Column], salt: int) -> str | None:
    targets = [c for c in columns if c.sql_type == "STRING" and not c.partition_source]
    if not targets:
        return None
    return f"UPDATE {TABLE} SET {targets[0].name} = 'updated' WHERE {_hash(salt, 100)} < 5"


def _filters(rng: random.Random, columns: list[Column], partition_kind: str, total_rows: int) -> tuple[str, ...]:
    """An id range on every table, and a predicate on the partition source or another column."""
    low = rng.randrange(max(total_rows, 1))
    filters = [f"id >= {low} AND id <= {low + rng.randint(0, max(total_rows // 4, 1))}"]
    source = next((c for c in columns if c.partition_source), None)
    if source is not None and partition_kind in ("identity", "truncate"):
        value = f"p{rng.randrange(8)}" if partition_kind == "identity" else f"{rng.randrange(6)}abcdef"
        filters.append(f"{source.name} = '{value}'")
    elif source is not None and partition_kind == "bucket":
        filters.append(f"{source.name} = {rng.randrange(max(total_rows, 1)) * 7919}")
    else:
        numeric = [c for c in columns if c.sql_type in ("INT", "BIGINT") and c.name != "id"]
        if numeric:
            filters.append(f"{rng.choice(numeric).name} < 0")
    return tuple(filters)


# Widening a column's own type, or a nested int's, as Iceberg allows: a nested path and the type
# text that changes in the column's type.
_PROMOTIONS = {
    "INT": ("", "BIGINT"),
    "FLOAT": ("", "DOUBLE"),
    "STRUCT<a: INT, b: STRING, t: TIMESTAMP>": (".a", "STRUCT<a: BIGINT, b: STRING, t: TIMESTAMP>"),
    "ARRAY<INT>": (".element", "ARRAY<BIGINT>"),
    # Not MAP<STRING, INT>: reading a promoted map value through PyIceberg aborts the process in
    # the runtime's Arrow C++ build, on a check that the map's keys have no nulls.
}


def _promotion(column: Column) -> tuple[str, str, str] | None:
    """The ALTER COLUMN path, the new type for the statement and the column's new SQL type."""
    if column.partition_source:
        return None
    if column.sql_type.startswith("DECIMAL("):
        precision, scale = (int(part) for part in column.sql_type[len("DECIMAL("):-1].split(","))
        if precision == 38:
            return None
        widened = f"DECIMAL({min(38, precision + 8)},{scale})"
        return column.name, widened, widened
    if column.sql_type not in _PROMOTIONS:
        return None
    path, new_type = _PROMOTIONS[column.sql_type]
    return f"{column.name}{path}", "BIGINT" if path else new_type, new_type


def _schema_change(rng: random.Random, columns: list[Column], commit: int, config: GeneratorConfig) -> tuple[str, str, list[Column]]:
    """One ALTER TABLE statement, a description and the columns after it. Added columns go last,
    where Spark adds them, so positional INSERT ... SELECT statements stay aligned.

    Dropping the column added last, or renaming a column back, makes the schema equal to an
    earlier one, and Iceberg then reuses that schema's ID. Files written in between then belong to
    a schema that is no longer any table schema's, while the table looks unchanged by ID."""
    kind = rng.choice(SCHEMA_CHANGES)
    droppable = [i for i, c in enumerate(columns) if i and not c.partition_source]
    if kind == "drop_column" and droppable:
        added = [i for i in droppable if columns[i].name.startswith("a")]
        i = added[-1] if added and rng.random() < 0.5 else rng.choice(droppable)
        return f"ALTER TABLE {TABLE} DROP COLUMN {columns[i].name}", f"drop {columns[i].name}", columns[:i] + columns[i + 1:]
    renamed = [i for i, c in enumerate(columns) if c.name.startswith("r") and "_" in c.name]
    if kind == "rename_column" and renamed and rng.random() < 0.5:
        i = rng.choice(renamed)
        original = columns[i].name.split("_", 1)[1]
        if original not in {c.name for c in columns}:
            statement = f"ALTER TABLE {TABLE} RENAME COLUMN {columns[i].name} TO {original}"
            return statement, f"rename {columns[i].name} back to {original}", columns[:i] + [replace(columns[i], name=original)] + columns[i + 1:]
    promotable = [(i, p) for i, c in enumerate(columns) if (p := _promotion(c))]
    if kind == "promote" and promotable:
        i, (path, statement_type, new_type) = rng.choice(promotable)
        columns = columns[:i] + [replace(columns[i], sql_type=new_type)] + columns[i + 1:]
        return f"ALTER TABLE {TABLE} ALTER COLUMN {path} TYPE {statement_type}", f"promote {path} to {statement_type}", columns
    if kind == "rename_column" and len(columns) > 1:
        i = rng.randrange(1, len(columns))
        new_name = f"r{commit}_{columns[i].name}"
        statement = f"ALTER TABLE {TABLE} RENAME COLUMN {columns[i].name} TO {new_name}"
        description = f"rename {columns[i].name} to {new_name}"
        return statement, description, columns[:i] + [replace(columns[i], name=new_name)] + columns[i + 1:]
    primitive = rng.choice(PRIMITIVE_TYPES)
    salt = 7001 + commit * 10
    sql_type, expression, edges = _primitive(primitive, salt, rng)
    added = Column(f"a{commit}_{primitive}", sql_type, _with_edges(expression, edges, True, salt + 7, config.edge_value_share, rng))
    return f"ALTER TABLE {TABLE} ADD COLUMN {added.name} {sql_type}", f"add {added.name} {sql_type}", columns + [added]


def _compaction(step_id: str, command: str, expect: str | None, timeout: int) -> dict:
    config = {"command": command, "timeout": timeout}
    if expect:
        config["expect"] = expect
    return {"id": step_id, "operation": "replace", "config": config}


class _Builder:
    """The steps of one workflow. Each method adds one kind of step and draws only from its own
    dimension's stream."""

    def __init__(self, streams: _Streams, columns: list[Column], config: GeneratorConfig,
                 compact_command: str, spark_compact_command: str | None):
        self.streams, self.columns, self.config = streams, columns, config
        self.compact_command, self.spark_compact_command = compact_command, spark_compact_command
        self.steps: list[dict] = []
        self.statements: list[str] = []
        self.commits: list[tuple[int, int]] = []
        self.events: list[str] = []
        self.schema_changes: list[str] = []
        self.next_id = 0
        # Every name a rename has taken a column away from, even one renamed back later.
        self.renamed: set[str] = set()
        self.duckdb_written = False
        # Spark's vectorized reader cannot read some DuckDB files, see SCALAR_SPARK_READS.
        self.scalar_reads = False

    def _flush(self):
        if self.statements:
            self.steps.append({"id": f"sql_{len(self.steps) + 1}", "operation": "sql",
                               "config": {"backend": "pyspark", "statements": [ALLOW_REQUIRED, *self.statements]}})
            self.statements.clear()

    def write(self):
        sizes, writers, config = self.streams["commit_sizes"], self.streams["writers"], self.config
        rows = max(1, int(config.min_rows_per_commit * (config.max_rows_per_commit / config.min_rows_per_commit) ** sizes.random()))
        files = sizes.randint(1, config.max_files_per_commit)
        start, end = self.next_id, self.next_id + rows
        writer = writers.random()
        if writer < config.pyiceberg_writer_share:
            self._flush()
            self.steps.append({"id": f"pyiceberg_{len(self.steps) + 1}", "operation": "sql",
                               "config": {"backend": "pyiceberg", "statements": [_insert(self.columns, start, end, files)]}})
            self.events.append(f"PyIceberg APPEND {rows} rows")
        elif writer < config.pyiceberg_writer_share + config.duckdb_writer_share:
            self._flush()
            statement = _duckdb_copy(self.columns, start, end, files, writers, config)
            self.duckdb_written = True
            self.scalar_reads = self.scalar_reads or "PARQUET_VERSION v2" in statement or "promote" in self.schema_changes
            self.steps.append({"id": f"duckdb_{len(self.steps) + 1}", "operation": "sql",
                               "config": {"backend": "duckdb", "statements": [statement]}})
            self.events.append(f"DuckDB APPEND {rows} rows, {statement.rsplit('(', 1)[1].rstrip(')')}")
        else:
            self.statements.append(_insert(self.columns, start, end, files))
            self.events.append(f"APPEND {rows} rows in up to {files} files")
        self.commits.append((start, end))
        self.next_id = end

    def delete(self, commit: int):
        rng = self.streams["deletes"]
        if rng.random() < self.config.delete_share:
            statement, kind = _delete(rng, self.commits, 5000 + commit)
            self.statements.append(statement)
            self.events.append(f"DELETE {kind}")

    def update(self, commit: int):
        if self.streams["updates"].random() < self.config.update_share:
            statement = _update(self.columns, 6000 + commit)
            if statement:
                self.statements.append(statement)
                self.events.append("UPDATE")

    def change_schema(self, commit: int):
        rng = self.streams["schema_changes"]
        if not commit or rng.random() >= self.config.schema_change_share:
            return
        statement, description, self.columns = _schema_change(rng, self.columns, commit, self.config)
        self.statements.append(statement)
        self.events.append(f"ALTER {description}")
        self.schema_changes.append(description.split()[0])
        if description.startswith("rename"):
            self.renamed.add(description.split()[1])
        self.scalar_reads = self.scalar_reads or (self.duckdb_written and description.startswith("promote"))
        if rng.random() < self.config.rewrite_manifests_share:
            self.statements.append(REWRITE_MANIFESTS)
            self.events.append("REWRITE MANIFESTS")

    def compact(self, commit: int):
        rng, config = self.streams["compactions"], self.config
        if not commit or rng.random() >= config.mid_compaction_share:
            return
        self._flush()
        if self.spark_compact_command and rng.random() < config.spark_compaction_share:
            if self.scalar_reads:
                self.steps.append({"id": f"spark_compact_{len(self.steps) + 1}", "operation": "sql",
                                   "config": {"backend": "pyspark", "statements": [IN_PROCESS_SPARK_COMPACTION, IN_PROCESS_SPARK_DELETE_REWRITE]}})
                self.events.append("Spark COMPACT in process")
            else:
                self.steps.append(_compaction(f"spark_compact_{len(self.steps) + 1}", self.spark_compact_command, None, config.compaction_timeout))
                self.events.append("Spark COMPACT")
        else:
            self.final_compaction()

    def final_compaction(self):
        self._flush()
        self.steps.append(_compaction(f"compact_{len(self.steps) + 1}", self.compact_command, "replace_or_unchanged", self.config.compaction_timeout))
        self.events.append("COMPACT")


def generate_workflow(
    seed: int,
    compact_command: str,
    spark_compact_command: str | None = None,
    config: GeneratorConfig = GeneratorConfig(),
) -> GeneratedWorkflow:
    """Build the workflow for one seed.

    compact_command compacts with the compactor under test and must leave the table either
    with a new replace snapshot or unchanged. spark_compact_command, when given, is a control
    compaction that some workflows run between commits.
    """
    streams = _Streams(seed)
    columns = _columns(streams["schema"], config)
    partition_kind, source, partition_clause = _partitioning(streams["partitioning"], config)
    if source is not None:
        columns.append(source)
    created = columns
    properties = _properties(streams["properties"], config)
    definitions = ", ".join(f"{c.name} {c.sql_type}{'' if c.nullable else ' NOT NULL'}" for c in columns)
    tblproperties = ", ".join(f"'{k}'='{v}'" for k, v in properties.items())
    create = f"CREATE TABLE {TABLE} ({definitions}) USING iceberg {partition_clause} TBLPROPERTIES ({tblproperties})"

    builder = _Builder(streams, columns, config, compact_command, spark_compact_command)
    for commit in range(streams["commit_count"].randint(1, config.max_commits)):
        builder.write()
        builder.delete(commit)
        builder.update(commit)
        builder.change_schema(commit)
        builder.compact(commit)
    builder.final_compaction()
    events, columns = builder.events, builder.columns

    dimensions = {
        "columns": definitions,
        "partition": partition_clause.removeprefix("PARTITIONED BY ") or "none",
        "properties": ", ".join(f"{k}={v}" for k, v in properties.items() if k != "format-version"),
        "workflow": ", ".join(events),
    }
    definition = {
        "name": f"random_{seed}",
        "setup": {"backend": "pyspark", "statements": [SCALAR_SPARK_READS, create] if builder.scalar_reads else [create]},
        "steps": builder.steps,
    }
    nested = next((c.name.split("_", 1)[1] for c in created if c.name.split("_", 1)[-1] in NESTED_TYPES), "none")
    writers = {e.split(" APPEND")[0] if " APPEND" in e else "Spark" for e in events if "APPEND" in e}
    commits = builder.commits
    features = {
        "partition": partition_kind,
        "nested": nested,
        "delete_mode": properties["write.delete.mode"],
        "codec": properties["write.parquet.compression-codec"],
        "metrics": properties["write.metadata.metrics.default"],
        "writers": "+".join(sorted(writers)),
        "commits": "1" if len(commits) == 1 else "2-3" if len(commits) <= 3 else "4+",
        "deletes": "+".join(sorted({e.split()[1] for e in events if e.startswith("DELETE")})) or "none",
        "update": str("UPDATE" in events),
        "compactions": "+".join(sorted({e for e in events[:-1] if "COMPACT" in e} | {"final"})),
        "schema_changes": "+".join(sorted(set(builder.schema_changes))) or "none",
        "manifests_rewritten": str("REWRITE MANIFESTS" in events),
    }
    for kind in PRIMITIVE_TYPES:
        features[f"type_{kind}"] = str(any(c.name.endswith(f"_{kind}") for c in columns))
    # A filter names a column that exists under the same name at every step.
    stable = [c for c in columns if c.name in {c.name for c in created} and c.name not in builder.renamed]
    stable_kind = partition_kind if any(c.partition_source for c in stable) else "none"
    filters = _filters(streams["filters"], stable, stable_kind, builder.next_id)
    return GeneratedWorkflow(seed, definition, dimensions, features, filters)


def pairwise_seeds(candidates: Iterable[int], config: GeneratorConfig = GeneratorConfig()) -> list[int]:
    """The fewest seeds, chosen greedily from candidates, whose workflows together cover every
    pair of feature values that any candidate covers. Deterministic for the same candidates,
    config and GENERATOR_VERSION."""
    covers = {}
    for seed in candidates:
        features = sorted(generate_workflow(seed, "compact", "spark", config).features.items())
        covers[seed] = set(itertools.combinations(features, 2))
    uncovered = set().union(*covers.values())
    chosen = []
    while uncovered:
        best = max(covers, key=lambda seed: (len(covers[seed] & uncovered), -seed))
        chosen.append(best)
        uncovered -= covers.pop(best)
    return sorted(chosen)
