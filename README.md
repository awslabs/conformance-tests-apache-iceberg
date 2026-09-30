# Apache Iceberg Table Fuzzer

Generate a comprehensive variety of Apache Iceberg tables for testing compaction, compression, encoding, and cross-engine compatibility scenarios.

## Installation

See [INSTALL.md](INSTALL.md) for setup instructions, including the Java version PySpark requires.

## Usage

```bash
# Activate the virtual environment created by ./setup.sh
source .venv/bin/activate

# Generate simple tables with PySpark
python3 -m iceberg_fuzzer.cli generate --backend pyspark --warehouse-path /tmp/warehouse --specs specs/simple.yaml

# Generate all tables with PyIceberg (parallel) - excludes partitioned tables
python3 -m iceberg_fuzzer.cli generate --backend pyiceberg --warehouse-path /tmp/warehouse --specs table_specs.yaml --num-parallel 4

# Generate partitioned tables with PySpark (minimal set)
python3 -m iceberg_fuzzer.cli generate --backend pyspark --warehouse-path /tmp/warehouse --specs specs/partitioned.yaml --minimal --num-parallel 4

# Generate evolution tables with PySpark (comprehensive evolution testing)
python3 -m iceberg_fuzzer.cli generate --backend pyspark --warehouse-path /tmp/warehouse --specs specs/evolution.yaml --num-parallel 4

# Generate tables in S3 Tables bucket (deletes existing tables first)
python3 -m iceberg_fuzzer.cli generate --backend pyspark --warehouse-path "arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket" --specs table_specs.yaml --delete-existing --num-parallel 4

# Generate single table
python3 -m iceberg_fuzzer.cli generate --backend pyspark --warehouse-path /tmp/warehouse --specs specs/simple.yaml --table-name unpart_boolean_gzip_lvl1_plain_v1

# Generate minimal set with PySpark
python3 -m iceberg_fuzzer.cli generate --backend pyspark --warehouse-path /tmp/warehouse --specs specs/simple.yaml --minimal

# Validate what was generated
python3 -m iceberg_fuzzer.cli validate --warehouse-path /tmp/warehouse --specs specs/simple.yaml --minimal --num-parallel 8

# Delete all tables in warehouse
python3 -m iceberg_fuzzer.cli delete --backend pyspark --warehouse-path /tmp/warehouse --confirm

# Delete all tables with parallel workers (faster)
python3 -m iceberg_fuzzer.cli delete --backend pyspark --warehouse-path /tmp/warehouse --confirm --num-parallel 8

# Deactivate virtual environment when done
deactivate
```

Which table type gets generated is determined by the `--specs` filename, not by a
flag. `specs/simple.yaml` produces single-type tables, `specs/write_patterns.yaml`
write-pattern tables, `specs/partitioned.yaml` partitioned tables,
`specs/evolution.yaml` evolution tables, and `table_specs.yaml` produces every
type in sequence.

## Commands

| Command | Purpose |
|---|---|
| `generate` | Generate tables from a specs YAML file |
| `validate` | Validate existing tables with DuckDB |
| `delete` | Delete every table in a warehouse |
| `build-shapes` | Build tables from exhaustive per-column shape configs |
| `workflow run` | Run a multi-step correctness workflow against an existing table |

Run `python3 -m iceberg_fuzzer.cli <command> --help` for the full option list.

### validate

Validates row counts, column counts, and optionally full row content by reading
the tables back with DuckDB.

```bash
# Validate tables with parallel workers
python3 -m iceberg_fuzzer.cli validate --warehouse-path /path/to/warehouse --specs specs/simple.yaml --num-parallel 16

# Validate the minimal set (must match the flags used to generate)
python3 -m iceberg_fuzzer.cli validate --warehouse-path /path/to/warehouse --specs specs/simple.yaml --minimal

# Full content validation (slower, validates all rows)
python3 -m iceberg_fuzzer.cli validate --warehouse-path /path/to/warehouse --specs specs/simple.yaml --full-content-validation
```

The set of tables `validate` expects is derived the same way `generate` derives
the set it writes, so `--minimal` must be passed to both or neither.

### build-shapes

`build-shapes` is a separate generation path from `generate`. Instead of a
compression/encoding matrix over primitive types, it enumerates per-column
*shapes* — data type (including `list<>`, `map<>`, and `struct<>`), nullability,
encoding, compression, row count, and value strategy — and builds one small table
per configuration into the `exhaustive_shapes` namespace.

```bash
# Build the first 10 configs
python3 -m iceberg_fuzzer.cli build-shapes --warehouse-path /tmp/warehouse --limit 10

# Build one config by ID
python3 -m iceberg_fuzzer.cli build-shapes --warehouse-path /tmp/warehouse --config-id 84823 --target-size-mb 0.01

# Build a nested-type column directly
python3 -m iceberg_fuzzer.cli build-shapes --warehouse-path /tmp/warehouse --data-type 'map<string,int>' --limit 1
```

Each table is validated as it is built; pass `--no-validate` for faster bulk
generation.

## Workflows

Workflows run multi-step operations against existing tables and validate correctness.
A workflow is defined in YAML with steps that perform operations like `replace`, `append`, `delete`, and `rollback`.
See `specs/workflows/` for example workflow definitions and `iceberg_fuzzer/workflow/loader.py` for the authoritative list of supported operations and their required config fields.

```bash
# Generate a table first
python3 -m iceberg_fuzzer.cli generate --backend pyspark --warehouse-path /tmp/warehouse --specs specs/simple.yaml --minimal --random 1

# Run a workflow against it
TABLE=$(ls /tmp/warehouse/simple/ | head -1)
python3 -m iceberg_fuzzer.cli workflow run \
    --workflow specs/workflows/compaction.yaml \
    --warehouse-path /tmp/warehouse \
    --table-name "$TABLE" \
    --namespace simple

# Dry-run to see what would execute
python3 -m iceberg_fuzzer.cli workflow run \
    --workflow specs/workflows/compaction.yaml \
    --warehouse-path /tmp/warehouse \
    --table-name "$TABLE" \
    --namespace simple \
    --dry-run
```

### Workflow YAML format

Every step needs an `id`, an `operation`, and a `config` mapping. The
operation-specific fields go inside `config` — a step that puts them at the top
level is rejected by the loader.

```yaml
name: compact_append_delete_compact
steps:
  - id: compact_1
    operation: replace
    config:
      command: >
        python3 scripts/example-pyspark-compact.py
        --warehouse ${warehouse}
        --namespace ${namespace}
        --table-name ${table_name}
      timeout: 600

  - id: append
    operation: append
    config:
      backend: pyspark
      num_rows: 5

  - id: delete
    operation: delete
    config:
      backend: pyspark
      num_rows: 2

  - id: rollback_to_append
    operation: rollback
    config:
      backend: pyspark
      to_step: append
```

**Step operations:**
- `replace` — runs an external command that should not change logical content (e.g., compaction). A checkpoint (row count + SHA256 content hash) is captured before and validated after. Requires `command`; `timeout` is optional.
- `append` — adds rows to the table via the specified backend. Validates the row count increased by `num_rows`. Requires `backend` and `num_rows`.
- `delete` — deletes rows from the table. Validates the row count decreased by `num_rows`. Requires `backend` and `num_rows`.
- `rollback` — rolls the table back to the snapshot as of an earlier step, and validates the state matches that step's checkpoint. Requires `backend` and `to_step` (the `id` of a previous step).

**Placeholders** available in `command`: `${warehouse}`, `${namespace}`, `${table_name}`.

## Requirements

- Python 3.12+
- Java 21–23, not 24 or later, because PySpark relies on the Java Security
  Manager that Java 24 removed. See [INSTALL.md](INSTALL.md).

Python dependencies are declared in `pyproject.toml` and installed by `uv sync`.

## Backend Capabilities

| Feature | PySpark | PyIceberg | Polars |
|---|---|---|---|
| Simple tables | ✅ | ✅ | ✅ |
| Write-pattern tables | ✅ | ⚠️ INSERT-only fallback | ❌ |
| Partitioned tables | ✅ | ❌ | ❌ |
| Evolution tables | ✅ | ❌ | ❌ |
| S3 Tables warehouses | ✅ | ❌ | ❌ |
| UPDATE / DELETE | ✅ | ❌ | ❌ |

`delete` supports the `pyspark` and `pyiceberg` backends.

## Architecture

The project uses an object-oriented design with a unified CLI:

- **`cli.py`**: Unified command-line interface that routes to appropriate backend
- **`table_spec.py`**: Loads specs YAML and expands it into the set of tables to generate
- **`generator.py`**: Abstract base class defining the table generation interface
- **`backend/pyspark/generator.py`**: PySpark implementation of TableGenerator
- **`backend/pyiceberg/generator.py`**: PyIceberg implementation of TableGenerator
- **`backend/polars/generator.py`**: Polars implementation of TableGenerator
- **`backend/pyspark/builders.py`**: PySpark-specific table generation functions
- **`backend/pyiceberg/builders.py`**: PyIceberg-specific table generation functions
- **`validator.py`**: Abstract base class defining the table validation interface
- **`validator/local_duckdb/validator_duckdb.py`**: DuckDB-based table validation for local warehouses
- **`workflow/engine.py`**: Multi-step workflow orchestration (checkpoint → operate → validate)
- **`backend/__init__.py`**: Abstract base classes for ValidationEngine and MutationEngine
- **`backend/duckdb/__init__.py`**: DuckDB-based validation (row count + content hash via SHA256)
- **`backend/pyspark/mutation.py`**: PySpark-based table mutation (append rows)

## PySpark Implementation

### Known Limitations

**Types Spark cannot express**:

Spark SQL has no TIME, UUID or fixed-length binary type, so the PySpark backend
cannot declare those columns in the DDL it generates. It rejects `time`, `uuid`
and `fixed(L)` rather than substituting another type, so a run covering them
reports those tables as failures and generates the rest.

`--minimal` restricts a run to the types every backend can write, so it covers
none of the three. For coverage of them, use `build-shapes` for `fixed(L)`, and a
non-Spark backend (PyIceberg or Polars) for `time` and `uuid`.

### Table Organization

Tables are organized into separate namespaces:
- **Simple tables**: `{catalog}.simple` namespace → `warehouse_path/simple/`
- **Write-pattern tables**: `{catalog}.write_pattern` namespace → `warehouse_path/write_pattern/`
- **Partitioned tables**: `{catalog}.partitioned` namespace → `warehouse_path/partitioned/`
- **Evolution tables**: `{catalog}.evolution` namespace → `warehouse_path/evolution/`
- **Shape tables** (`build-shapes`): `exhaustive_shapes` namespace → `warehouse_path/exhaustive_shapes/`

### Table Generation

**Single-Type Tables**
- **Pattern**: `unpart_{datatype}_{compression}_{encoding}_{version}`
- **Examples**:
  - `unpart_boolean_gzip_lvl1_plain_v1`
  - `unpart_string_medium_zstd_lvl3_rle_v2`
- **Coverage**: one table per combination of primitive type × compression config × encoding × format version. The dimensions are `primitive_types`, `compression_configs`, `encoding_types`, and `iceberg_versions` in `specs/simple.yaml`.
- **Minimal set**: `uncompressed` + `plain` encoding + v1 & v2, excluding `time` and `uuid`
- **Files per table**: 3 data files (created via repartition for compaction testing)

For lists, maps, and structs, use `build-shapes` — the `specs/simple.yaml` matrix
covers primitive types only.

**Write-Pattern Tables**
- **Pattern**: `unpart_{pattern}_zstd_plain_dictionary_{version}_{delete_mode}`
- **Patterns**: insert_heavy, update_heavy, delete_heavy, mixed_chaos, batch_replace, gradual_growth, burst_ops, truncate_cycles
- **Versions**: v1, v2
- **Delete Modes**: cow (copy-on-write), posdelete (position deletes). v1 supports `cow` only, so there is no `v1_posdelete` variant.
- **Files per table**: Multiple data files (3-30 depending on pattern) for compaction testing

### Delete File Support

**Position Deletes** (posdelete mode):
- ✅ Fully supported in PySpark
- Creates position delete files using `DELETE FROM` SQL statements
- Example: `delete_heavy_*_posdelete` tables have position delete files

**Copy-on-Write** (cow mode):
- ✅ No delete files created
- Uses overwrite operations instead of deletes
- Rewrites data files without separate delete files

**Equality Deletes**: not generated.
- ⚠️ **Spark Limitation**: PySpark/Spark SQL cannot create equality delete files
- Spark's DELETE and MERGE operations always create **position deletes**, even when a table sets `write.delete.equality-field-columns`
- There is therefore no `eqdelete` mode; it would be indistinguishable from `posdelete`

**To create actual equality delete files**, use:
- Apache Flink with Iceberg connector
- Trino/Presto
- Iceberg Java API with RowDelta operations
- Other engines that support equality deletes

### Commit History

All write-pattern tables preserve commit history (no DROP TABLE operations):
- **insert_heavy**: 10 commits (10 appends)
- **gradual_growth**: 10 commits (10 appends)
- **update_heavy**: 6 commits (1 create + 5 overwrites)
- **burst_ops**: 4 commits (3 appends + 1 overwrite)
- **batch_replace**: 2 commits (1 create + 1 overwrite)
- **delete_heavy**: 2 commits (1 create + 1 delete/overwrite)
- **mixed_chaos**: 3-4 commits (create + append + delete + optional append)
- **truncate_cycles**: 7 commits (create + 3 cycles of delete+append)

### Usage

```bash
# Generate every table type (thousands of tables; expect this to take hours)
python3 -m iceberg_fuzzer.cli generate --backend pyspark --warehouse-path /path/to/warehouse --specs table_specs.yaml --num-parallel 16

# Generate only single-type tables
python3 -m iceberg_fuzzer.cli generate --backend pyspark --warehouse-path /path/to/warehouse --specs specs/simple.yaml --num-parallel 16

# Generate minimal set for testing (uncompressed + plain encoding + v1&v2, excluding time types)
python3 -m iceberg_fuzzer.cli generate --backend pyspark --warehouse-path /path/to/warehouse --specs specs/simple.yaml --minimal --num-parallel 16

# Generate only write-pattern tables
python3 -m iceberg_fuzzer.cli generate --backend pyspark --warehouse-path /path/to/warehouse --specs specs/write_patterns.yaml --num-parallel 16

# Generate partitioned tables (minimal set)
python3 -m iceberg_fuzzer.cli generate --backend pyspark --warehouse-path /path/to/warehouse --specs specs/partitioned.yaml --minimal --num-parallel 4
```

### Partitioned Tables

- **Pattern**: `part_{transform_spec}_{pattern}_{delete_mode}`
- **Coverage**: one table per combination of partition transform × write pattern × delete mode
- **Minimal set**: a few essential transforms × `mixed_chaos` × both delete modes
- **Examples** (the minimal set):
  - `part_identity_category_mixed_chaos_cow`
  - `part_bucket4_id_mixed_chaos_posdelete`
  - `part_month_timestamp_mixed_chaos_cow`
  - `part_identity_category_bucket4_id_mixed_chaos_posdelete`
- **Files per table**: Multiple data files distributed across partitions for compaction testing
- **Final rows**: 100,000 rows per table
- **Compaction-ready**: Multiple files per partition ensure meaningful compaction work

**Partition Transforms in the minimal set**:

- `identity(category)` - Direct partitioning (5 partitions: A-E)
- `bucket[4](id)` - Hash partitioning (4 buckets)
- `month(timestamp)` - Temporal partitioning (60 monthly partitions)
- `identity(category), bucket[4](id)` - Multi-column partitioning (20 partitions)

The full set adds further bucket widths, `truncate`, `year`/`day`/`hour`/`date`,
boolean and numeric identity partitioning, multi-column combinations, and a
single-partition edge case.

**Write Patterns**: insert_heavy, update_heavy, delete_heavy, mixed_chaos
- `mixed_chaos`: Creates 150K rows, deletes 50K in chunks, ends with 100K rows

**Delete Modes**:
- `cow`: Copy-on-write (no delete files, uses overwrite operations)
- `posdelete`: Position deletes (attempts to create position delete files via chunked DELETE operations)

**Key Features**:
- **Minimal set**: Covers the major partition transform categories in a handful of tables
- **Enhanced DELETE operations**: Uses chunked DELETE operations to maximize chances of position delete file creation
- **Merge-on-read properties**: Aggressive settings for posdelete tables to force position delete files
- **Fanout mode**: Uses `write.distribution-mode=none` to support non-identity partition transforms
- **Multiple files per partition**: Ensures compaction opportunities across different partition strategies
- **Deterministic data**: Same data generation logic ensures reproducible results

## Evolution Tables

Tables exercising Iceberg evolution features. Each one is defined explicitly in
the `evolution_tables` list in `specs/evolution.yaml`, which is the authoritative
list.

- **Pattern**: `evol_{evolution_type}_{scenario}_{delete_mode}`
- **Coverage**: Format upgrades (v1→v2), schema changes, partition evolution, sort orders, properties, metadata
- **Namespace**: `{catalog}.evolution` → `warehouse_path/evolution/`
- **Rows per table**: 1000 rows with evolution history preserved
- **Backend support**: PySpark only (evolution features require Spark-specific operations)

**Evolution Categories**:
- **Format Version**: v1→v2 upgrades
- **Schema Evolution**: Add/drop/rename columns, type widening
- **Partition Evolution**: Add/remove/change partition transforms
- **Sort Order Evolution**: Add/change/remove sort orders
- **Combined Evolution**: Multi-operation scenarios
- **Properties Evolution**: Compression, encoding, delete modes
- **Metadata Evolution**: Comments, custom properties
- **Edge Cases**: Rollbacks, concurrent changes

**Key Features**:
- **Schema-aware data generation**: Handles evolved schemas correctly after operations
- **Evolution validation**: Custom validation queries verify operations succeeded
- **Snapshot preservation**: Maintains complete evolution history

### Usage

```bash
# Generate evolution tables
python3 -m iceberg_fuzzer.cli generate --backend pyspark --warehouse-path /path/to/warehouse --specs specs/evolution.yaml --num-parallel 4

# Validate them with evolution-specific checks
python3 -m iceberg_fuzzer.cli validate --warehouse-path /path/to/warehouse --specs specs/evolution.yaml --num-parallel 4
```

## PyIceberg Implementation

### Table Organization

Tables are organized into separate namespaces:
- **Simple tables**: `simple` namespace → `warehouse_path/simple/`
- **Write-pattern tables**: `write_pattern` namespace → `warehouse_path/write_pattern/`

### Coverage

**Single-Type Tables**: the same matrix as PySpark, for primitive types.

**Write-Pattern Tables**: patterns that only need append and overwrite are
generated faithfully; the rest fall back to INSERT-only because PyIceberg has no
row-level UPDATE or DELETE.

Generated as specified:
- `insert_heavy` — pure append operations
- `batch_replace` — uses overwrite operations
- `gradual_growth` — incremental inserts
- `burst_ops` — burst + overwrite pattern

Degraded to INSERT-only:
- `update_heavy` — no UPDATE support
- `delete_heavy` — no DELETE support
- `mixed_chaos` — no mixed operations in one transaction
- `truncate_cycles` — limited truncate support, uses overwrite

### PyIceberg Limitations

**Missing Operations**:
- ❌ Direct UPDATE operations (`UPDATE table SET col = value WHERE condition`)
- ❌ Direct DELETE operations (`DELETE FROM table WHERE condition`)
- ❌ TRUNCATE operations (`DELETE FROM table`)
- ❌ Full equality delete support
- ❌ Mixed INSERT/UPDATE/DELETE in single transaction

**Available Workarounds**:
- ✅ `table.overwrite(data)` - Replace entire table contents
- ✅ `table.append(data)` - Add new rows
- ✅ Position deletes (limited, v2 only)
- ✅ Copy-on-write operations

### Table Schema Details

**Single-Type Tables**: 2 columns (id + value)
**Write-Pattern Tables**: 24 columns
- 1 ID column
- 20 primitive type columns
- 3 list type columns

**Final State**: All tables converge to exactly 1000 rows with deterministic, identical content for compaction testing.

### Usage

```bash
# Generate single-type tables only
python3 -m iceberg_fuzzer.cli generate --backend pyiceberg --warehouse-path /path/to/warehouse --specs specs/simple.yaml --num-parallel 4

# Generate write-pattern tables only (some degraded to INSERT-only)
python3 -m iceberg_fuzzer.cli generate --backend pyiceberg --warehouse-path /path/to/warehouse --specs specs/write_patterns.yaml

# Generate all supported table types
python3 -m iceberg_fuzzer.cli generate --backend pyiceberg --warehouse-path /path/to/warehouse --specs table_specs.yaml --num-parallel 4
```

### Future Improvements

To fully implement all 8 patterns, we would need:
1. PyIceberg UPDATE/DELETE operation support
2. Enhanced transaction capabilities
3. Better equality delete file creation
4. Row-level modification APIs

For now, the available patterns provide sufficient variety for compaction testing scenarios.
