# Apache Iceberg Table Fuzzer

Generate a comprehensive variety of Apache Iceberg tables for testing compaction, compression, encoding, and cross-engine compatibility scenarios.

## Installation

### Prerequisites

- Python 3.9+
- Java 21+ (for PySpark)

### Setup with Virtual Environment (Recommended)

```bash
# Clone the repository
git clone https://github.com/gsoundar/iceberg-table-generator.git
cd iceberg-table-generator

# Create and activate virtual environment
python3 -m venv iceberg-fuzzer-env
source iceberg-fuzzer-env/bin/activate  # On Windows: iceberg-fuzzer-env\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Set JAVA_HOME (required for PySpark)
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk  # Adjust path as needed
```

### Install Java (if not already installed)

**Amazon Linux/RHEL/CentOS:**
```bash
sudo yum install -y java-21-amazon-corretto java-21-amazon-corretto-devel
export JAVA_HOME=/usr/lib/jvm/java-21-amazon-corretto
```

**Ubuntu/Debian:**
```bash
sudo apt install -y openjdk-21-jdk
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
```

## Usage

Run directly from source (no installation required):

```bash
# Activate virtual environment
source iceberg-fuzzer-env/bin/activate

# Generate simple tables with PySpark
python3 -m iceberg_fuzzer.cli generate --backend pyspark --warehouse-path /tmp/warehouse --specs specs/simple.yaml

# Generate all tables with PyIceberg (parallel) - excludes partitioned tables
python3 -m iceberg_fuzzer.cli generate --backend pyiceberg --warehouse-path /tmp/warehouse --specs table_specs.yaml --num-parallel 4

# Generate partitioned tables with PySpark (minimal set)
python3 -m iceberg_fuzzer.cli generate --backend pyspark --warehouse-path /tmp/warehouse --specs specs/partitioned.yaml --num-parallel 4

# Generate evolution tables with PySpark (40 tables - comprehensive evolution testing)
python3 -m iceberg_fuzzer.cli generate --backend pyspark --warehouse-path /tmp/warehouse --specs specs/evolution.yaml --num-parallel 4

# Generate tables in S3 Tables bucket (deletes existing tables first)
python3 -m iceberg_fuzzer.cli generate --backend pyspark --warehouse-path "arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket" --specs table_specs.yaml --delete-existing --num-parallel 4

# Generate single table
python3 -m iceberg_fuzzer.cli generate --backend pyspark --warehouse-path /tmp/warehouse --specs table_specs.yaml --table-name unpart_boolean_gzip_lvl1_plain_v1

# Delete all tables in warehouse
python3 -m iceberg_fuzzer.cli delete --backend pyspark --warehouse-path /tmp/warehouse --confirm

# Delete all tables with parallel workers (faster)
python3 -m iceberg_fuzzer.cli delete --backend pyspark --warehouse-path /tmp/warehouse --confirm --num-parallel 8

# Generate minimal set with PySpark
python3 -m iceberg_fuzzer.cli generate --backend pyspark --warehouse-path /tmp/warehouse --specs table_specs.yaml --minimal

# Deactivate virtual environment when done
deactivate
```

## Requirements

- Python 3.9+
- PySpark 3.5.3 (automatically installed)
- PyIceberg 0.5.0+ (automatically installed)
- Polars 0.20.0+ (automatically installed)
- PyArrow 12.0.0+ (automatically installed)

## Unified CLI

**Options**:
- `--backend`: Choose `pyspark` or `pyiceberg`
- `--warehouse-path`: Local path or S3 Tables ARN (PySpark only)
- `--specs`: Path to table specs YAML file
- `--num-parallel`: Number of parallel workers (default: 1)
- `--table-type`: Not applicable - use specific specs files (`specs/simple.yaml`, `specs/write_patterns.yaml`, `specs/partitioned.yaml`)

**Backend Capabilities**:
- **PySpark**: All table types, S3 Tables support, partitioned tables, minimal mode
- **PyIceberg**: Simple and write-pattern tables only, local warehouse only
- **Polars**: Simple tables only (fastest for local), local warehouse only

## Architecture

The project uses an object-oriented design with a unified CLI:

- **`cli.py`**: Unified command-line interface that routes to appropriate backend
- **`generator.py`**: Abstract base class defining the table generation interface
- **`backend/pyspark/generator.py`**: PySpark implementation of TableGenerator
- **`backend/pyiceberg/generator.py`**: PyIceberg implementation of TableGenerator
- **`backend/polars/generator.py`**: Polars implementation of TableGenerator
- **`backend/pyspark/cli.py`**: PySpark-specific table generation functions
- **`backend/pyiceberg/cli.py`**: PyIceberg-specific table generation functions
- **`validator.py`**: Abstract base class defining the table validation interface
- **`validator/local_duckdb/validator_duckdb.py`**: DuckDB-based table validation for local warehouses

## PySpark Implementation

### Known Limitations

**Time Data Type Issues**:
- ❌ `time` data type has precision/timezone handling issues in PySpark
- ❌ Excluded from minimal mode: `time`, `list<time>`, `map<int,time>`, `struct<field:time>`
- ✅ `timestamp` and `timestamptz` work correctly
- **Reason**: Spark lacks native TIME type, uses TIMESTAMP internally causing value shifts

### Table Organization

Tables are organized into separate namespaces:
- **Simple tables**: `{catalog}.simple` namespace → `warehouse_path/simple/`
- **Write-pattern tables**: `{catalog}.write_pattern` namespace → `warehouse_path/write_pattern/`
- **Partitioned tables**: `{catalog}.partitioned` namespace → `warehouse_path/partitioned/`
- **Evolution tables**: `{catalog}.evolution` namespace → `warehouse_path/evolution/`

### Table Generation

**Single-Type Tables**: 2,560 tables
- **Pattern**: `unpart_{datatype}_{compression}_{encoding}_{version}`
- **Examples**: 
  - `unpart_boolean_gzip_lvl1_plain_v1`
  - `unpart_string_zstd_lvl3_rle_v2`
- **Coverage**: 80 data types × 8 compression configs (gzip 1-3, zstd 1-3, snappy, lz4) × 2 encodings (plain, rle) × 2 versions
- **Files per table**: 3 data files (created via repartition for compaction testing)

**Write-Pattern Tables**: 48 tables
- **Pattern**: `unpart_{pattern}_zstd_plain_dictionary_{version}_{delete_mode}`
- **Patterns**: insert_heavy, update_heavy, delete_heavy, mixed_chaos, batch_replace, gradual_growth, burst_ops, truncate_cycles
- **Versions**: v1, v2
- **Delete Modes**: cow (copy-on-write), posdelete (position deletes), eqdelete (equality deletes)
- **Files per table**: Multiple data files (3-30 depending on pattern) for compaction testing

### Delete File Support

**Position Deletes** (posdelete mode):
- ✅ Fully supported in PySpark
- Creates position delete files using `DELETE FROM` SQL statements
- Example: `delete_heavy_*_posdelete` tables have position delete files

**Equality Deletes** (eqdelete mode):
- ⚠️ **Spark Limitation**: PySpark/Spark SQL does not support creating equality delete files
- Tables are configured with `write.delete.equality-field-columns='id'` property
- However, Spark's DELETE and MERGE operations always create **position deletes**
- Both `posdelete` and `eqdelete` tables will have position delete files when using Spark

**To create actual equality delete files**, use:
- Apache Flink with Iceberg connector
- Trino/Presto
- Iceberg Java API with RowDelta operations
- Other engines that support equality deletes

**Copy-on-Write** (cow mode):
- ✅ No delete files created
- Uses overwrite operations instead of deletes
- Rewrites data files without separate delete files

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
# Generate all tables (2,560 single-type + 48 write-pattern + 8 partitioned = 2,616 tables)
python3 cli.py generate --backend pyspark --warehouse-path /path/to/warehouse --specs table_specs.yaml --num-parallel 16

# Generate only single-type tables (2,560 tables)
python3 cli.py generate --backend pyspark --warehouse-path /path/to/warehouse --specs specs/simple.yaml --num-parallel 16

# Generate minimal set for testing (152 tables: none compression+plain encoding+v1&v2, excluding time types)
python3 cli.py generate --backend pyspark --warehouse-path /path/to/warehouse --specs specs/simple.yaml --minimal --num-parallel 16

# Generate only write-pattern tables (48 tables)
python3 cli.py generate --backend pyspark --warehouse-path /path/to/warehouse --specs specs/write_patterns.yaml --num-parallel 16

# Generate partitioned tables (8 tables - minimal set)
python3 cli.py generate --backend pyspark --warehouse-path /path/to/warehouse --specs specs/partitioned.yaml --num-parallel 4
```

**Options**:
- `--table-type simple`: Generate only single-type tables (default)
- `--table-type write-patterns`: Generate only write-pattern tables  
- `--table-type all`: Generate all table types (single-type + write-pattern + partitioned for PySpark)
- `--table-type minimal`: Generate minimal set (80 tables with snappy+plain+v2)
- `--num-parallel N`: Number of parallel workers (default: 1)

### Partitioned Tables

**Partitioned Tables**: 8 tables (minimal set)
- **Pattern**: `part_{transform_spec}_mixed_chaos_{delete_mode}`
- **Examples**:
  - `part_identity_category_mixed_chaos_cow`
  - `part_bucket_4__id_mixed_chaos_posdelete`
  - `part_months_timestamp_mixed_chaos_cow`
  - `part_identity_category_bucket_4__id_mixed_chaos_posdelete`
- **Coverage**: 4 partition transforms × 1 pattern × 2 delete modes = 8 tables
- **Files per table**: Multiple data files distributed across partitions for compaction testing
- **Final rows**: 100,000 rows per table
- **Compaction-ready**: Multiple files per partition ensure meaningful compaction work

**Partition Transforms** (4 essential types):

Essential transforms:
- `identity(category)` - Direct partitioning (5 partitions: A-E)
- `bucket(4, id)` - Hash partitioning (4 buckets)
- `months(timestamp)` - Temporal partitioning (60 monthly partitions)
- `identity(category), bucket(4, id)` - Multi-column partitioning (20 partitions)

**Write Pattern**:
- `mixed_chaos`: Creates 150K rows, deletes 50K in chunks, ends with 100K rows

**Delete Modes**:
- `cow`: Copy-on-write (no delete files, uses overwrite operations)
- `posdelete`: Position deletes (attempts to create position delete files via chunked DELETE operations)

**Key Features**:
- **Minimal set**: Covers all major partition transform categories with only 8 tables
- **Enhanced DELETE operations**: Uses chunked DELETE operations to maximize chances of position delete file creation
- **Merge-on-read properties**: Aggressive settings for posdelete tables to force position delete files
- **Fanout mode**: Uses `write.distribution-mode=none` to support non-identity partition transforms
- **Multiple files per partition**: Ensures compaction opportunities across different partition strategies
- **Deterministic data**: Same data generation logic ensures reproducible results

### Usage

```bash
# Generate partitioned tables (8 tables - minimal set)
python3 cli.py generate --backend pyspark --warehouse-path /path/to/warehouse --specs specs/partitioned.yaml --num-parallel 4

# Generate evolution tables (40 tables - comprehensive evolution testing)
python3 cli.py generate --backend pyspark --warehouse-path /path/to/warehouse --specs specs/evolution.yaml --num-parallel 4
```

```bash
# Validate tables with parallel workers
python3 iceberg_fuzzer/validator/local_duckdb/validator_duckdb.py --warehouse-path /path/to/warehouse --specs table_specs.yaml --num-parallel 16

# Validate evolution tables with evolution-specific checks
python3 iceberg_fuzzer/validator/local_duckdb/validator_duckdb.py --warehouse-path /path/to/warehouse --specs specs/evolution.yaml --num-parallel 4

# Full content validation (slower, validates all rows)
python3 iceberg_fuzzer/validator/local_duckdb/validator_duckdb.py --warehouse-path /path/to/warehouse --specs table_specs.yaml --full-content-validation
```

## Evolution Tables

**Evolution Tables**: 40 tables testing comprehensive Iceberg evolution features
- **Pattern**: `evol_{evolution_type}_{scenario}_{delete_mode}`
- **Coverage**: Format upgrades (v1→v2), schema changes, partition evolution, sort orders, properties, metadata
- **Namespace**: `{catalog}.evolution` → `warehouse_path/evolution/`
- **Rows per table**: 1000 rows with evolution history preserved
- **Backend support**: PySpark only (evolution features require Spark-specific operations)

**Evolution Categories**:
- **Format Version**: v1→v2 upgrades (4 tables)
- **Schema Evolution**: Add/drop/rename columns, type widening (10 tables)  
- **Partition Evolution**: Add/remove/change partition transforms (6 tables)
- **Sort Order Evolution**: Add/change/remove sort orders (4 tables)
- **Combined Evolution**: Multi-operation scenarios (7 tables)
- **Properties Evolution**: Compression, encoding, delete modes (3 tables)
- **Metadata Evolution**: Comments, custom properties (2 tables)
- **Edge Cases**: Rollbacks, concurrent changes (4 tables)

**Key Features**:
- **Schema-aware data generation**: Handles evolved schemas correctly after operations
- **Evolution validation**: Custom validation queries verify operations succeeded
- **Snapshot preservation**: Maintains complete evolution history
- **100% validation success**: All tables pass comprehensive evolution-specific validation

### Usage

```bash
# Generate evolution tables (40 tables)
python3 cli.py generate --backend pyspark --warehouse-path /path/to/warehouse --specs specs/evolution.yaml --num-parallel 4
```

## PyIceberg Implementation

### Table Organization

Tables are organized into separate namespaces:
- **Simple tables**: `simple` namespace → `warehouse_path/simple/`
- **Write-pattern tables**: `write_pattern` namespace → `warehouse_path/write_pattern/`

### Successfully Created Tables

#### Single-Type Tables (12,800 tables)
- **Pattern**: `unpart_{datatype}_{compression}_{encoding}_{version}`
- **Examples**: 
  - `unpart_boolean_gzip_lvl1_plain_v1`
  - `unpart_string_zstd_lvl3_dictionary_v2`
- **Status**: ✅ All 12,800 tables created successfully
- **Coverage**: 80 data types × 10 compression configs × 8 encodings × 2 versions

#### Comprehensive Multi-Column Tables (Partial Success)

**✅ Successfully Created (16 tables)**:
- `unpart_insert_heavy_zstd_plain_dictionary_v1_cow` - Pure append operations
- `unpart_insert_heavy_zstd_plain_dictionary_v2_posdelete` 
- `unpart_batch_replace_zstd_plain_dictionary_v1_cow` - Uses overwrite operations
- `unpart_batch_replace_zstd_plain_dictionary_v2_posdelete`
- `unpart_gradual_growth_zstd_plain_dictionary_v1_cow` - Incremental inserts
- `unpart_gradual_growth_zstd_plain_dictionary_v2_posdelete`
- `unpart_burst_ops_zstd_plain_dictionary_v1_cow` - Burst + overwrite pattern
- `unpart_burst_ops_zstd_plain_dictionary_v2_posdelete`

**❌ Limited Implementation (16 tables)**:
- `unpart_update_heavy_zstd_plain_dictionary_v*` - No UPDATE support, fallback to INSERT-only
- `unpart_delete_heavy_zstd_plain_dictionary_v*` - No DELETE support, fallback to INSERT-only  
- `unpart_mixed_chaos_zstd_plain_dictionary_v*` - No mixed operations, fallback to INSERT-only
- `unpart_truncate_cycles_zstd_plain_dictionary_v*` - Limited truncate support, uses overwrite

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
**Comprehensive Tables**: 24 columns (simplified from planned 81)
- 1 ID column
- 20 primitive type columns  
- 3 list type columns (simplified)

**Final State**: All tables converge to exactly 1000 rows with deterministic, identical content for compaction testing.

### Usage

```bash
# Generate single-type tables only (12,800 tables)
python3 cli.py generate --backend pyiceberg --warehouse-path /path/to/warehouse --specs table_specs.yaml --table-type simple --num-parallel 1

# Generate write-pattern tables only (32 tables, some limited)
python3 cli.py generate --backend pyiceberg --warehouse-path /path/to/warehouse --specs table_specs.yaml --table-type write-patterns

# Generate all tables (12,832 tables total)
python3 cli.py generate --backend pyiceberg --warehouse-path /path/to/warehouse --specs table_specs.yaml --table-type all --num-parallel 1
```

**Options**:
- `--table-type simple`: Generate only single-type tables (default)
- `--table-type write-patterns`: Generate only comprehensive write-pattern tables  
- `--table-type all`: Generate both single-type and write-pattern tables

### Future Improvements

To fully implement all 8 patterns, we would need:
1. PyIceberg UPDATE/DELETE operation support
2. Enhanced transaction capabilities  
3. Better equality delete file creation
4. Row-level modification APIs

For now, the available patterns provide sufficient variety for compaction testing scenarios.
