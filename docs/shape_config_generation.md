# Shape Config Generation: Exhaustive Table Structures

## The Problem

We want to generate **all structurally distinct Iceberg tables** with up to 6 data rows and up to 3 position delete entries. Each table has a specific arrangement of:
- Data files (which rows live in which file)
- Delete files (which rows are marked for deletion, grouped how)
- Snapshots (which files were committed together, in what order)
- Manifest structure (how files are grouped in manifests)

**The generator's bounds:** It enumerates tables of up to 6 data rows and up to 3 position delete entries, which keeps the enumeration finite and small enough to build. The bounds are a choice, not a proof that a compaction bug shows on a table this small.

**What is a sentinel file?** In the target compaction implementation, files above a certain byte-size threshold are skipped (not selected for compaction). The generator simulates this by designating one file as the "sentinel" -- at build time, this file gets padded with extra rows to exceed the threshold. This lets us test interactions between compacted files (small) and non-compacted files (large/sentinel). The sentinel row is always `row_count // 2` (e.g., row 3 for a 6-row table), and whichever file contains that row becomes the sentinel file.

---

## Pipeline Overview

```
INPUT: max_row_count=6, seed=42

     +------------------+
     |  L1: Row Count   |  Iterates row_count = 1, 2, ..., 6
     +------------------+
              |
              +--- row_count=1 ---> [Special single-row path]
              |                      (see "Special Handling" section)
              |
              +--- row_count=2..6 --> continues below
              |
              v
     +------------------+
     |  L2: Delete      |  7 abstract patterns
     |      Patterns    |  Input:  row_count
     +------------------+  Output: row_count + pattern (e.g., ['i','j'])
              |
              v
     +------------------+
     |  L3: File        |  Bell partitions for data + deletes
     |      Partitioning|  Input:  row_count + pattern
     +------------------+  Output: data_files[] + delete_files[] + sentinel info
              |
              | (reps first, then non-reps)
              v
     +------------------+
     |  L4: Value       |  3 strategies: row_numbers, nulls, repetition
     |      Assignment  |  Input:  skeleton (files with rows, no values)
     +------------------+  Output: skeleton with row['value'] populated
              |
              v
     +------------------+
     |  L5: Snapshot    |  Input:  valued config
     |      Ordering    |  Output: config + snapshots[]
     +------------------+  (reps: exhaustive; non-reps: 1 random)
              |
              v
     +------------------+
     |  L6: Manifest    |  x2: minimal + maximal
     |      Structure   |  Input:  config + snapshots
     +------------------+  Output: final config with manifest groupings
              |
              v
     WITH-SENTINEL CONFIGS (194,960 for seed=42)


     +------------------+
     |  No-Sentinel     |  Separate path: all files small
     |  Generation      |  1 representative per equivalence class
     +------------------+  x 3 orderings x 2 strategies x 2 manifests
              |
              v
     NO-SENTINEL CONFIGS (7,392 for seed=42)


     TOTAL OUTPUT: 202,352 configs (seed=42)
```

---

## Level 1: Row Count

**Input:** `max_row_count` (default 6)
**Output:** Routes to either the single-row special path or the main pipeline (L2+)

Iterates `row_count` from 1 to 6. Row_count=1 is handled separately (see below). Row_count=2..6 enters the main L2-L6 pipeline.

---

## Level 2: Delete Patterns

**Input:** `row_count` (e.g., 4)
**Output:** `(row_count, pattern, delete_count)` tuples

7 abstract patterns describe what kinds of deletes the table has:

| Pattern | Variables | Meaning | Example with 4 rows |
|---------|-----------|---------|---------------------|
| `[]` | 0 | No deletes | Table has only data files |
| `['i']` | 1 | 1 delete entry | Delete row 2 once |
| `['i','i']` | 1 | Same row deleted twice | Delete row 2 twice (in 1 or 2 files) |
| `['i','j']` | 2 | Two different rows deleted | Delete row 1 and row 3 |
| `['i','i','i']` | 1 | Same row deleted three times | Delete row 2 three times |
| `['i','i','j']` | 2 | One row twice, another once | Delete row 1 twice + row 3 once |
| `['i','j','k']` | 3 | Three different rows | Delete rows 0, 2, 3 |

Variables are placeholders -- `i`, `j`, `k` represent distinct row choices. The actual row assignment happens in L3 via "instantiation."

**Example:** For `row_count=4`, all 7 patterns are valid. For `row_count=2`, patterns with 3 distinct variables (`['i','j','k']`) still work because the generator can map variables to 2 available rows (but `k` would collide with `i` or `j`, making it equivalent to a 2-variable pattern -- this is caught by physical dedup later, if it survives).

---

## Level 3: File Partitioning

**Input:** `(row_count, pattern, delete_count)`
**Output:** Structural skeletons: `data_files[]` + `delete_files[]` + sentinel info + equivalence metadata

This is the most complex level. It determines the physical file layout.

### Step 3a: Partition data rows into files (Bell partitions)

Uses Bell number partitions: all ways to split `row_count` items into non-empty groups.

```
row_count=4, B(4)=15 partitions:

  [[0,1,2,3]]              -- 1 file, 4 rows
  [[0,1,2],[3]]            -- 2 files (3+1)
  [[0,1,3],[2]]            -- 2 files (3+1)
  [[0,2,3],[1]]            -- 2 files (3+1)
  [[1,2,3],[0]]            -- 2 files (3+1)
  [[0,1],[2,3]]            -- 2 files (2+2)
  [[0,2],[1,3]]            -- 2 files (2+2)
  [[0,3],[1,2]]            -- 2 files (2+2)
  [[0,1],[2],[3]]          -- 3 files (2+1+1)
  ...
  [[0],[1],[2],[3]]        -- 4 files (1+1+1+1)
```

### Step 3b: Identify equivalence classes and representatives

Two data partitions are **equivalent** if:
1. Same multiset of file sizes
2. Sentinel row (row `row_count//2`) is in a file of the same size

```
row_count=4, sentinel_row=2:

Class ((2,2), 2):  -- two size-2 files, sentinel in a size-2 file
  [[0,1],[2,3]]    <-- representative (lexicographically smallest)
  [[0,2],[1,3]]
  [[0,3],[1,2]]

Class ((1,3), 3):  -- sizes 1+3, sentinel in size-3 file
  [[0,1,2],[3]]    <-- representative
  [[0,2,3],[1]]

Class ((1,3), 1):  -- sizes 1+3, sentinel in size-1 file
  [[1,2,3],[0]]    <-- representative (only member -- sentinel_row=2 is in [1,2,3]
                       which has size 3... wait, [0] has sentinel? No: sentinel=2,
                       so [[0,1,3],[2]] has sentinel in size-1 file [2])
```

The **representative** (lexicographically smallest partition in each class) gets exhaustive L5 orderings. Non-representatives get 1 random ordering.

### Step 3c: Partition delete entries into files

Independently uses Bell partitions on the delete entries.

```
pattern=['i','i','j'], delete_count=3:
B(3)=5 ways to group 3 entries into files:

  [[0,1,2]]           -- 1 delete file with all 3 entries
  [[0,1],[2]]         -- 2 delete files (entries 0,1 together; entry 2 alone)
  [[0,2],[1]]         -- 2 delete files
  [[1,2],[0]]         -- 2 delete files
  [[0],[1],[2]]       -- 3 delete files (1 entry each)
```

### Step 3d: Sentinel instantiation (which rows get deleted?)

Each pattern variable must be assigned to an actual row. The generator creates variants based on whether the delete targets the sentinel file or not:

```
pattern=['i','j'], row_count=4, sentinel_row=2:

Instantiation 0: {i: None, j: None}       -- both target non-sentinel rows
Instantiation 1: {i: None, j: 'sentinel'} -- j targets row 2 (sentinel)
Instantiation 2: {i: 'sentinel', j: None} -- i targets row 2 (sentinel)
```

For `['i','j','k']`: only generates `all-non-sentinel`, `k=sentinel`, `i=sentinel` -- skips `j=sentinel` (coverage/explosion trade-off).

### Step 3e: Two-pass ordering

L3 collects all skeletons into two bags:
- **Reps bag:** skeletons where the data partition is the representative of its equivalence class
- **Non-reps bag:** all others

Reps bag is pushed through L4 first, ensuring the L5 EquivalenceTracker sees representatives before non-representatives.

### Example: Complete L3 output for one combination

```
Input: row_count=4, pattern=['i'], delete_count=1

One skeleton from L3:
{
  row_count: 4,
  delete_pattern: ['i'],
  delete_count: 1,
  data_files: [
    {file_id: 'data_f0', rows: [{row_id:0, value:None, pos:0}, {row_id:1, value:None, pos:1}]},
    {file_id: 'data_f1', rows: [{row_id:2, value:None, pos:0}, {row_id:3, value:None, pos:1}]}
  ],
  delete_files: [
    {file_id: 'del_f0', deletes: [{row_id:0, data_file:'data_f0', position:0}]}
  ],
  sentinel_row_id: 2,
  sentinel_in_file: 'data_f1',
  delete_instantiation_id: 0,
  is_data_partition_representative: true,
  data_partition_equiv_class: "((2, 2), 2)"
}
```

Note: `value: None` -- values are not yet assigned. That's L4's job.

---

## Level 4: Value Assignment

**Input:** Skeleton from L3 (files with rows, `row['value']` is None)
**Output:** Same skeleton with `row['value']` populated for every row

Three strategies are applied to each skeleton, producing up to 3 configs:

### L4a: `row_numbers` (always produced)

Each row's value = its row_id. Deterministic, no randomness.

```
Input skeleton (2 files, 4 rows):
  data_f0: [{row_id:0, value:None}, {row_id:1, value:None}]
  data_f1: [{row_id:2, value:None}, {row_id:3, value:None}]

Output:
  data_f0: [{row_id:0, value:0}, {row_id:1, value:1}]
  data_f1: [{row_id:2, value:2}, {row_id:3, value:3}]
  value_strategy: 'row_numbers'
```

### L4b: `nulls` (always produced)

Seeded RNG picks which rows are NULL. Remaining rows get their row_id as value.

```
Input: same skeleton, seed picks null_rows=[1,3]

Output:
  data_f0: [{row_id:0, value:0}, {row_id:1, value:None}]
  data_f1: [{row_id:2, value:2}, {row_id:3, value:None}]
  value_strategy: 'nulls'
  null_rows: [1, 3]
```

Number of NULLs: `random.randint(1, row_count//2)`, at least 1.

### L4c: `repetition` (produced only if feasible)

A deleted row's value is repeated in another row in the same file AND in a different file. Tests whether compaction correctly handles duplicate values across files.

**Feasibility (P1):** needs a deleted row whose file has >= 2 rows AND >= 2 data files exist.

```
Input: skeleton with row 0 deleted, file_0 has [row_0, row_1], file_1 has [row_2, row_3]

Output (P1: same-file + cross-file repetition):
  data_f0: [{row_id:0, value:0}, {row_id:1, value:0}]   <-- row_1 repeats row_0's value
  data_f1: [{row_id:2, value:0}, {row_id:3, value:5}]   <-- row_2 repeats row_0's value
  value_strategy: 'repetition'
```

**When skipped:** row_count=1 (can't repeat), row_count=2 with only 1 file (P1 fails), no deletes.

**Priority:** P1 (both same-file + cross-file from one deleted row) or skip. P2 (commented out in code, not active).

---

## Level 5: Snapshot Ordering

**Input:** Valued config (all rows have values, but no snapshot structure yet)
**Output:** Config with `snapshots[]` -- ordered list of snapshot objects

**Constraint:** Every delete file must appear in the same or a later snapshot than the data files it references, unless `deletes_before_data` lifts it, as the sample's witnesses do, see Sample.

### Representatives: exhaustive orderings

For configs where `is_data_partition_representative=True`, all valid snapshot orderings are generated.

```
Input: 2 data files + 1 delete file (del_f0 references data_f0)
       3 files total, Fubini(3) = 13 ordered partitions

Valid orderings (delete must be >= data_f0's snapshot):

  [{data_f0, data_f1, del_f0}]              -- all in 1 snapshot
  [{data_f0, data_f1}, {del_f0}]            -- data first, delete second
  [{data_f0, del_f0}, {data_f1}]            -- f0+del together, f1 later
  [{data_f1}, {data_f0, del_f0}]            -- f1 first, then f0+del
  [{data_f0}, {data_f1, del_f0}]            -- f0 first, then f1+del
  [{data_f0}, {data_f1}, {del_f0}]          -- each alone, del last
  [{data_f0}, {del_f0}, {data_f1}]          -- f0, del, f1
  [{data_f1}, {data_f0}, {del_f0}]          -- f1, f0, del
  [{data_f1}, {data_f0, del_f0}]            -- f1 first, then f0+del
  ...

Invalid (filtered out):
  [{del_f0}, {data_f0, data_f1}]            -- delete before its data = INVALID
  [{del_f0, data_f1}, {data_f0}]            -- delete before data_f0 = INVALID
```

**Collapsing (>= 6 files):** Consecutive snapshots of the same "label type" (unreferenced-non-sentinel data, unreferenced-with-sentinel data, delete-only) collapse into one -- avoids combinatorial explosion from semantically equivalent orderings.

### Non-representatives: 1 random ordering

Picks a random snapshot count (1 to min(num_files, 4)), randomly assigns files to snapshots, checks constraints. Falls back to single-snapshot if repeated attempts fail.

---

## Level 6: Manifest Structure

**Input:** Config with snapshots
**Output:** 2 final configs (one minimal, one maximal)

For each snapshot, determines how files are grouped into manifests:

```
Snapshot: {data_files: [data_f0, data_f1], delete_files: [del_f0]}

Minimal manifests:
  data_manifests:   [[data_f0, data_f1]]   -- one manifest for all data
  delete_manifests: [[del_f0]]             -- one manifest for all deletes

Maximal manifests:
  data_manifests:   [[data_f0], [data_f1]] -- each file in its own manifest
  delete_manifests: [[del_f0]]             -- (only 1 delete, same either way)
```

Every config from L5 produces exactly 2 final configs (one per manifest strategy).

---

## Special Handling: row_count=1

With a single row, the main pipeline simplifies dramatically:

- Only 1 data file (can't partition 1 row)
- Only single-variable patterns: `[]`, `['i']`, `['i','i']`, `['i','i','i']`
  (multi-variable patterns collapse to same row -- pointless)
- No instantiation variants (sentinel = the only row = the only file)
- L4c skipped (can't repeat with 1 row)

**What still varies:**
- Delete file partitioning via Bell numbers (e.g., 3 entries -> 1, 2, or 3 files)
- Dedup by size-multiset (since all entries are identical, partitions with same file sizes are equivalent)
- Snapshot orderings: all-in-one, data-then-deletes, max-spread
- Manifest: minimal/maximal
- Value strategy: `row_numbers` or `nulls`

```
Example: pattern=['i','i'], 2 identical delete entries

Bell partitions of 2 entries:
  [[0,1]]     -> 1 delete file with 2 entries (size multiset: (2,))
  [[0],[1]]   -> 2 delete files with 1 entry each (size multiset: (1,1))

Both kept (different size multisets).

Snapshot orderings for [[0],[1]] (1 data + 2 delete files):
  1. [{data_f0, del_f0, del_f1}]                     -- all together
  2. [{data_f0}, {del_f0, del_f1}]                   -- data first, deletes second
  3. [{data_f0}, {del_f0}, {del_f1}]                 -- max spread

x 2 value strategies x 2 manifest strategies = 12 configs from this one partition.
```

**Total row_count=1:** 64 configs.

---

## No-Sentinel Configs (separate generation path)

After the main with-sentinel pipeline completes, a separate path generates configs where **no file is padded** (all files are small, compaction takes everything).

**Input:** All with-sentinel configs (re-generated internally via a temporary generator)
**Output:** No-sentinel configs with `sentinel_in_file: null`

### Equivalence class for no-sentinel

`(num_data_files, num_delete_files, delete_coverage_pattern)` -- ignoring file sizes since all files are small and all get compacted.

One representative config is kept per equivalence class.

### Per-class generation

Each representative gets:
- 3 snapshot orderings:
  1. All files in one snapshot
  2. Max spread (each file its own snapshot, deletes after data)
  3. Data in one snapshot, deletes in the next (only when deletes exist)
- 2 value strategies: `row_numbers` and `nulls`
- 2 manifest strategies: minimal and maximal

```
Example class: (2 data files, 1 delete file, delete covers data_f0 partially)

Ordering 1: [{data_f0, data_f1, del_f0}]
Ordering 2: [{data_f0}, {data_f1}, {del_f0}]
Ordering 3: [{data_f0, data_f1}, {del_f0}]

x 2 value strategies x 2 manifest strategies = up to 12 configs per class
(minus physical dedup)
```

**Note on `nulls` strategy in this path:** As in the main pipeline's `_assign_values_nulls`, a seeded choice of between one and `row_count // 2` rows is set to NULL and recorded in `null_rows`, and the other rows keep their row numbers; the no-sentinel path assigns them inline.

**Physical dedup:** Applied via `_physical_delete_signature` to remove configs that would produce identical physical tables (e.g., two classes that differ only in which file was the sentinel -- without sentinel, they become identical).

**Total no-sentinel:** 7,392 configs (seed=42).

---

## What Gets Deduplicated (and Why)

| Dedup | Where applied | What it catches | Example |
|-------|---------------|-----------------|---------|
| Size-multiset (row_count=1) | Single-row path | Bell partitions of identical entries producing same file layout | `[[0,1],[2]]` vs `[[0,2],[1]]` when all entries are `(data_f0, row_0, pos_0)` |
| L3 equivalence classes | L3 (data partitions) | Partitions with same file-size multiset + same sentinel-file-size | `[[0,1],[2,3]]` and `[[0,3],[1,2]]` are same class -- non-rep gets 1 ordering instead of exhaustive |
| L5 equivalence collapsing | L5 (>= 6 files) | Snapshot orderings that differ only in ordering of "uninteresting" consecutive snapshots | `[unref_data][unref_data][delete]` collapsed to `[unref_data][delete]` |
| Physical signature | No-sentinel path | Different equiv-class representatives collapsing when sentinel is removed | Two configs differing only in sentinel file -- without sentinel they're identical |

**Physical signature includes:** data file rows `(row_id, value)` per file, snapshot structure, sorted delete entries within each file, manifest strategy, value strategy, and `sentinel_in_file`.

**Note:** The with-sentinel path does NOT apply physical signature dedup. Duplicates there (from instantiation collapsing, e.g., `['i','j']` inst_0 at row_count=2 producing same table as `['i','i']` inst_0) are a known issue tracked in the plan as Phase 1.3.4.

---

## What It Does NOT Do

- **No type awareness.** Configs contain abstract integer indices (or None). Type mapping (int -> string -> timestamp) happens at build time via `TypeAwareSchema`.
- **No compression/encoding.** Build-time parameters, not structural properties.
- **No partitioned tables.** Unpartitioned only. Partitioned extension is Phase 3.
- **No equality deletes.** Position deletes only. Equality deletes are Phase 3.
- **No P2 fallback for L4c.** Code has P2 commented out. When P1 fails, repetition is skipped entirely. P3 fallback ("just pick any 2 rows to share a value") also not implemented.
- **Not all orderings for non-representatives.** Only 1 random ordering (seed-dependent). Representatives get exhaustive.
- **No `j=sentinel` for 3-variable patterns.** `['i','j','k']` only generates `all-non-sentinel`, `k=sentinel`, `i=sentinel`.

---

## Sample

`shape_sampling.py` picks which configs to build; nothing builds them yet, since `build-shapes` still
builds every config `generate_all` yields. It keeps one class per dependency structure, the
`EquivalenceTracker` key plus whether the config has a sentinel. Within a class, configs are added
until every relation some config of the class has is set: a delete committed with a data file it
names or after it, a delete on the sentinel after a delete on a file a compaction rewrites, the
sentinel committed with such a file, and a row deleted twice by one delete file; and, on a data file
the builder writes with the config's rows alone, never the sentinel, which it pads, and from deletes
that [apply by sequence number](https://iceberg.apache.org/spec/#scan-planning), entries reaching the
file's row count while a row of it stays live, every row deleted though no one delete file deletes
them all, and a delete on its last row. At seed 42 that is 4,243 configs in 3,005 classes.

The sample stops taking a class's configs by relations alone, and value strategies enter only
through its tie break, by the pairs of ordering features a config adds, so a class need not hold each
of its value strategies. At seed 42 its configs hold 3,775 with row numbers, 278 with nulls and 190
with repetition, and every class holds more than one value strategy, of which the sample keeps each
in 15 of the 3,005. It takes no maximal-manifest config, since each ties its minimal twin and has the
higher config id.

The generator commits every delete file with or after the data files it names, so the sample adds
witnesses from a second pass under `deletes_before_data`: for each class with a sentinel whose second
pass holds an eligible config, one that commits a delete file before a data file it names and every
delete file no earlier than the first data commit, the eligible config holding the most of the three
early relations, `delete_before_target`, `file_mixed_sequence` and `delete_spans_sequence`. A delete
file committed before every data file would not last, since at a later commit Iceberg Java drops
delete files older than every live data file,
[`MergingSnapshotProducer.apply`](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.0/core/src/main/java/org/apache/iceberg/MergingSnapshotProducer.java#L928-L937).
Only the main pipeline's orderings read `deletes_before_data`, so classes without a sentinel get no
witness. At seed 42 there are 2,196 witnesses, numbered past the generator's last config id, and of
the 2,386 classes with a sentinel, 190 get none: 90 with no delete file, 22 with one data file, and
78 with two data files or more and a delete file whose second pass holds no eligible config.

---

## Output Format

Each config is a complete structural description of one Iceberg table, here config 8315 of the seed 42 run:

```json
{
  "config_id": 8315,
  "row_count": 4,
  "delete_pattern": ["i", "j"],
  "delete_count": 2,
  "value_strategy": "row_numbers",
  "manifest_strategy": "minimal",
  "sentinel_in_file": "data_f1",
  "sentinel_row_id": 2,
  "is_data_partition_representative": true,
  "data_partition_equiv_class": "((2, 2), 2)",
  "is_representative": true,
  "data_files": [
    {
      "file_id": "data_f0",
      "file_index": 0,
      "rows": [
        {"row_id": 0, "value": 0, "position": 0},
        {"row_id": 1, "value": 1, "position": 1}
      ]
    },
    {
      "file_id": "data_f1",
      "file_index": 1,
      "rows": [
        {"row_id": 2, "value": 2, "position": 0},
        {"row_id": 3, "value": 3, "position": 1}
      ]
    }
  ],
  "delete_files": [
    {
      "file_id": "del_f0",
      "file_index": 0,
      "deletes": [
        {"row_id": 0, "data_file": "data_f0", "data_file_index": 0, "position": 0}
      ]
    },
    {
      "file_id": "del_f1",
      "file_index": 1,
      "deletes": [
        {"row_id": 2, "data_file": "data_f1", "data_file_index": 1, "position": 0}
      ]
    }
  ],
  "snapshots": [
    {
      "snapshot_id": 0,
      "data_files": ["data_f0", "data_f1"],
      "delete_files": [],
      "data_manifests": [["data_f0", "data_f1"]],
      "delete_manifests": []
    },
    {
      "snapshot_id": 1,
      "data_files": [],
      "delete_files": ["del_f0", "del_f1"],
      "data_manifests": [],
      "delete_manifests": [["del_f0", "del_f1"]]
    }
  ]
}
```

**Field definitions:**
- `position`: The row's 0-indexed ordinal position within its data file. Position delete entries reference `(file_path, position)`.
- `sentinel_in_file`: Which data file gets padded at build time. `null` for no-sentinel configs.
- `sentinel_row_id`: The row that determines the sentinel file (`row_count // 2`).
- `is_data_partition_representative`: Whether this partition is the representative of its L3 equivalence class.
- `is_representative`: Whether this config is the representative of its L5 equivalence class (gets exhaustive orderings).
- `data_partition_equiv_class`: String key of the L3 class (file sizes + sentinel file size).
- `value_strategy`: Which L4 strategy was used (`row_numbers`, `nulls`, `repetition`).
- `manifest_strategy`: How files are grouped in manifests (`minimal`, `maximal`).
- `is_no_sentinel`: `true` for no-sentinel configs (absent or `false` for with-sentinel).
- `null_rows`: (only for the `nulls` strategy, in either path) List of row_ids set to NULL.

---

## Current Numbers (seed=42, default flags)

Default flags: `exhaustive_mode=False`, `deletes_before_data=False`. Every config count in this document comes from one run of `TableConfigGenerator(max_row_count=6, seed=42).generate_all()`, the sample's from `select_configs` over it and the witnesses' from the second pass `witnesses` makes at the same seed.

| row_count | with-sentinel | no-sentinel | total |
|-----------|--------------|-------------|-------|
| 1 | 64 | 0 | 64 |
| 2 | 1,064 | 268 | 1,332 |
| 3 | 4,750 | 728 | 5,478 |
| 4 | 16,392 | 1,508 | 17,900 |
| 5 | 43,666 | 1,808 | 45,474 |
| 6 | 129,024 | 3,080 | 132,104 |
| **total** | **194,960** | **7,392** | **202,352** |

**Seed dependence:** Non-representative counts are seed-dependent (different seeds produce slightly different totals because sampling + physical dedup interact). The row_count=1 counts and representative counts are deterministic regardless of seed.

---

## End-to-End Trace: One Config Through the Pipeline

Starting from scratch, here's how config 8315 of the seed 42 run gets generated:

```
L1: row_count = 4

L2: pattern = ['i', 'j'], delete_count = 2

L3: data_partition = [[0,1],[2,3]]
    --> equiv class ((2,2), 2), this IS the representative
    delete_partition = [[0],[1]]  (2 delete files, 1 entry each)
    instantiation 2, one of the two that put an entry on the sentinel row
    --> the entries map to row 0 (in data_f0) and row 2 (in data_f1)
    sentinel_in_file = data_f1 (contains row 2 = row_count//2)

    Skeleton:
      data_f0: [row_0 pos_0, row_1 pos_1]
      data_f1: [row_2 pos_0, row_3 pos_1]  <-- sentinel file
      del_f0: [(row_0, data_f0, pos_0)]
      del_f1: [(row_2, data_f1, pos_0)]

L4: value_strategy = 'row_numbers'
    row_0.value=0, row_1.value=1, row_2.value=2, row_3.value=3

L5: is_representative=True --> exhaustive orderings
    This config has 4 files total (2 data + 2 delete)
    Constraint: del_f0 >= data_f0, del_f1 >= data_f1
    One of its orderings: [{data_f0, data_f1}, {del_f0, del_f1}]

L6: manifest_strategy = 'minimal'
    snapshot_0: data_manifests=[[data_f0, data_f1]], delete_manifests=[]
    snapshot_1: data_manifests=[], delete_manifests=[[del_f0, del_f1]]

    --> config_id 8315 assigned, yielded as final output
```
