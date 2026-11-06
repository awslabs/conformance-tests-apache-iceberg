"""How the mixed_chaos pattern chooses the rows it deletes.

The pattern appends MIXED_CHAOS_TOTAL_ROWS_APPENDED rows and removes a third of
them to land on MIXED_CHAOS_TOTAL_ROWS_FINAL. Which third decides whether the
posdelete variant holds any position delete file at all.

Each append batch is written as its own data files, so a delete covering a
contiguous range of ids covers whole files, and their id bounds show it. Iceberg
satisfies such a delete by dropping the files -- correctly and more cheaply than
recording positions -- and the table ends up with no position delete in it
despite its name. Iceberg cannot evaluate `id % POSDELETE_MODULUS = 0` against a
file's bounds, so it cannot drop files for it: Spark reads the rows and writes
down the position of each one it deletes.

Whether that leaves a file partly deleted depends on the partition transform,
since a file holds one partition's rows. A month, day or hour partition fixes the
timestamp's month, which generate_partitioned_data sets to 1 + id % 12. 3 divides
12, so each such partition holds a single id % 3, and each of its files is either
deleted in full, by position deletes, or untouched. The same goes for any
combination including one of them. part_month_timestamp_mixed_chaos_posdelete
ends with 300 of its 900 data files fully deleted and none partly. Every other
transform puts every id % 3 in each partition, so each file is partly deleted.
"""

# Every row whose id is a multiple of this is deleted from a posdelete table.
POSDELETE_MODULUS = 3

# Rows appended before any delete runs.
MIXED_CHAOS_TOTAL_ROWS_APPENDED = 150000

# Rows remaining afterwards, whichever delete mode is used.
MIXED_CHAOS_TOTAL_ROWS_FINAL = 100000

MIXED_CHAOS_APPEND_BATCH_COUNT = 15
MIXED_CHAOS_DELETE_CHUNK_COUNT = 5


def mixed_chaos_append_batches() -> list[tuple[int, int]]:
    """The (row count, id offset) of each append batch, in order."""
    batch_rows = MIXED_CHAOS_TOTAL_ROWS_APPENDED // MIXED_CHAOS_APPEND_BATCH_COUNT
    return [
        (batch_rows, batch * batch_rows)
        for batch in range(MIXED_CHAOS_APPEND_BATCH_COUNT)
    ]


def mixed_chaos_delete_chunks() -> list[tuple[int, int]]:
    """The inclusive (start_id, end_id) range of each DELETE chunk, in order."""
    chunk_rows = MIXED_CHAOS_TOTAL_ROWS_APPENDED // MIXED_CHAOS_DELETE_CHUNK_COUNT
    return [
        (chunk * chunk_rows + 1, (chunk + 1) * chunk_rows)
        for chunk in range(MIXED_CHAOS_DELETE_CHUNK_COUNT)
    ]
