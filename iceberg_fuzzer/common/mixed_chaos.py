"""How the mixed_chaos pattern chooses the rows it deletes.

The pattern appends MIXED_CHAOS_TOTAL_ROWS_APPENDED rows and removes a third of
them to land on MIXED_CHAOS_TOTAL_ROWS_FINAL. Which third decides whether the
posdelete variant holds any position delete file at all.

Each append batch is written as its own data files, so a delete covering a
contiguous range of ids covers whole files, and Iceberg satisfies it by dropping
them -- correctly and more cheaply than recording positions. The table then ends
up with no position delete in it despite its name. Deleting every
POSDELETE_MODULUS-th row instead leaves every file partially deleted, so the
positions have to be written down.
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
