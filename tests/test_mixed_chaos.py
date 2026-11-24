"""Invariants between the mixed_chaos row ranges and the counts they derive from.

The batches have to divide the appended rows evenly, the chunks have to cover
exactly the ids the batches append, and removing every POSDELETE_MODULUS-th of
those ids has to land on the final row count.
"""

from iceberg_fuzzer.common import row_id_from_index
from iceberg_fuzzer.common.mixed_chaos import (
    MIXED_CHAOS_APPEND_BATCH_COUNT,
    MIXED_CHAOS_DELETE_CHUNK_COUNT,
    MIXED_CHAOS_TOTAL_ROWS_APPENDED,
    MIXED_CHAOS_TOTAL_ROWS_FINAL,
    POSDELETE_MODULUS,
    mixed_chaos_append_batches,
    mixed_chaos_delete_chunks,
)


def appended_ids():
    """The ids the append batches write, derived the way the generator derives them.

    The delete chunks are checked against these rather than against 1 to
    MIXED_CHAOS_TOTAL_ROWS_APPENDED, so a batch written at the wrong offset shows
    up here as it would in the table.
    """
    return [
        row_id_from_index(index, row_offset)
        for row_count, row_offset in mixed_chaos_append_batches()
        for index in range(row_count)
    ]


class TestAppendBatches:
    def test_there_are_as_many_as_declared(self):
        assert len(mixed_chaos_append_batches()) == MIXED_CHAOS_APPEND_BATCH_COUNT

    def test_they_are_all_the_same_size(self):
        # An uneven split leaves the last batch short and the ids it covers wrong.
        sizes = {row_count for row_count, _ in mixed_chaos_append_batches()}
        assert len(sizes) == 1

    def test_the_batches_divide_the_appended_rows_evenly(self):
        assert MIXED_CHAOS_TOTAL_ROWS_APPENDED % MIXED_CHAOS_APPEND_BATCH_COUNT == 0

    def test_they_append_each_id_once(self):
        # Overlapping batches would write some ids twice.
        ids = appended_ids()
        assert len(ids) == MIXED_CHAOS_TOTAL_ROWS_APPENDED
        assert len(set(ids)) == len(ids)


class TestDeleteChunks:
    def test_they_cover_every_appended_id_once(self):
        # A remainder leaves the tail of the id range undeleted and the table over
        # its row count; an overlap deletes nothing extra but hides a gap.
        covered = []
        for start_id, end_id in mixed_chaos_delete_chunks():
            covered.extend(range(start_id, end_id + 1))
        assert sorted(covered) == sorted(appended_ids())

    def test_the_bounds_are_inclusive(self):
        chunks = mixed_chaos_delete_chunks()
        ids = appended_ids()
        assert chunks[0][0] == min(ids)
        assert chunks[-1][1] == max(ids)

    def test_they_do_not_overlap(self):
        chunks = mixed_chaos_delete_chunks()
        for (_, earlier_end), (later_start, _) in zip(chunks, chunks[1:]):
            assert later_start == earlier_end + 1

    def test_there_are_as_many_as_declared(self):
        assert len(mixed_chaos_delete_chunks()) == MIXED_CHAOS_DELETE_CHUNK_COUNT

    def test_the_chunks_divide_the_appended_rows_evenly(self):
        assert MIXED_CHAOS_TOTAL_ROWS_APPENDED % MIXED_CHAOS_DELETE_CHUNK_COUNT == 0


class TestTheRowCounts:
    def test_deleting_every_nth_row_lands_on_the_final_count(self):
        removed = MIXED_CHAOS_TOTAL_ROWS_APPENDED // POSDELETE_MODULUS
        assert MIXED_CHAOS_TOTAL_ROWS_APPENDED - removed == MIXED_CHAOS_TOTAL_ROWS_FINAL

    def test_the_modulus_divides_the_appended_rows_evenly(self):
        # Otherwise the fraction removed is not exactly 1/POSDELETE_MODULUS and
        # the row count above holds only by rounding.
        assert MIXED_CHAOS_TOTAL_ROWS_APPENDED % POSDELETE_MODULUS == 0

    def test_deleting_within_the_chunks_lands_on_the_final_count(self):
        # The count has to hold over the chunks, not just over the range as a
        # whole: a gap between chunks leaves its rows undeleted. It is counted
        # over the appended ids, so a batch at the wrong offset shows as the
        # wrong row count, as it would in the table.
        chunks = mixed_chaos_delete_chunks()

        def deleted(row_id):
            return row_id % POSDELETE_MODULUS == 0 and any(
                start_id <= row_id <= end_id for start_id, end_id in chunks
            )

        survivors = sum(1 for row_id in appended_ids() if not deleted(row_id))
        assert survivors == MIXED_CHAOS_TOTAL_ROWS_FINAL

    def test_the_final_count_is_below_the_appended_count(self):
        assert MIXED_CHAOS_TOTAL_ROWS_FINAL < MIXED_CHAOS_TOTAL_ROWS_APPENDED
