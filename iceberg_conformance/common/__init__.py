"""Shared utilities used across all backends."""


def row_id_from_index(index: int, offset: int) -> int:
    """Compute a row ID from a 0-based index and an offset.

    This is the single source of truth for how row IDs are derived.
    All backends use this to produce deterministic, sequential IDs.

    Args:
        index: 0-based row index within the batch.
        offset: Starting offset (e.g., current max ID in the table).

    Returns:
        A 1-based row ID.
    """
    return index + 1 + offset
