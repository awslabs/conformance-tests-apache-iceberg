"""
Consolidated logic for generating and manipulating Iceberg timestamp values.
"""

from datetime import datetime, timedelta, timezone

# Cycled through by row id.
# Seven entries, coprime with 24 hours so they advance independently.
TIMESTAMPTZ_OFFSETS = (
    timedelta(hours=9),                   # +09:00
    timedelta(hours=-8),                  # -08:00, a sign error shows here
    timedelta(hours=5, minutes=30),       # +05:30, whole hours
    timedelta(hours=-9, minutes=-30),     # -09:30, negative and half an hour
    timedelta(hours=12, minutes=45),      # +12:45, Chatham Islands standard time
    timedelta(hours=14),                  # +14:00, the largest the tz database has
    timedelta(0),                         # +00:00
)


def timestamptz_zone(row_id: int) -> timezone:
    """The zone the `timestamptz` value for `row_id` is expressed in."""
    return timezone(TIMESTAMPTZ_OFFSETS[row_id % len(TIMESTAMPTZ_OFFSETS)])


# TODO: Vary by row ID. The hour cycles every 24 and the offset every 7, so values
# repeat every 168 rows -- a row cannot be identified by its value. Varying the date
# would also cover daylight saving transitions.
TIMESTAMP_DATE = datetime(2023, 1, 1)


def timestamp_value(row_id: int) -> datetime:
    """Zone-free `timestamp` value for `row_id`."""
    return TIMESTAMP_DATE.replace(hour=row_id % 24)


def timestamptz_value(row_id: int) -> datetime:
    """
    The `timestamptz` value for `row_id`.

    Timezone is varied deterministically based on row ID.
    """
    return timestamp_value(row_id).replace(tzinfo=timestamptz_zone(row_id))
