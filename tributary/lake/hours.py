"""Closed hours: event-time hours whose row counts can no longer change.

The stream job drops any event older than its watermark (newest event time
minus the watermark delay), so once ``newest - delay`` has passed the end of an
hour, neither sink will ever receive another row for it. Those hours can be
compacted and reconciled; the current hour and the one the watermark is still
inside cannot.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta

_UNITS = {"second": 1, "minute": 60, "hour": 3600, "day": 86_400}
_INTERVAL = re.compile(r"^\s*(\d+)\s*(second|minute|hour|day)s?\s*$")


def parse_interval(text: str) -> timedelta:
    """Parse a Spark-style interval such as ``"10 minutes"`` (the watermark delay)."""
    match = _INTERVAL.match(text.lower())
    if match is None:
        raise ValueError(f"not an interval like '10 minutes': {text!r}")
    return timedelta(seconds=int(match.group(1)) * _UNITS[match.group(2)])


def floor_hour(ts: datetime) -> datetime:
    """The start of ``ts``'s hour, in UTC."""
    return ts.astimezone(UTC).replace(minute=0, second=0, microsecond=0)


def closed_boundary(newest: datetime, delay: timedelta) -> datetime:
    """Start of the first hour that is not closed; every hour before it is.

    Hour ``h`` is closed when ``h + 1 hour <= newest - delay``, which is exactly
    ``h < floor_hour(newest - delay)``.
    """
    return floor_hour(newest - delay)


def trino_timestamp(ts: datetime) -> str:
    """A Trino ``TIMESTAMP ... UTC`` literal (for statements that take no parameters)."""
    return f"TIMESTAMP '{ts.astimezone(UTC):%Y-%m-%d %H:%M:%S} UTC'"
