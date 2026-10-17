from datetime import UTC, datetime, timedelta, timezone

import pytest

from tributary.lake.hours import closed_boundary, floor_hour, parse_interval, trino_timestamp
from tributary.lake.pruning import scan_stats


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("10 minutes", timedelta(minutes=10)),
        ("1 minute", timedelta(minutes=1)),
        ("30 seconds", timedelta(seconds=30)),
        ("2 hours", timedelta(hours=2)),
        (" 1 Day ", timedelta(days=1)),
    ],
)
def test_parse_interval(text: str, expected: timedelta) -> None:
    assert parse_interval(text) == expected


@pytest.mark.parametrize("text", ["", "ten minutes", "10", "10 fortnights", "-1 minute"])
def test_parse_interval_rejects_garbage(text: str) -> None:
    with pytest.raises(ValueError, match="interval"):
        parse_interval(text)


def test_floor_hour_normalises_to_utc() -> None:
    pacific = timezone(timedelta(hours=-7))
    assert floor_hour(datetime(2026, 10, 17, 1, 59, 59, 999, tzinfo=pacific)) == datetime(
        2026, 10, 17, 8, tzinfo=UTC
    )


def test_an_hour_closes_once_newest_minus_delay_passes_its_end() -> None:
    delay = timedelta(minutes=10)
    # Newest event 00:21: the watermark is at 00:11, so 23:00-00:00 is closed, 00:00 is not.
    assert closed_boundary(datetime(2026, 10, 17, 0, 21, tzinfo=UTC), delay) == datetime(
        2026, 10, 17, 0, tzinfo=UTC
    )
    # Newest event 00:09: the watermark (23:59) is still inside the 23:00 hour.
    assert closed_boundary(datetime(2026, 10, 17, 0, 9, tzinfo=UTC), delay) == datetime(
        2026, 10, 16, 23, tzinfo=UTC
    )
    # Exactly on the edge: the watermark at 00:00 closes 23:00.
    assert closed_boundary(datetime(2026, 10, 17, 0, 10, tzinfo=UTC), delay) == datetime(
        2026, 10, 17, 0, tzinfo=UTC
    )


def test_trino_timestamp_literal_is_utc() -> None:
    ts = datetime(2026, 10, 17, 9, 0, tzinfo=timezone(timedelta(hours=2)))
    assert trino_timestamp(ts) == "TIMESTAMP '2026-10-17 07:00:00 UTC'"


PLAN = """\
 Fragment 3 [SOURCE]
     CPU: 25.00ms, Input: 5000 rows (200.20kB); per task: avg.: 5000.00, Output: 5000 rows
     └─ TableScan[table = iceberg:raw.recentchange$data@866467554808 constraint on [event_ts]]
            Layout: [event_id:varchar]
            Input: 1800 rows (72.00kB), Physical input: 98.10kB, Physical input time: 34.54us, \
Splits: 2, Splits generation wait time: 39.57ms
"""


def test_scan_stats_reads_the_constrained_lake_scan() -> None:
    assert scan_stats(PLAN, "iceberg.raw.recentchange") == (1800, 2)


def test_scan_stats_refuses_a_plan_without_the_constrained_scan() -> None:
    unconstrained = PLAN.replace(" constraint on [event_ts]", "")
    with pytest.raises(ValueError, match="no event_ts-constrained scan"):
        scan_stats(unconstrained, "iceberg.raw.recentchange")
