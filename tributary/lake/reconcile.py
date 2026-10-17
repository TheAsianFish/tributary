"""Per-hour reconciliation of the Iceberg lake against the PostgreSQL warehouse.

Both sinks read the same topic with the same parser and dedup, so for every
closed hour they must hold the same set of event ids. The check compares
distinct ids per hour (``raw.recentchange`` has a primary key, so its row count
is its distinct count) and, on the lake side, rows against distinct ids:
Iceberg has no key, so a duplicate that slipped past the dedup state shows up
as ``lake_rows > lake_distinct``. Each hour becomes one row in
``ops.quality_runs``.

An hour only counts as closed once *both* stores have moved past it, so a lake
that trails by one trigger is not flagged. A lake that has stopped would then
close no hours at all and pass silently; the separate ``lake_lag`` check fails
when the lake's newest event trails the warehouse's by more than
``TRIBUTARY_LAKE_MAX_LAG_MINUTES``.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import psycopg
import structlog

from tributary.lake.hours import closed_boundary, parse_interval
from tributary.lake.maintenance import lake_table, newest_event
from tributary.lake.trino_client import fetch
from tributary.warehouse.quality import CheckResult, record_checks

if TYPE_CHECKING:
    from tributary.config import Settings

TARGET = "lake.raw.recentchange"
SOURCE = "reconcile"

log = structlog.get_logger(__name__)


class ReconciliationError(RuntimeError):
    """At least one closed hour differs between the warehouse and the lake."""


@dataclass(frozen=True, slots=True)
class LakeCount:
    rows: int
    distinct: int


def lag_check(
    warehouse_newest: datetime, lake_newest: datetime | None, max_lag: timedelta, now: datetime
) -> CheckResult:
    """Fail when the lake's newest event trails the warehouse's by more than ``max_lag``."""
    lag = None if lake_newest is None else (warehouse_newest - lake_newest).total_seconds()
    ok = lag is not None and lag <= max_lag.total_seconds()
    return CheckResult(
        check_name="lake_lag",
        check_type="lake_lag",
        target=TARGET,
        status="pass" if ok else "fail",
        failures=0 if ok else 1,
        details={
            "warehouse_newest": warehouse_newest.isoformat(),
            "lake_newest": None if lake_newest is None else lake_newest.isoformat(),
            "lag_seconds": lag,
            "max_lag_seconds": max_lag.total_seconds(),
        },
        started_at=now,
        finished_at=now,
    )


def compare_hours(
    warehouse: Mapping[datetime, int], lake: Mapping[datetime, LakeCount], now: datetime
) -> list[CheckResult]:
    """One check per hour present in either store, in hour order."""
    checks = []
    for hour in sorted(set(warehouse) | set(lake)):
        expected = warehouse.get(hour, 0)
        got = lake.get(hour, LakeCount(0, 0))
        missing = max(expected - got.distinct, 0)
        extra = max(got.distinct - expected, 0)
        duplicates = got.rows - got.distinct
        failures = missing + extra + duplicates
        checks.append(
            CheckResult(
                check_name=f"reconcile_hour:{hour:%Y-%m-%dT%H}",
                check_type="reconcile",
                target=TARGET,
                status="pass" if failures == 0 else "fail",
                failures=failures,
                details={
                    "hour": hour.isoformat(),
                    "warehouse_distinct": expected,
                    "lake_rows": got.rows,
                    "lake_distinct": got.distinct,
                    "missing_in_lake": missing,
                    "extra_in_lake": extra,
                    "lake_duplicates": duplicates,
                },
                started_at=now,
                finished_at=now,
            )
        )
    return checks


def _utc(value: object) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"expected a timestamp, got {value!r}")
    return value.astimezone(UTC)


def warehouse_newest(conninfo: str) -> datetime | None:
    with psycopg.connect(conninfo) as conn:
        row = conn.execute("SELECT max(event_ts) FROM raw.recentchange").fetchone()
    return None if row is None or row[0] is None else _utc(row[0])


def warehouse_hours(conninfo: str, start: datetime, end: datetime) -> dict[datetime, int]:
    """Rows per UTC hour in ``[start, end)``; the primary key makes rows = distinct ids."""
    with psycopg.connect(conninfo) as conn:
        rows = conn.execute(
            "SELECT date_trunc('hour', event_ts, 'UTC'), count(*) FROM raw.recentchange"
            " WHERE event_ts >= %s AND event_ts < %s GROUP BY 1",
            (start, end),
        ).fetchall()
    return {_utc(hour): int(n) for hour, n in rows}


def lake_hours(settings: Settings, start: datetime, end: datetime) -> dict[datetime, LakeCount]:
    """Rows and distinct ids per UTC hour in ``[start, end)``; the filter prunes partitions."""
    rows = fetch(
        settings,
        f"SELECT date_trunc('hour', event_ts), count(*), count(DISTINCT event_id)"
        f" FROM {lake_table(settings)} WHERE event_ts >= ? AND event_ts < ? GROUP BY 1",
        [start, end],
    )
    return {_utc(hour): LakeCount(int(str(n)), int(str(d))) for hour, n, d in rows}


def reconcile_closed_hours(settings: Settings, run_id: str) -> list[CheckResult]:
    """Compare the closed hours of the lookback window, record them, raise on a mismatch."""
    now = datetime.now(UTC)
    w_newest = warehouse_newest(settings.pg_conninfo)
    if w_newest is None:
        log.info("reconcile.skipped", reason="warehouse empty")
        return []
    l_newest = newest_event(settings)
    checks = [lag_check(w_newest, l_newest, timedelta(minutes=settings.lake_max_lag_minutes), now)]
    if l_newest is not None:
        end = closed_boundary(min(w_newest, l_newest), parse_interval(settings.watermark_delay))
        start = end - timedelta(hours=settings.reconcile_lookback_hours)
        checks += compare_hours(
            warehouse_hours(settings.pg_conninfo, start, end),
            lake_hours(settings, start, end),
            now,
        )
    record_checks(settings.pg_conninfo, run_id, checks, source=SOURCE)
    failed = [c for c in checks if c.status != "pass"]
    log.info("reconcile.done", checks=len(checks), failed=len(failed), run_id=run_id)
    if failed:
        names = ", ".join(c.check_name for c in failed)
        raise ReconciliationError(f"{len(failed)} lake check(s) failed: {names}")
    return checks
