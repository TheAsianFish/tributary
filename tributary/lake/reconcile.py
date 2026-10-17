"""Per-hour reconciliation of the Iceberg lake against the PostgreSQL warehouse.

Both sinks read the same topic with the same parser and dedup, so for every
closed hour they should hold the same set of event ids. Per hour the check
compares, on both sides, the number of distinct ids and an id fingerprint:
the sum over the hour's distinct ids of a signed 32-bit hash (the first four
bytes of the id's MD5). A sum does not depend on order, so PostgreSQL and
Trino compute it with plain aggregates, and an exchange (one id missing from
the lake, another extra, counts equal) changes it unless the two hashes
happen to coincide (about one chance in four billion per exchange). The ids
are lowercase UUID strings on both sides (``events.event_id`` rejects any
other form, PostgreSQL's ``uuid::text`` prints the same form), so both hash
the same bytes. ``raw.recentchange`` has a primary key, so its rows are its
distinct ids; on the lake side the check also compares rows against distinct
ids: Iceberg has no key, so a duplicate that slipped past the dedup state
shows up as ``lake_rows > lake_distinct``. Each hour becomes one row in
``ops.quality_runs``.

"Should", not "must": the two queries trigger at different rates, so at any
Kafka offset their watermarks differ. An event later than the watermark
delay can be kept by one and dropped by the other, and after a long outage a
copy whose dedup key had expired can pass as new in one of them (PostgreSQL's
key absorbs it, Iceberg stores it). With the measured lateness (max 143 s
against a 10-minute delay) neither happens on the sample; when an hour fails,
its ``details`` (missing, extra, duplicates, ``ids_match``) say which kind it
was.

An hour only counts as closed once *both* stores have moved past it: the
lake's newest event (Iceberg commits a batch atomically) and the newest event
of the warehouse's fully committed batches (``ops.stream_progress``), so a
lake that trails by one trigger, or a warehouse batch half committed, is not
flagged. A lake that has stopped would then close no hours at all and pass
silently; the separate ``lake_lag`` check fails when the lake's newest event
trails the warehouse's by more than ``TRIBUTARY_LAKE_MAX_LAG_MINUTES``.
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
class IdSet:
    """One hour of the warehouse: distinct ids and their fingerprint (see the module doc)."""

    distinct: int
    fingerprint: int


@dataclass(frozen=True, slots=True)
class LakeCount:
    """One hour of the lake: rows, distinct ids and the fingerprint of the distinct ids."""

    rows: int
    distinct: int
    fingerprint: int


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
    warehouse: Mapping[datetime, IdSet], lake: Mapping[datetime, LakeCount], now: datetime
) -> list[CheckResult]:
    """One check per hour present in either store, in hour order.

    An hour fails on a count difference, on lake duplicates, or on a
    fingerprint difference. With equal counts and differing fingerprints the
    sets differ by an exchange of ids, which counts as one failure: how many
    ids were exchanged is not recoverable from the fingerprints.
    """
    checks = []
    for hour in sorted(set(warehouse) | set(lake)):
        expected = warehouse.get(hour, IdSet(0, 0))
        got = lake.get(hour, LakeCount(0, 0, 0))
        missing = max(expected.distinct - got.distinct, 0)
        extra = max(got.distinct - expected.distinct, 0)
        duplicates = got.rows - got.distinct
        ids_match = expected.distinct == got.distinct and expected.fingerprint == got.fingerprint
        exchanged = 1 if not ids_match and missing == extra == 0 else 0
        failures = missing + extra + duplicates + exchanged
        checks.append(
            CheckResult(
                check_name=f"reconcile_hour:{hour:%Y-%m-%dT%H}",
                check_type="reconcile",
                target=TARGET,
                status="pass" if failures == 0 else "fail",
                failures=failures,
                details={
                    "hour": hour.isoformat(),
                    "warehouse_distinct": expected.distinct,
                    "lake_rows": got.rows,
                    "lake_distinct": got.distinct,
                    "missing_in_lake": missing,
                    "extra_in_lake": extra,
                    "lake_duplicates": duplicates,
                    "warehouse_fingerprint": expected.fingerprint,
                    "lake_fingerprint": got.fingerprint,
                    "ids_match": ids_match,
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
    """Newest event time of the micro-batches the warehouse has fully committed.

    Not ``max(event_ts)`` of the table: a batch commits partition by partition,
    and the dedup shuffle spreads event times over partitions, so while one
    batch is half written the table's maximum can sit ahead of rows still to
    come for an hour that would already look closed. Rows loaded without the
    stream (``tributary sample load``) leave no progress; then the table's
    maximum is the only mark there is.
    """
    with psycopg.connect(conninfo, autocommit=True) as conn:
        return committed_newest(conn)


def committed_newest(conn: psycopg.Connection[tuple[object, ...]]) -> datetime | None:
    """``warehouse_newest`` on an open connection, so a test can run it inside a transaction.

    The progress table is looked up with ``to_regclass`` rather than by
    catching the error, which would abort an enclosing transaction (and a
    ``CASE`` would not help: the table name is resolved when the statement is
    parsed).
    """
    row: tuple[object, ...] | None = None
    exists = conn.execute("SELECT to_regclass('ops.stream_progress') IS NOT NULL").fetchone()
    if exists is not None and exists[0]:
        row = conn.execute(
            "SELECT max(max_event_ts) FROM ops.stream_progress WHERE query = 'warehouse'"
        ).fetchone()
    if row is None or row[0] is None:
        row = conn.execute("SELECT max(event_ts) FROM raw.recentchange").fetchone()
    return None if row is None or row[0] is None else _utc(row[0])


def warehouse_hours(conninfo: str, start: datetime, end: datetime) -> dict[datetime, IdSet]:
    """Ids and fingerprint per UTC hour in ``[start, end)``; the key makes rows = distinct ids.

    The hash is the first 8 hex digits of the MD5 read as a signed 32-bit
    integer, the same value Trino's ``from_big_endian_32`` gives in
    ``lake_hours``; ``sum`` over ``int`` is ``bigint`` on both sides.
    """
    with psycopg.connect(conninfo) as conn:
        rows = conn.execute(
            "SELECT date_trunc('hour', event_ts, 'UTC'), count(*),"
            " sum(('x' || substr(md5(event_id::text), 1, 8))::bit(32)::int)"
            " FROM raw.recentchange WHERE event_ts >= %s AND event_ts < %s GROUP BY 1",
            (start, end),
        ).fetchall()
    return {_utc(hour): IdSet(int(n), int(fp)) for hour, n, fp in rows}


def lake_hours(settings: Settings, start: datetime, end: datetime) -> dict[datetime, LakeCount]:
    """Rows, distinct ids and their fingerprint per UTC hour in ``[start, end)``.

    The inner query groups by id so the fingerprint sums each id once (as the
    warehouse's key does) while ``sum(n)`` still counts the rows; the filter
    prunes partitions.
    """
    rows = fetch(
        settings,
        "SELECT hour, sum(n), count(*),"
        " sum(from_big_endian_32(substr(md5(to_utf8(event_id)), 1, 4)))"
        " FROM (SELECT date_trunc('hour', event_ts) AS hour, event_id, count(*) AS n"
        f" FROM {lake_table(settings)} WHERE event_ts >= ? AND event_ts < ? GROUP BY 1, 2)"
        " GROUP BY 1",
        [start, end],
    )
    return {
        _utc(hour): LakeCount(int(str(n)), int(str(d)), int(str(fp))) for hour, n, d, fp in rows
    }


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
