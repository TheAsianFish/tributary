"""The ``stream_loss`` check: what the stream job dropped or rejected on the way in.

Two kinds of loss never reach either sink, so no count over the tables can see
them: rows the dedup operator drops because they are behind the watermark, and
records the parser rejects (malformed, or an event time further ahead of its
Kafka record time than the skew bound). The stream job writes both per batch
into ``ops.stream_progress`` from its progress reports, with the batch's Kafka
offsets; this module sums the batches of the last
``TRIBUTARY_STREAM_CHECK_WINDOW_HOURS`` per query and records one check per
query in ``ops.quality_runs``.

A row the watermark dropped is not necessarily a lost event: Spark counts a late
row before the dedup can know that it is a copy of an event that already landed
(a redelivery, a producer retry, a replay), so a counter alone cannot tell a
duplicate from a loss. A reconciliation pass (:mod:`tributary.stream.reconcile`)
reads the topic itself and settles it: each event below the committed offsets is
in ``raw.recentchange``, rejected, or missing, and the missing ones are written
back. So, for the warehouse query:

- ``fail`` when more dropped rows than ``TRIBUTARY_STREAM_MAX_DROPPED_ROWS``
  (default 0) sit in batches no pass has covered (a pass covers a batch when it
  read every offset the batch read), or when an event a pass in the window found
  missing is still not in ``raw.recentchange``, or when the rejected records
  exceed ``TRIBUTARY_STREAM_MAX_REJECTED_FRACTION`` of the input (default 0.1%:
  one bad record among thousands is the producer's problem, a steady share is
  the pipeline's);
- ``pass`` otherwise, with the counts in ``details``;
- no check at all for a query with no batch in the window.

With no pass on record this is exactly the check before reconciliation existed:
every dropped row counts. The lake query's drops are never covered (a pass judges
PostgreSQL, and nothing writes back to Iceberg), so its check still fails on the
counter; the per-hour PostgreSQL-against-Iceberg reconciliation names the gap.

The warehouse query's check targets ``raw.recentchange``, which every mart is
built from, so an answer's provenance names it when it fails; the lake query's
targets ``lake.raw.recentchange``, as the reconciliation's checks do.
"""

from __future__ import annotations

import sys
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import psycopg
import structlog
from psycopg.rows import TupleRow

from tributary.config import Settings
from tributary.stream.sink import Offsets, ensure_schema
from tributary.warehouse.quality import CheckResult, CheckStatus, record_checks

log = structlog.get_logger(__name__)

SOURCE = "stream"
CHECK_TYPE = "stream_loss"
TARGETS = {"warehouse": "raw.recentchange", "lake": "lake.raw.recentchange"}

WINDOW_SQL = (
    "SELECT query, count(*), coalesce(sum(input_rows), 0), coalesce(sum(dropped_by_watermark), 0),"
    " coalesce(sum(malformed_rows), 0), coalesce(sum(future_rows), 0),"
    " coalesce(sum(canary_rows), 0), min(committed_at), max(committed_at)"
    " FROM ops.stream_progress"
    " WHERE input_rows IS NOT NULL AND committed_at >= %s AND committed_at <= %s"
    " GROUP BY query ORDER BY query"
)


DROPPED_SQL = (
    "SELECT query, batch_id, dropped_by_watermark, start_offsets, end_offsets, committed_at"
    " FROM ops.stream_progress"
    " WHERE input_rows IS NOT NULL AND dropped_by_watermark > 0"
    " AND committed_at >= %s AND committed_at <= %s"
    " ORDER BY query, batch_id"
)
PASSES_SQL = (
    "SELECT id, query, start_offsets, end_offsets, finished_at FROM ops.reconcile_runs"
    " WHERE finished_at >= %s ORDER BY id"
)
# Events a pass in the window found missing and that are still not in the table,
# whatever any later pass did (an id written since, by anyone, no longer counts).
STILL_MISSING_SQL = (
    "SELECT r.query, count(DISTINCT m.event_id) FROM ops.reconcile_missing m"
    " JOIN ops.reconcile_runs r ON r.id = m.pass_id"
    " WHERE r.finished_at >= %s AND r.finished_at <= %s"
    " AND NOT EXISTS (SELECT 1 FROM raw.recentchange c WHERE c.event_id = m.event_id)"
    " GROUP BY r.query"
)


@dataclass(frozen=True, slots=True)
class DroppedBatch:
    """A batch that dropped rows as late, and where it read in the topic."""

    query: str
    batch_id: int
    dropped: int
    start_offsets: Offsets | None
    end_offsets: Offsets | None
    committed_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class PassWindow:
    """The offsets one reconciliation pass read: ``[start, end)`` per partition."""

    pass_id: int
    query: str
    start_offsets: Offsets
    end_offsets: Offsets


def covers(window: PassWindow, batch: DroppedBatch) -> bool:
    """Whether the pass read every offset the batch read (and judged the same query).

    A batch recorded before offsets were (no end offsets) is never covered. One
    with end offsets and no start (Spark reports none for a query's first batch)
    is taken to start at offset 0, the most a pass must have read to cover it.
    A partition the batch read nothing from (start = end) needs nothing.
    """
    if window.query != batch.query or batch.end_offsets is None:
        return False
    starts = batch.start_offsets or {}
    for topic, ends in batch.end_offsets.items():
        pass_start = window.start_offsets.get(topic)
        pass_end = window.end_offsets.get(topic)
        for partition, end in ends.items():
            begin = starts.get(topic, {}).get(partition, 0)
            if begin >= end:
                continue
            if pass_start is None or pass_end is None:
                return False
            if pass_start.get(partition, sys.maxsize) > begin or pass_end.get(partition, -1) < end:
                return False
    return True


def uncovered(batches: Iterable[DroppedBatch], passes: Sequence[PassWindow]) -> list[DroppedBatch]:
    """The batches with dropped rows that no pass has covered."""
    return [b for b in batches if not any(covers(p, b) for p in passes)]


class StreamLossError(RuntimeError):
    """At least one stream_loss check failed (raised after the checks were recorded)."""


@dataclass(frozen=True, slots=True)
class Thresholds:
    max_dropped_rows: int = 0
    max_rejected_fraction: float = 0.001

    @classmethod
    def from_settings(cls, settings: Settings) -> Thresholds:
        return cls(settings.stream_max_dropped_rows, settings.stream_max_rejected_fraction)


@dataclass(frozen=True, slots=True)
class WindowCounts:
    """One query's loss counters summed over the batches in the window."""

    query: str
    batches: int
    input_rows: int
    dropped_by_watermark: int
    malformed_rows: int
    future_rows: int
    canary_rows: int
    first_batch_at: datetime | None = None
    last_batch_at: datetime | None = None
    covered_dropped: int = 0
    """Dropped rows in batches a reconciliation pass has read and settled."""
    still_missing: int = 0
    """Events a pass in the window found missing that are still not in the table."""
    passes: int = 0
    """Reconciliation passes of this query in the window."""

    @property
    def rejected_rows(self) -> int:
        return self.malformed_rows + self.future_rows

    @property
    def uncovered_dropped(self) -> int:
        return self.dropped_by_watermark - self.covered_dropped


def evaluate(
    counts: WindowCounts,
    thresholds: Thresholds,
    *,
    window: timedelta,
    now: datetime,
) -> CheckResult:
    """The check for one query's window (pure)."""
    rejected_fraction = counts.rejected_rows / counts.input_rows if counts.input_rows else 0.0
    reasons = []
    if counts.uncovered_dropped > thresholds.max_dropped_rows:
        covered = (
            f" and not covered by a reconciliation pass ({counts.covered_dropped} covered)"
            if counts.passes
            else ""
        )
        reasons.append(
            f"{counts.uncovered_dropped} rows dropped by the watermark{covered}"
            f" (allowed {thresholds.max_dropped_rows})"
        )
    if counts.still_missing:
        reasons.append(
            f"{counts.still_missing} events a reconciliation pass found missing"
            " are still not in the table"
        )
    if rejected_fraction > thresholds.max_rejected_fraction:
        reasons.append(
            f"{counts.rejected_rows} of {counts.input_rows} records rejected"
            f" ({rejected_fraction:.3%}, allowed {thresholds.max_rejected_fraction:.3%})"
        )
    status: CheckStatus = "fail" if reasons else "pass"
    return CheckResult(
        check_name=f"{CHECK_TYPE}_{counts.query}",
        check_type=CHECK_TYPE,
        target=TARGETS.get(counts.query, counts.query),
        status=status,
        failures=counts.uncovered_dropped + counts.still_missing + counts.rejected_rows,
        details={
            "message": "; ".join(reasons) if reasons else None,
            "window_hours": window.total_seconds() / 3600,
            "batches": counts.batches,
            "input_rows": counts.input_rows,
            "dropped_by_watermark": counts.dropped_by_watermark,
            "covered_dropped": counts.covered_dropped,
            "uncovered_dropped": counts.uncovered_dropped,
            "reconcile_passes": counts.passes,
            "still_missing": counts.still_missing,
            "malformed_rows": counts.malformed_rows,
            "future_rows": counts.future_rows,
            "canary_rows": counts.canary_rows,
            "rejected_fraction": round(rejected_fraction, 6),
            "max_dropped_rows": thresholds.max_dropped_rows,
            "max_rejected_fraction": thresholds.max_rejected_fraction,
        },
        started_at=counts.first_batch_at,
        finished_at=now,
    )


def dropped_batches(
    conn: psycopg.Connection[TupleRow], start: datetime, end: datetime
) -> list[DroppedBatch]:
    """The batches committed in ``[start, end]`` that dropped rows as late."""
    rows = conn.execute(DROPPED_SQL, (start, end)).fetchall()
    return [
        DroppedBatch(
            query=str(r[0]),
            batch_id=int(r[1]),
            dropped=int(r[2]),
            start_offsets=_offsets(r[3]),
            end_offsets=_offsets(r[4]),
            committed_at=r[5] if isinstance(r[5], datetime) else None,
        )
        for r in rows
    ]


def pass_windows(conn: psycopg.Connection[TupleRow], since: datetime) -> list[PassWindow]:
    """Every reconciliation pass that finished at or after ``since``."""
    windows = []
    for r in conn.execute(PASSES_SQL, (since,)).fetchall():
        start, end = _offsets(r[2]), _offsets(r[3])
        if start is not None and end is not None:
            windows.append(PassWindow(int(r[0]), str(r[1]), start, end))
    return windows


def _offsets(value: object) -> Offsets | None:
    """A jsonb offsets column as :data:`Offsets` (None when NULL or not that shape)."""
    if not isinstance(value, dict):
        return None
    out: Offsets = {}
    for topic, partitions in value.items():
        if not isinstance(partitions, dict):
            return None
        out[str(topic)] = {str(p): int(o) for p, o in partitions.items()}
    return out


def window_counts(conninfo: str, start: datetime, end: datetime) -> list[WindowCounts]:
    """Per query, the loss counters of the batches committed in ``[start, end]``, with
    the dropped rows reconciliation passes have covered and the events they left missing."""
    ensure_schema(conninfo, "ops.sql")
    with psycopg.connect(conninfo, autocommit=True) as conn:
        rows = conn.execute(WINDOW_SQL, (start, end)).fetchall()
        dropped = dropped_batches(conn, start, end)
        passes = pass_windows(conn, start)
        exists = conn.execute("SELECT to_regclass('raw.recentchange') IS NOT NULL").fetchone()
        missing: dict[str, int] = {}
        if exists and exists[0]:
            for query, n in conn.execute(STILL_MISSING_SQL, (start, end)).fetchall():
                missing[str(query)] = int(n)
    left = uncovered(dropped, passes)
    covered: dict[str, int] = {}
    for b in dropped:
        covered[b.query] = covered.get(b.query, 0) + b.dropped
    for b in left:
        covered[b.query] -= b.dropped
    return [
        WindowCounts(
            query=str(r[0]),
            batches=int(r[1]),
            input_rows=int(r[2]),
            dropped_by_watermark=int(r[3]),
            malformed_rows=int(r[4]),
            future_rows=int(r[5]),
            canary_rows=int(r[6]),
            first_batch_at=r[7] if isinstance(r[7], datetime) else None,
            last_batch_at=r[8] if isinstance(r[8], datetime) else None,
            covered_dropped=covered.get(str(r[0]), 0),
            still_missing=missing.get(str(r[0]), 0),
            passes=sum(1 for p in passes if p.query == str(r[0])),
        )
        for r in rows
    ]


def uncovered_batches(
    conninfo: str, start: datetime, end: datetime, query: str = "warehouse"
) -> list[DroppedBatch]:
    """The batches of ``query`` in ``[start, end]`` whose dropped rows no pass has covered."""
    ensure_schema(conninfo, "ops.sql")
    with psycopg.connect(conninfo, autocommit=True) as conn:
        dropped = [b for b in dropped_batches(conn, start, end) if b.query == query]
        passes = pass_windows(conn, start)
    return uncovered(dropped, passes)


def check_stream(
    settings: Settings, run_id: str, *, now: datetime | None = None, raise_on_fail: bool = True
) -> list[CheckResult]:
    """Evaluate and record the window's checks; raise :class:`StreamLossError` if one
    failed (after recording it), unless ``raise_on_fail`` is false."""
    now = now or datetime.now(UTC)
    window = timedelta(hours=settings.stream_check_window_hours)
    thresholds = Thresholds.from_settings(settings)
    checks = [
        evaluate(c, thresholds, window=window, now=now)
        for c in window_counts(settings.pg_conninfo, now - window, now)
    ]
    record_checks(settings.pg_conninfo, run_id, checks, source=SOURCE)
    failed = [c for c in checks if c.status != "pass"]
    log.info("stream_loss.done", checks=len(checks), failed=len(failed), run_id=run_id)
    if failed and raise_on_fail:
        raise StreamLossError("; ".join(f"{c.check_name}: {c.details['message']}" for c in failed))
    return checks


def summary(checks: Sequence[CheckResult]) -> list[dict[str, object]]:
    """The checks as plain dicts (for the CLI and the benchmark's result files)."""
    return [
        {"check": c.check_name, "status": c.status, "target": c.target, **c.details} for c in checks
    ]
