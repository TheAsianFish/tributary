"""The ``stream_loss`` check: what the stream job dropped or rejected on the way in.

Two kinds of loss never reach either sink, so no count over the tables can see
them: rows the dedup operator drops because they are behind the watermark (a
real event, lost for good), and records the parser rejects (malformed, or an
event time further ahead of its Kafka record time than the skew bound). The
stream job writes both per batch into ``ops.stream_progress`` from its progress
reports; this module sums the batches of the last
``TRIBUTARY_STREAM_CHECK_WINDOW_HOURS`` per query and records one check per
query in ``ops.quality_runs``:

- ``fail`` when more rows than ``TRIBUTARY_STREAM_MAX_DROPPED_ROWS`` (default 0)
  were dropped by the watermark, or when the rejected records exceed
  ``TRIBUTARY_STREAM_MAX_REJECTED_FRACTION`` of the input (default 0.1%: one
  bad record among thousands is the producer's problem, a steady share is the
  pipeline's);
- ``pass`` otherwise, with the counts in ``details``;
- no check at all for a query with no batch in the window.

The warehouse query's check targets ``raw.recentchange``, which every mart is
built from, so an answer's provenance names it when it fails; the lake query's
targets ``lake.raw.recentchange``, as the reconciliation's checks do.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import psycopg
import structlog

from tributary.config import Settings
from tributary.stream.sink import ensure_schema
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

    @property
    def rejected_rows(self) -> int:
        return self.malformed_rows + self.future_rows


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
    if counts.dropped_by_watermark > thresholds.max_dropped_rows:
        reasons.append(
            f"{counts.dropped_by_watermark} rows dropped by the watermark"
            f" (allowed {thresholds.max_dropped_rows})"
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
        failures=counts.dropped_by_watermark + counts.rejected_rows,
        details={
            "message": "; ".join(reasons) if reasons else None,
            "window_hours": window.total_seconds() / 3600,
            "batches": counts.batches,
            "input_rows": counts.input_rows,
            "dropped_by_watermark": counts.dropped_by_watermark,
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


def window_counts(conninfo: str, start: datetime, end: datetime) -> list[WindowCounts]:
    """Per query, the loss counters of the batches committed in ``[start, end]``."""
    ensure_schema(conninfo, "ops.sql")
    with psycopg.connect(conninfo, autocommit=True) as conn:
        rows = conn.execute(WINDOW_SQL, (start, end)).fetchall()
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
        )
        for r in rows
    ]


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
