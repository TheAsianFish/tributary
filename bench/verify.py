"""Exactly-once and latency, read back from the stores.

A run's events are every id ``prefix-0`` .. ``prefix-(n-1)`` (see
:mod:`bench.load`). Its rows are fetched by that id range from
``raw.recentchange`` and from the Iceberg table (through Trino), and compared
with the expected set: ``missing`` (produced and acknowledged, never landed),
``duplicates`` (rows beyond the first per id) and ``unexpected`` (an id in the
run's range that was never produced). All three must be 0.

End-to-end latency is ``ingested_at`` minus ``kafka_ts``: the Kafka record
timestamp, set by the producer when it hands the event over, against the
sink's ``clock_timestamp()`` at insert. Both clocks are the host's (containers
share its kernel clock), so there is no cross-machine skew.
"""

from __future__ import annotations

import time
from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass
from typing import Any

import psycopg

from bench.load import bench_id, id_range
from tributary.config import Settings


@dataclass(frozen=True)
class Delivery:
    """How a store's rows for one run compare with what was produced."""

    expected: int
    rows: int
    distinct: int
    missing: int
    duplicates: int
    unexpected: int

    @property
    def exactly_once(self) -> bool:
        return self.missing == 0 and self.duplicates == 0 and self.unexpected == 0

    def as_dict(self) -> dict[str, Any]:
        return {**asdict(self), "exactly_once": self.exactly_once}


def compare(prefix: str, expected: int, landed: Iterable[str]) -> Delivery:
    """Count missing, duplicate and unexpected ids of ``landed`` against ``prefix-0..n-1``."""
    counts = Counter(str(i).lower() for i in landed)
    wanted = {bench_id(prefix, s) for s in range(expected)}
    present = set(counts)
    return Delivery(
        expected=expected,
        rows=sum(counts.values()),
        distinct=len(present),
        missing=len(wanted - present),
        duplicates=sum(n - 1 for n in counts.values()),
        unexpected=len(present - wanted),
    )


def pg_ids(conninfo: str, prefix: str) -> list[str]:
    """Every ``event_id`` (one per row) of the run in ``raw.recentchange``."""
    low, high = id_range(prefix)
    with psycopg.connect(conninfo) as conn:
        rows = conn.execute(
            "SELECT event_id::text FROM raw.recentchange WHERE event_id BETWEEN %s AND %s",
            (low, high),
        ).fetchall()
    return [str(r[0]) for r in rows]


def pg_count(conninfo: str, prefix: str) -> int:
    low, high = id_range(prefix)
    with psycopg.connect(conninfo) as conn:
        row = conn.execute(
            "SELECT count(*) FROM raw.recentchange WHERE event_id BETWEEN %s AND %s", (low, high)
        ).fetchone()
    return int(str(row[0])) if row else 0


def lake_ids(settings: Settings, prefix: str) -> list[str]:
    """Every ``event_id`` (one per row) of the run in the Iceberg table, through Trino."""
    from tributary.lake.trino_client import fetch

    low, high = id_range(prefix)
    rows = fetch(
        settings,
        f"SELECT event_id FROM {settings.trino_lake_table} WHERE event_id BETWEEN ? AND ?",
        [low, high],
    )
    return [str(r[0]) for r in rows]


def lake_count(settings: Settings, prefix: str) -> int:
    from tributary.lake.trino_client import fetch

    low, high = id_range(prefix)
    rows = fetch(
        settings,
        f"SELECT count(*) FROM {settings.trino_lake_table} WHERE event_id BETWEEN ? AND ?",
        [low, high],
    )
    return int(str(rows[0][0])) if rows else 0


def wait_until(
    count: Any, expected: int, timeout: float, *, settle: float = 0.0, poll: float = 2.0
) -> tuple[int, float]:
    """Poll ``count()`` until it reaches ``expected`` (or stops changing for ``settle`` s).

    Returns the last count and the seconds waited. ``settle`` > 0 ends the
    wait early once the count has not moved for that long *and* is short of
    ``expected``, so a run that lost rows does not wait out the full timeout.
    """
    # Any: a zero-argument callable returning int.
    start = time.monotonic()
    last = -1
    last_change = start
    while True:
        try:
            n = int(count())
        except Exception:
            n = last
        now = time.monotonic()
        if n != last:
            last, last_change = n, now
        if n >= expected:
            return n, now - start
        if now - start > timeout or (settle and now - last_change > settle):
            return n, now - start
        time.sleep(poll)


LATENCY_SQL = """
SELECT count(*),
       percentile_cont(ARRAY[0.5, 0.95, 0.99]) WITHIN GROUP (ORDER BY ms),
       max(ms),
       min(ingested_at), max(ingested_at), min(kafka_ts), max(kafka_ts)
FROM (
    SELECT extract(epoch FROM ingested_at - kafka_ts) * 1000 AS ms, ingested_at, kafka_ts
    FROM raw.recentchange
    WHERE event_id BETWEEN %s AND %s
      AND kafka_ts >= to_timestamp(%s) AND kafka_ts < to_timestamp(%s)
) t
"""


@dataclass(frozen=True)
class Latency:
    """End-to-end latency (ms) of the rows produced in a window, and their landing rate."""

    rows: int
    p50_ms: float | None
    p95_ms: float | None
    p99_ms: float | None
    max_ms: float | None
    produced_per_s: float | None
    """Rows over the span of their Kafka timestamps."""
    landed_per_s: float | None
    """Rows over the span of their insert times."""


def latency(
    conninfo: str, prefix: str, start: float = 0.0, end: float = 4_000_000_000.0
) -> Latency:
    """Quantiles of ``ingested_at - kafka_ts`` for the run's rows produced in ``[start, end)``."""
    low, high = id_range(prefix)
    with psycopg.connect(conninfo) as conn:
        row = conn.execute(LATENCY_SQL, (low, high, start, end)).fetchone()
    assert row is not None
    n = int(str(row[0]))
    if n == 0:
        return Latency(0, None, None, None, None, None, None)
    p50, p95, p99 = (float(x) for x in row[1])
    landed_span = (row[4] - row[3]).total_seconds()
    produced_span = (row[6] - row[5]).total_seconds()
    return Latency(
        rows=n,
        p50_ms=round(p50, 1),
        p95_ms=round(p95, 1),
        p99_ms=round(p99, 1),
        max_ms=round(float(row[2]), 1),
        produced_per_s=round(n / produced_span, 1) if produced_span > 0 else None,
        landed_per_s=round(n / landed_span, 1) if landed_span > 0 else None,
    )


def quantile(values: Sequence[float], q: float) -> float | None:
    """Linear-interpolated quantile (the same rule as ``percentile_cont``)."""
    if not values:
        return None
    ordered = sorted(values)
    pos = q * (len(ordered) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(ordered) - 1)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (pos - lo)


def truncate_landing(conninfo: str) -> None:
    """Empty ``raw.recentchange`` and ``ops.stream_progress`` (benchmark stacks only)."""
    with psycopg.connect(conninfo) as conn:
        conn.execute("TRUNCATE raw.recentchange, ops.stream_progress")
