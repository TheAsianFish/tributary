"""The benchmark's decision rules, fixed before any run and tested offline.

**Sustained.** A rate is sustained when the job keeps up with it. Each
micro-batch's progress reports the broker's latest offsets when the trigger
fired and the offsets the batch started from; their difference is the queue
the batch found. Under a rate the job can carry, that queue stays bounded (a
trigger's worth of events, or a batch's worth when batches run longer than
the trigger). Above it, every batch finds more than the one before. The rule:
over the last ``window`` seconds of a step, the least-squares slope of the
queue against trigger time is at most ``tolerance`` times the offered rate
(5%: a job falling 5% behind is not keeping up), from at least
``min_batches`` batches. The step must also have been offered: a load
generator that sent under 98% of its target makes the step "not offered", not
sustained.

**Ladder.** Steps start at ``start`` times the full pipeline's drain rate and
grow by ``factor`` until a step is not sustained; if the first is not, they
shrink by ``factor`` until one is. The sustained rate is the highest
sustained step, so its resolution is ``factor``.

**Drain rate.** A stage's ceiling is how fast it empties a backlog: rows over
the summed trigger time of the full batches, the first batch left out (JIT
warm-up, connection set-up, state store creation) and the last left out if
it is partial.

**Attribution.** The stages of one task run one after another (Kafka read,
parse and dedup in the JVM; the rows' trip to the Python worker; the
PostgreSQL write), so their per-event times add up: ``1 / noop`` for the
JVM, ``1 / python - 1 / noop`` for the transfer, ``1 / sink`` for the
database (measured alone). The bottleneck is the stage with the largest
share of the per-event time; the broker runs in its own process, in
parallel, and is the bottleneck only if its own ceiling is below the
pipeline's rate.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

TOLERANCE = 0.05
OFFERED_MIN = 0.98
MIN_BATCHES = 4


def _offsets(raw: object) -> int:
    if not isinstance(raw, Mapping):
        return 0
    return sum(
        int(v) for parts in raw.values() if isinstance(parts, Mapping) for v in parts.values()
    )


@dataclass(frozen=True)
class Batch:
    """The parts of one ``StreamingQueryProgress`` the rules use."""

    batch_id: int
    trigger_at: float
    """When the trigger fired (epoch seconds)."""
    queued: int
    """Broker latest offsets minus the batch's start offsets, summed over partitions."""
    rows: int
    trigger_ms: float
    add_batch_ms: float
    durations: Mapping[str, float]

    @property
    def ended_at(self) -> float:
        return self.trigger_at + self.trigger_ms / 1000


def batch_from_progress(progress: Mapping[str, Any]) -> Batch | None:
    """A :class:`Batch` from one logged progress line; None for a line without sources."""
    sources = progress.get("sources") or []
    if not sources:
        return None
    src = sources[0]
    durations = {k: float(v) for k, v in (progress.get("duration_ms") or {}).items()}
    trigger_ms = durations.get("triggerExecution", 0.0)
    stamp = progress.get("timestamp")
    # The structlog timestamp is when the listener logged the batch, i.e. its end.
    ended = datetime.fromisoformat(str(stamp).replace("Z", "+00:00")).timestamp()
    start = _offsets(src.get("start"))
    return Batch(
        batch_id=int(progress.get("batch_id", -1)),
        trigger_at=ended - trigger_ms / 1000,
        queued=_offsets(src.get("latest")) - start,
        rows=int(progress.get("input_rows", 0)),
        trigger_ms=trigger_ms,
        add_batch_ms=durations.get("addBatch", 0.0),
        durations=durations,
    )


def slope(points: Sequence[tuple[float, float]]) -> float | None:
    """Least-squares slope of y on x; None with fewer than two distinct x."""
    n = len(points)
    if n < 2:
        return None
    mx = sum(x for x, _ in points) / n
    my = sum(y for _, y in points) / n
    sxx = sum((x - mx) ** 2 for x, _ in points)
    if sxx == 0:
        return None
    return sum((x - mx) * (y - my) for x, y in points) / sxx


@dataclass(frozen=True)
class Verdict:
    sustained: bool
    reason: str
    queue_slope: float | None
    batches: int
    offered_ratio: float


def judge_step(
    batches: Sequence[Batch],
    rate: float,
    offered: int,
    step_end: float,
    seconds: float,
    *,
    window: float = 60.0,
    tolerance: float = TOLERANCE,
    min_batches: int = MIN_BATCHES,
) -> Verdict:
    """Apply the sustained rule to one step that ended at ``step_end`` after ``seconds``."""
    ratio = offered / (rate * seconds) if rate > 0 and seconds > 0 else 0.0
    in_window = [b for b in batches if step_end - window <= b.trigger_at <= step_end]
    s = slope([(b.trigger_at, float(b.queued)) for b in in_window])
    if ratio < OFFERED_MIN:
        return Verdict(False, "not offered", s, len(in_window), ratio)
    if len(in_window) < min_batches:
        return Verdict(False, "too few batches", s, len(in_window), ratio)
    if s is None or s > tolerance * rate:
        return Verdict(False, "queue growing", s, len(in_window), ratio)
    return Verdict(True, "kept up", s, len(in_window), ratio)


def next_rate(history: Sequence[tuple[float, bool]], factor: float) -> float | None:
    """The ladder's next rate, or None when the sustained rate is known.

    ``history`` is every (rate, sustained) so far, in order. Going up, stop
    after the first failure; going down (the first step failed), stop at the
    first success.
    """
    if not history:
        raise ValueError("the ladder needs a first step")
    _first_rate, first_ok = history[0]
    last_rate, last_ok = history[-1]
    if first_ok:
        return last_rate * factor if last_ok else None
    return last_rate / factor if not last_ok else None


def sustained_rate(history: Sequence[tuple[float, bool]]) -> float | None:
    """The highest sustained step (None if none was)."""
    ok = [r for r, good in history if good]
    return max(ok) if ok else None


@dataclass(frozen=True)
class Drain:
    rows: int
    seconds: float
    batches: int

    @property
    def rows_per_s(self) -> float | None:
        return self.rows / self.seconds if self.seconds > 0 else None


def drain_rate(batches: Sequence[Batch], cap: int) -> Drain:
    """Rows per second of trigger time over the full batches after the first."""
    data = [b for b in batches if b.rows > 0]
    if len(data) > 1:
        data = data[1:]
    if len(data) > 1 and data[-1].rows < 0.9 * cap:
        data = data[:-1]
    return Drain(
        rows=sum(b.rows for b in data),
        seconds=sum(b.trigger_ms for b in data) / 1000,
        batches=len(data),
    )


@dataclass(frozen=True)
class Attribution:
    """Per-event time of each serial stage (µs) and the bottleneck."""

    jvm_us: float
    transfer_us: float
    database_us: float
    predicted_per_s: float
    bottleneck: str
    shares: Mapping[str, float]


def attribute(
    noop: float, python: float, sink: float, broker: float | None, pipeline: float
) -> Attribution:
    """Split the per-event time between the serial stages and name the bottleneck.

    All arguments are events/s: the ``noop`` and ``python`` drains, the sink
    replay, the broker's consumer ceiling, and the full pipeline's drain.
    """
    jvm = 1e6 / noop
    transfer = max(0.0, 1e6 / python - jvm)
    database = 1e6 / sink
    total = jvm + transfer + database
    shares = {
        "spark (JVM)": jvm / total,
        "python transfer": transfer / total,
        "postgresql sink": database / total,
    }
    if broker is not None and broker < pipeline * 1.1:
        bottleneck = "broker"
    else:
        bottleneck = max(shares, key=lambda k: shares[k])
    return Attribution(
        jvm_us=round(jvm, 2),
        transfer_us=round(transfer, 2),
        database_us=round(database, 2),
        predicted_per_s=round(1e6 / total, 1),
        bottleneck=bottleneck,
        shares={k: round(v, 3) for k, v in shares.items()},
    )
