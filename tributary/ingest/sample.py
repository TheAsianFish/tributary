"""The committed sample: gzip JSONL of trimmed events, and its statistics."""

from __future__ import annotations

import gzip
import json
import math
import os
import tempfile
from collections import Counter
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from tributary.events import Event, InvalidEventError, event_id, event_time

WATERMARK_DELAYS_S = (60, 120, 300, 600, 1800)
TOP_WIKIS = 5


def read_sample(path: Path, limit: int | None = None) -> Iterator[Event]:
    """Yield the events of a gzip JSONL file in file order, at most ``limit`` of them."""
    if limit is not None and limit <= 0:
        return
    count = 0
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, start=1):
            if not line.strip():
                continue
            event = json.loads(line)
            if not isinstance(event, dict):
                raise ValueError(f"{path}:{lineno}: not a JSON object")
            yield event
            count += 1
            if limit is not None and count >= limit:
                return


def encode_event(event: Event) -> str:
    """The canonical compact JSON form used in the sample and on the wire."""
    return json.dumps(event, ensure_ascii=False, separators=(",", ":"))


def write_sample(path: Path, events: Iterable[Event]) -> int:
    """Write events as gzip JSONL, atomically and byte-stably; return the count.

    The gzip header carries no file name and mtime 0, so the same events give
    the same bytes. The file appears under ``path`` only once fully written.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    tmp = Path(tmp_name)
    count = 0
    try:
        with os.fdopen(fd, "wb") as raw:
            with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as gz:
                for event in events:
                    gz.write(encode_event(event).encode("utf-8") + b"\n")
                    count += 1
            raw.flush()
            os.fsync(raw.fileno())
        tmp.chmod(0o644)  # mkstemp creates 0600; the sample is read by other users (containers)
        tmp.replace(path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    return count


@dataclass(frozen=True)
class Distribution:
    """Nearest-rank percentiles of a sample of values (seconds)."""

    count: int
    p50: float
    p90: float
    p99: float
    p999: float
    max: float

    @classmethod
    def of(cls, values: Sequence[float]) -> Distribution:
        ordered = sorted(values)
        if not ordered:
            return cls(0, 0.0, 0.0, 0.0, 0.0, 0.0)
        q = _quantile
        return cls(
            count=len(ordered),
            p50=q(ordered, 0.5),
            p90=q(ordered, 0.9),
            p99=q(ordered, 0.99),
            p999=q(ordered, 0.999),
            max=ordered[-1],
        )


def _quantile(ordered: Sequence[float], q: float) -> float:
    rank = max(1, math.ceil(q * len(ordered)))
    return ordered[rank - 1]


@dataclass(frozen=True)
class SampleStats:
    """Shape of an event stream, in stream order. See ``sample_stats``."""

    total: int
    invalid: int
    distinct_ids: int
    duplicate_ids: int
    by_type: dict[str, int]
    wikis: int
    top_wikis: list[tuple[str, int]]
    bot_share: float
    first_event_time: datetime | None
    last_event_time: datetime | None
    min_event_time: datetime | None
    max_event_time: datetime | None
    late_events: int
    lateness: Distribution
    dropped_by_watermark: dict[int, int]
    emit_delay: Distribution
    emit_delay_missing: int = 0
    watermark_delays_s: tuple[int, ...] = field(default=WATERMARK_DELAYS_S)


@dataclass
class _Accumulator:
    total: int = 0
    invalid: int = 0
    bots: int = 0
    emit_missing: int = 0
    ids: set[str] = field(default_factory=set)
    types: Counter[str] = field(default_factory=Counter)
    wikis: Counter[str] = field(default_factory=Counter)
    times: list[datetime] = field(default_factory=list)
    lateness: list[float] = field(default_factory=list)
    emit_delay: list[float] = field(default_factory=list)
    running_max: datetime | None = None

    def add(self, event: Event) -> None:
        self.total += 1
        try:
            eid, ts = event_id(event), event_time(event)
        except InvalidEventError:
            self.invalid += 1
            return
        self.ids.add(eid)
        self.types[str(event.get("type"))] += 1
        self.wikis[str(event.get("wiki"))] += 1
        self.bots += event.get("bot") is True
        self.times.append(ts)
        self.running_max = ts if self.running_max is None else max(self.running_max, ts)
        self.lateness.append((self.running_max - ts).total_seconds())
        delay = _emit_delay(event, ts)
        if delay is None:
            self.emit_missing += 1
        else:
            self.emit_delay.append(delay)


def _emit_delay(event: Event, ts: datetime) -> float | None:
    meta = event.get("meta")
    dt = meta.get("dt") if isinstance(meta, dict) else None
    if not isinstance(dt, str):
        return None
    try:
        emitted = datetime.fromisoformat(dt)
    except ValueError:
        return None
    if emitted.tzinfo is None:
        return None
    return (emitted - ts).total_seconds()


def sample_stats(
    events: Iterable[Event], watermark_delays_s: Sequence[int] = WATERMARK_DELAYS_S
) -> SampleStats:
    """Summarise a stream, including how late its events arrive relative to each other.

    Lateness of an event is the running maximum of ``event_time`` over the
    events before it (in iteration order) minus its own ``event_time``. An
    event-time watermark with delay *d* drops events whose lateness exceeds
    *d*. Spark advances its watermark once per micro-batch, from the previous
    batch's maximum, not after every event, so these per-event drop counts
    are the worst case for a single ordered stream; spreading the stream over
    Kafka partitions changes the order Spark sees.

    Emit delay is ``meta.dt - timestamp``: how long after the change the
    stream emitted it. Events with an invalid id or timestamp count only in
    ``total`` and ``invalid``.
    """
    acc = _Accumulator()
    for event in events:
        acc.add(event)
    valid = acc.total - acc.invalid
    times = acc.times
    return SampleStats(
        total=acc.total,
        invalid=acc.invalid,
        distinct_ids=len(acc.ids),
        duplicate_ids=valid - len(acc.ids),
        by_type=dict(acc.types.most_common()),
        wikis=len(acc.wikis),
        top_wikis=acc.wikis.most_common(TOP_WIKIS),
        bot_share=acc.bots / valid if valid else 0.0,
        first_event_time=times[0] if times else None,
        last_event_time=times[-1] if times else None,
        min_event_time=min(times) if times else None,
        max_event_time=max(times) if times else None,
        late_events=sum(1 for v in acc.lateness if v > 0),
        lateness=Distribution.of(acc.lateness),
        dropped_by_watermark={d: sum(1 for v in acc.lateness if v > d) for d in watermark_delays_s},
        emit_delay=Distribution.of(acc.emit_delay),
        emit_delay_missing=acc.emit_missing,
        watermark_delays_s=tuple(watermark_delays_s),
    )


def _fmt_time(value: datetime | None) -> str:
    return value.isoformat() if value is not None else "-"


def _fmt_dist(dist: Distribution) -> str:
    return (
        f"p50 {dist.p50:g} s, p90 {dist.p90:g} s, p99 {dist.p99:g} s, "
        f"p99.9 {dist.p999:g} s, max {dist.max:g} s"
    )


def _fmt_delay(seconds: int) -> str:
    return f"{seconds // 60} min" if seconds % 60 == 0 else f"{seconds} s"


def _pct(part: int, whole: int) -> str:
    return f"{100 * part / whole:.3f}%" if whole else "-"


def format_stats(stats: SampleStats) -> str:
    """A plain-text (markdown-friendly) report of ``sample_stats``."""
    valid = stats.total - stats.invalid
    span = (
        stats.max_event_time - stats.min_event_time
        if stats.max_event_time and stats.min_event_time
        else None
    )
    lines = [
        "## Sample",
        f"- events: {stats.total} ({stats.invalid} invalid)",
        f"- distinct ids: {stats.distinct_ids}; duplicate ids: {stats.duplicate_ids}",
        "- by type: " + ", ".join(f"{k} {v}" for k, v in stats.by_type.items()),
        f"- wikis: {stats.wikis}; top: " + ", ".join(f"{wiki} {n}" for wiki, n in stats.top_wikis),
        f"- bot share: {100 * stats.bot_share:.1f}%",
        "",
        "## Event time",
        f"- first / last in stream order: {_fmt_time(stats.first_event_time)} / "
        f"{_fmt_time(stats.last_event_time)}",
        f"- min / max: {_fmt_time(stats.min_event_time)} / {_fmt_time(stats.max_event_time)}"
        + (f" (span {span})" if span is not None else ""),
        "",
        "## Lateness in stream order (running max of event time minus event time)",
        f"- late events: {stats.late_events} ({_pct(stats.late_events, valid)})",
        f"- {_fmt_dist(stats.lateness)}",
        "- dropped by a watermark of (worst case, per-event watermark):",
    ]
    for delay in stats.watermark_delays_s:
        dropped = stats.dropped_by_watermark.get(delay, 0)
        lines.append(f"  - {_fmt_delay(delay)}: {dropped} ({_pct(dropped, valid)})")
    lines += [
        "",
        "## Emit delay (meta.dt minus timestamp)",
        f"- p50 {stats.emit_delay.p50:g} s, p99 {stats.emit_delay.p99:g} s, "
        f"max {stats.emit_delay.max:g} s ({stats.emit_delay_missing} without meta.dt)",
    ]
    return "\n".join(lines) + "\n"
