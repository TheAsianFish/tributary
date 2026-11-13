"""Keyed benchmark events and an open-loop producer.

Every benchmark run mints its own ids: ``<run prefix>-<scrambled sequence, 12 hex digits>``,
a valid lowercase UUID whose first 24 hex digits name the run. A run's rows
are then one index range of ``raw.recentchange`` (``event_id BETWEEN`` the
prefix's first and last id), so loss and duplicates are counted per run
without shipping 50,000 ids to the database.

The payloads are the committed sample's events, cycled, with the id replaced and
event time moved to the wall clock at send time minus the event's own
lateness in the sample (how far it trailed the running maximum of event time,
at most 143 s). A replay keeps the sample's disorder, and its event times
keep up with the clock however fast it is sent, so the stream job's
watermark never drops a benchmark event as late and runs do not interfere
through it. ``meta.dt`` becomes the send time.
"""

from __future__ import annotations

import json
import multiprocessing as mp
import resource
import secrets
import time
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from tributary.events import event_time
from tributary.ingest.sample import read_sample

ID_DIGITS = 12
"""Hex digits of the sequence number at the end of every benchmark id."""


def new_prefix() -> str:
    """A fresh run prefix: the first 24 hex digits of a UUID, dashed (``8-4-4-4``)."""
    h = secrets.token_hex(12)
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}"


_SPACE = 16**ID_DIGITS
_MULTIPLIER = 0x9E3779B97F4B  # odd, so multiplication is a bijection modulo 2**48
_MASK = 0x5DEECE66D1F3


def scramble(seq: int) -> int:
    """A bijection of ``[0, 16**12)``: consecutive sequence numbers land far apart."""
    if not 0 <= seq < _SPACE:
        raise ValueError(f"sequence out of range: {seq}")
    return int((seq * _MULTIPLIER) % _SPACE) ^ _MASK


def bench_id(prefix: str, seq: int) -> str:
    """The id of event ``seq`` of the run ``prefix``.

    The sequence is scrambled so a run's ids arrive in random order within its
    range, as real UUIDv4s do: sequential ids would only ever append at the
    right edge of the primary key's btree and flatter the PostgreSQL sink.
    """
    return f"{prefix}-{scramble(seq):0{ID_DIGITS}x}"


def id_range(prefix: str) -> tuple[str, str]:
    """The first and last id a run can mint (inclusive), for ``BETWEEN``."""
    return f"{prefix}-{0:0{ID_DIGITS}x}", f"{prefix}-{_SPACE - 1:0{ID_DIGITS}x}"


def lateness(times: Sequence[float]) -> list[int]:
    """Seconds each event trails the running maximum of the event times before it."""
    out: list[int] = []
    newest = float("-inf")
    for t in times:
        newest = max(newest, t)
        out.append(int(newest - t))
    return out


_MARKS = {"id": "\x00ID\x00", "dt": "\x00DT\x00", "ts": 4_000_000_000_123}


@dataclass(frozen=True)
class Template:
    """One sample event as compact JSON with holes for id, ``meta.dt`` and ``timestamp``.

    ``pieces`` has one more element than ``holes``; rendering interleaves them,
    three string joins instead of a ``json.dumps`` per event, so one producer
    process is not the first thing to saturate.
    """

    pieces: tuple[str, ...]
    holes: tuple[str, ...]
    lateness_s: int

    def render(self, event_id: str, now: float, ahead_s: int | None = None) -> bytes:
        """The event sent at ``now``; ``ahead_s`` stamps it that far past ``now`` instead
        of its own lateness behind it (the poison of ``bench.faults``' ``poison`` scenario;
        negative: that far behind, ``bench.late``'s late events)."""
        stamp = datetime.fromtimestamp(now, UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        ts = int(now) + ahead_s if ahead_s is not None else int(now) - self.lateness_s
        values = {"id": event_id, "dt": stamp, "ts": str(ts)}
        out = [self.pieces[0]]
        for hole, piece in zip(self.holes, self.pieces[1:], strict=True):
            out.append(values[hole])
            out.append(piece)
        return "".join(out).encode()


def make_template(event: dict[str, Any], lateness_s: int) -> Template:
    """Split the event's compact JSON around the three fields a benchmark rewrites."""
    marked: dict[str, Any] = json.loads(json.dumps(event))
    marked["meta"]["id"] = _MARKS["id"]
    marked["meta"]["dt"] = _MARKS["dt"]
    marked["timestamp"] = _MARKS["ts"]
    text = json.dumps(marked, ensure_ascii=False, separators=(",", ":"))
    tokens = {name: json.dumps(mark) for name, mark in _MARKS.items()}
    found = sorted((text.index(tok), name) for name, tok in tokens.items())
    pieces: list[str] = []
    cursor = 0
    for pos, name in found:
        # A string value keeps its quotes in the pieces either side of the hole.
        quote = '"' if isinstance(_MARKS[name], str) else ""
        pieces.append(text[cursor:pos] + quote)
        cursor = pos + len(tokens[name]) - len(quote)
    pieces.append(text[cursor:])
    return Template(tuple(pieces), tuple(name for _, name in found), lateness_s)


def load_templates(path: Path) -> list[Template]:
    """Every sample event as a :class:`Template`, in sample order."""
    events = list(read_sample(path))
    late = lateness([event_time(e).timestamp() for e in events])
    return [make_template(e, s) for e, s in zip(events, late, strict=True)]


@dataclass(frozen=True)
class Step:
    """Offer ``rate`` events/s (all workers together) for ``seconds``."""

    rate: float
    seconds: float


@dataclass(frozen=True)
class LoadPlan:
    """What a load generator sends: ids ``prefix-0 ..``, paced by ``steps``.

    ``limit`` stops the run after that many events in total (the fault runs
    send exactly 50,000); without it the steps alone decide. A step rate of 0
    sends as fast as the workers can (the backlog fills).
    """

    bootstrap: str
    topic: str
    prefix: str
    steps: tuple[Step, ...]
    sample_path: Path
    workers: int = 1
    limit: int | None = None
    poison: tuple[int, int] | None = None
    """``(seq, ahead_s)``: event ``seq`` is stamped ``ahead_s`` past its send time."""
    shifts: tuple[tuple[int, int], ...] = ()
    """More ``(seq, ahead_s)`` pairs, ``ahead_s`` negative for an event sent late."""
    malformed: tuple[int, ...] = ()
    """Sequence numbers sent as a record that is not JSON (the key is still the id)."""

    def stamps(self) -> dict[int, int]:
        """Every event stamped off its own lateness: seq to seconds past its send time."""
        out = dict(self.shifts)
        if self.poison is not None:
            out[self.poison[0]] = self.poison[1]
        return out


def malformed_value(event_id: str) -> bytes:
    """A record the parser rejects as malformed: a JSON object cut off after the id."""
    return b'{"meta":{"id":"' + event_id.encode() + b'","dt":'


def due(elapsed: float, rate: float) -> int:
    """How many events of an open-loop step are due ``elapsed`` seconds into it."""
    if elapsed <= 0:
        return 0
    return int(elapsed * rate)


class _Shared:
    """Counters shared by the workers and the parent; workers publish every 0.25 s."""

    def __init__(self, ctx: Any, steps: int) -> None:
        # Any: multiprocessing's context and shared-value types are not public in typeshed.
        self.sent: Any = ctx.Array("q", steps)
        self.delivered: Any = ctx.Value("q", 0)
        self.failed: Any = ctx.Value("q", 0)
        self.ready: Any = ctx.Value("i", 0)
        self.done: Any = ctx.Value("i", 0)
        self.cpu_s: Any = ctx.Value("d", 0.0)
        self.stop: Any = ctx.Event()


def _add(value: Any, n: int) -> None:
    if n:
        with value.get_lock():
            value.value += n


def _worker(plan: LoadPlan, index: int, start: Any, shared: _Shared) -> None:
    """One producer process: ids ``index, index + workers, ...``, its share of each step."""
    from confluent_kafka import KafkaError, Message

    from tributary.ingest.producer import make_producer

    templates = load_templates(plan.sample_path)
    producer = make_producer(plan.bootstrap, client_id=f"tributary-bench-{index}")
    counts = {"delivered": 0, "failed": 0}

    def on_delivery(err: KafkaError | None, _msg: Message) -> None:
        counts["failed" if err is not None else "delivered"] += 1

    published = {"delivered": 0, "failed": 0, "sent": 0}

    def publish(step_no: int, sent_in_step: int) -> None:
        with shared.sent.get_lock():
            shared.sent[step_no] += sent_in_step - published["sent"]
        published["sent"] = sent_in_step
        for name in ("delivered", "failed"):
            _add(getattr(shared, name), counts[name] - published[name])
            published[name] = counts[name]

    _add(shared.ready, 1)
    while start.value == 0.0:  # the parent sets the common start once every worker is ready
        time.sleep(0.005)
    t0 = float(start.value)
    time.sleep(max(0.0, t0 - time.monotonic()))
    seq = index
    limit = plan.limit
    stamps = plan.stamps()
    broken = frozenset(plan.malformed)
    step_start = t0
    try:
        for step_no, step in enumerate(plan.steps):
            rate = step.rate / plan.workers
            step_end = step_start + step.seconds
            sent_in_step = 0
            published["sent"] = 0
            last_publish = time.monotonic()
            exhausted = False
            while not shared.stop.is_set():
                now = time.monotonic()
                if now >= step_end:
                    break
                target = due(now - step_start, rate) if rate > 0 else sent_in_step + 500
                if sent_in_step >= target:
                    producer.poll(0)
                    time.sleep(0.001)
                    continue
                wall = time.time()
                while sent_in_step < target:
                    if limit is not None and seq >= limit:
                        exhausted = True
                        break
                    event_id = bench_id(plan.prefix, seq)
                    if seq in broken:
                        value = malformed_value(event_id)
                    else:
                        template = templates[seq % len(templates)]
                        value = template.render(event_id, wall, stamps.get(seq))
                    while True:
                        try:
                            producer.produce(
                                plan.topic,
                                value=value,
                                key=event_id.encode(),
                                on_delivery=on_delivery,
                            )
                            break
                        except BufferError:
                            producer.poll(0.05)
                    seq += plan.workers
                    sent_in_step += 1
                producer.poll(0)
                if now - last_publish >= 0.25:
                    publish(step_no, sent_in_step)
                    last_publish = now
                if exhausted:
                    break
            publish(step_no, sent_in_step)
            step_start = step_end
            if exhausted or shared.stop.is_set():
                break
    finally:
        # A broker outage can hold messages in the local queue; give them the
        # producer's own delivery timeout (300 s) before counting them as failed.
        undelivered = producer.flush(330)
        counts["failed"] += undelivered
        publish_final = counts["delivered"] - published["delivered"]
        _add(shared.delivered, publish_final)
        _add(shared.failed, counts["failed"] - published["failed"])
        usage = resource.getrusage(resource.RUSAGE_SELF)
        with shared.cpu_s.get_lock():
            shared.cpu_s.value += usage.ru_utime + usage.ru_stime
        _add(shared.done, 1)


@dataclass(frozen=True)
class LoadResult:
    """What a load generator sent, per step, and what the broker acknowledged."""

    sent_per_step: tuple[int, ...]
    delivered: int
    failed: int
    started_at: float
    """``time.time()`` when the first step began."""
    cpu_s: float = 0.0
    """CPU seconds the worker processes used, start-up included (they share the host)."""

    @property
    def sent(self) -> int:
        return sum(self.sent_per_step)


class LoadGenerator:
    """Run a :class:`LoadPlan` in ``plan.workers`` processes (spawned, so no forked JVM state).

    The steps start together at a shared ``time.monotonic()`` deadline (the
    clock is system-wide on Linux), a second after every worker has loaded its
    templates, so the pacing of all workers lines up.
    """

    def __init__(self, plan: LoadPlan) -> None:
        self.plan = plan
        self._ctx = mp.get_context("spawn")
        self._shared = _Shared(self._ctx, len(plan.steps))
        self._procs: list[Any] = []
        self.t0 = 0.0
        self.started_at = 0.0

    def start(self, timeout: float = 60.0) -> None:
        # The workers wait for t0; pick it once they are ready.
        self.t0 = time.monotonic() + 3600.0
        ready_deadline = time.monotonic() + timeout
        shared_t0 = self._ctx.Value("d", 0.0)
        self._procs = [
            self._ctx.Process(
                target=_worker, args=(self.plan, i, shared_t0, self._shared), daemon=True
            )
            for i in range(self.plan.workers)
        ]
        for proc in self._procs:
            proc.start()
        while self._shared.ready.value < self.plan.workers:
            if time.monotonic() > ready_deadline:
                self.stop()
                raise TimeoutError("load generator workers did not start")
            time.sleep(0.05)
        self.t0 = time.monotonic() + 1.0
        self.started_at = time.time() + 1.0
        with shared_t0.get_lock():
            shared_t0.value = self.t0

    @property
    def sent(self) -> int:
        return int(sum(self._shared.sent[:]))

    @property
    def delivered(self) -> int:
        return int(self._shared.delivered.value)

    @property
    def failed(self) -> int:
        return int(self._shared.failed.value)

    def sent_in(self, step: int) -> int:
        return int(self._shared.sent[step])

    def running(self) -> bool:
        return any(p.is_alive() for p in self._procs)

    def stop(self) -> None:
        self._shared.stop.set()

    def join(self, timeout: float | None = None) -> LoadResult:
        for proc in self._procs:
            proc.join(timeout)
        return LoadResult(
            sent_per_step=tuple(int(n) for n in self._shared.sent[:]),
            delivered=self.delivered,
            failed=self.failed,
            started_at=self.started_at,
            cpu_s=round(float(self._shared.cpu_s.value), 2),
        )
