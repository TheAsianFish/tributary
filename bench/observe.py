"""What the benchmark watches while a run is in progress.

- :class:`LogTail` follows the stream container's JSON log: every
  ``StreamingQueryProgress`` (``stream.progress``) and every sink write
  (``sink.batch``), each kept with its raw line for ``bench/results/``.
- :class:`Sampler` records, every ``interval`` seconds, the broker's end
  offsets, the offsets the job has finished (from the newest progress
  line), the lag between them, and the CPU time each container has used
  (cgroup v2 ``cpu.stat``, exact to the microsecond, unlike ``docker stats``).
- :class:`PgActivity` samples ``pg_stat_activity`` once a second: what the
  warehouse's client backends are doing or waiting on.

Lag is measured, not read from Kafka: Spark's Kafka source keeps its offsets in
its checkpoint and never commits them to a consumer group, so there is no
consumer-group lag to ask the broker for.
"""

from __future__ import annotations

import contextlib
import json
import subprocess
import threading
import time
from collections import Counter
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, Any

import psycopg

CGROUP_ROOT = Path("/sys/fs/cgroup/system.slice")


def progress_offsets(progress: Mapping[str, Any]) -> int | None:
    """Total of the end offsets in one ``stream.progress`` line (None before the first batch)."""
    total = 0
    found = False
    for source in progress.get("sources") or []:
        end = source.get("end")
        if not isinstance(end, Mapping):
            continue
        for partitions in end.values():
            if isinstance(partitions, Mapping):
                total += sum(int(v) for v in partitions.values())
                found = True
    return total if found else None


class LogTail:
    """Follow ``docker logs -f`` of one container from ``since`` (a ``time.time()``).

    Parsed lines are kept in memory by event name; every line (JSON or not)
    is also written to ``raw`` if given. ``docker logs -f`` ends when the
    container stops; the tail then stops too (fault runs read the log after
    the fact instead, since a restarted container keeps its log).
    """

    def __init__(self, container: str, since: float, raw: Path | None = None) -> None:
        self.container = container
        self.since = since
        self.raw = raw
        self.events: dict[str, list[dict[str, Any]]] = {}
        self._lock = threading.Lock()
        self._proc: subprocess.Popen[str] | None = None
        self._thread: threading.Thread | None = None
        self.latest: dict[str, dict[str, Any]] = {}
        """The newest ``stream.progress`` per query name."""

    def start(self) -> None:
        self._proc = subprocess.Popen(
            ["docker", "logs", "-f", "--since", f"{self.since:.3f}", self.container],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        self._thread = threading.Thread(target=self._read, daemon=True)
        self._thread.start()

    def _read(self) -> None:
        assert self._proc is not None
        assert self._proc.stdout is not None
        sink: IO[str] | None = self.raw.open("a") if self.raw is not None else None
        try:
            for line in self._proc.stdout:
                if sink is not None:
                    sink.write(line)
                self.feed(line, received=time.time())
        finally:
            if sink is not None:
                sink.close()

    def feed(self, line: str, received: float) -> None:
        """Parse one log line (public so tests and after-the-fact reads share it)."""
        record = parse_log_line(line)
        if record is None:
            return
        record["_received"] = received
        name = str(record.get("event"))
        with self._lock:
            self.events.setdefault(name, []).append(record)
            if name == "stream.progress":
                self.latest[str(record.get("query"))] = record

    def get(self, event: str) -> list[dict[str, Any]]:
        with self._lock:
            return list(self.events.get(event, []))

    def newest(self, query: str) -> dict[str, Any] | None:
        with self._lock:
            return self.latest.get(query)

    def stop(self) -> None:
        if self._proc is not None and self._proc.poll() is None:
            self._proc.terminate()
            with contextlib.suppress(subprocess.TimeoutExpired):
                self._proc.wait(5)
        if self._thread is not None:
            self._thread.join(5)


def parse_log_line(line: str) -> dict[str, Any] | None:
    """A structlog JSON line as a dict; None for Spark's own plain-text lines."""
    line = line.strip()
    if not line.startswith("{"):
        return None
    try:
        record = json.loads(line)
    except ValueError:
        return None
    return record if isinstance(record, dict) and "event" in record else None


def read_log(container: str, since: float, until: float | None = None) -> list[dict[str, Any]]:
    """Every structured line a container logged in ``[since, until]`` (across restarts)."""
    argv = ["docker", "logs", "--since", f"{since:.3f}"]
    if until is not None:
        argv += ["--until", f"{until:.3f}"]
    out = subprocess.run([*argv, container], capture_output=True, text=True, check=True)
    records = []
    for line in (out.stdout + out.stderr).splitlines():
        record = parse_log_line(line)
        if record is not None:
            records.append(record)
    return records


def cpu_usec(container_id: str, root: Path = CGROUP_ROOT) -> int | None:
    """Microseconds of CPU the container's cgroup has used (None if not readable)."""
    try:
        text = (root / f"docker-{container_id}.scope" / "cpu.stat").read_text()
    except OSError:
        return None
    for line in text.splitlines():
        key, _, value = line.partition(" ")
        if key == "usage_usec":
            return int(value)
    return None


def host_cpu_busy() -> tuple[float, float]:
    """(busy, total) jiffies of the whole machine from ``/proc/stat``."""
    fields = [float(x) for x in Path("/proc/stat").read_text().splitlines()[0].split()[1:]]
    idle = fields[3] + fields[4]  # idle + iowait
    total = sum(fields[:8])
    return total - idle, total


@dataclass
class Sample:
    """One sampler tick."""

    t: float
    """``time.time()``."""
    broker_end: int | None
    processed: dict[str, int | None]
    """Finished end offsets per query, from the newest progress line."""
    cpu_usec: dict[str, int | None]
    host_busy: float
    host_total: float

    def lag(self, query: str) -> int | None:
        done = self.processed.get(query)
        if self.broker_end is None or done is None:
            return None
        return self.broker_end - done


def broker_end_offsets(consumer: Any, topic: str, partitions: int) -> int | None:
    """Sum of the high watermarks of ``topic``'s partitions (None if the broker is away)."""
    # Any: confluent_kafka.Consumer; imported lazily by the caller.
    from confluent_kafka import KafkaException, TopicPartition

    total = 0
    try:
        for p in range(partitions):
            _low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=2.0)
            total += int(high)
    except KafkaException:
        return None
    return total


@dataclass
class Sampler:
    """Sample lag and CPU every ``interval`` seconds in a background thread."""

    bootstrap: str
    topic: str
    partitions: int
    tail: LogTail | None
    containers: Mapping[str, str]
    """Service name to container id."""
    queries: Sequence[str] = ("warehouse",)
    interval: float = 2.0
    samples: list[Sample] = field(default_factory=list)

    def __post_init__(self) -> None:
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        from confluent_kafka import Consumer

        consumer = Consumer(
            {"bootstrap.servers": self.bootstrap, "group.id": "tributary-bench-sampler"}
        )
        try:
            while not self._stop.is_set():
                self.samples.append(self.sample(consumer))
                self._stop.wait(self.interval)
        finally:
            consumer.close()

    def sample(self, consumer: Any) -> Sample:
        processed: dict[str, int | None] = {}
        for q in self.queries:
            newest = self.tail.newest(q) if self.tail is not None else None
            processed[q] = progress_offsets(newest) if newest is not None else None
        busy, total = host_cpu_busy()
        return Sample(
            t=time.time(),
            broker_end=broker_end_offsets(consumer, self.topic, self.partitions),
            processed=processed,
            cpu_usec={name: cpu_usec(cid) for name, cid in self.containers.items()},
            host_busy=busy,
            host_total=total,
        )

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(10)

    def between(self, start: float, end: float) -> list[Sample]:
        return [s for s in self.samples if start <= s.t <= end]


def cpu_cores(samples: Sequence[Sample]) -> dict[str, float]:
    """Average cores each container used between the first and last sample."""
    if len(samples) < 2:
        return {}
    first, last = samples[0], samples[-1]
    seconds = last.t - first.t
    out: dict[str, float] = {}
    for name, end in last.cpu_usec.items():
        begin = first.cpu_usec.get(name)
        if end is not None and begin is not None and seconds > 0:
            out[name] = round((end - begin) / 1e6 / seconds, 3)
    return out


def host_cores(samples: Sequence[Sample], ncpu: int) -> float | None:
    """Average busy cores of the whole machine between the first and last sample."""
    if len(samples) < 2:
        return None
    first, last = samples[0], samples[-1]
    total = last.host_total - first.host_total
    if total <= 0:
        return None
    return round((last.host_busy - first.host_busy) / total * ncpu, 3)


ACTIVITY_SQL = """
SELECT coalesce(wait_event_type, 'CPU') AS wait_type,
       coalesce(wait_event, state) AS wait_event,
       count(*)
FROM pg_stat_activity
WHERE backend_type = 'client backend' AND pid <> pg_backend_pid() AND datname = current_database()
  AND state <> 'idle'
GROUP BY 1, 2
"""
"""Active client backends by what they wait on; no wait event means running on CPU.

Idle backends (a sink connection between batches) are left out, and counted
separately, so the tally is of time spent inside statements.
"""


@dataclass
class PgActivity:
    """Count active client backends by wait event, once a second."""

    conninfo: str
    interval: float = 1.0
    counts: Counter[str] = field(default_factory=Counter)
    ticks: int = 0
    idle: int = 0

    def __post_init__(self) -> None:
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        with psycopg.connect(self.conninfo, autocommit=True) as conn:
            while not self._stop.is_set():
                rows = conn.execute(ACTIVITY_SQL).fetchall()
                idle_row = conn.execute(
                    "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'client backend'"
                    " AND pid <> pg_backend_pid() AND datname = current_database()"
                    " AND state = 'idle'"
                ).fetchone()
                with self._lock:
                    self.ticks += 1
                    self.idle += int(str(idle_row[0])) if idle_row else 0
                    for wait_type, wait_event, n in rows:
                        self.counts[f"{wait_type}:{wait_event}"] += int(n)
                self._stop.wait(self.interval)

    def snapshot(self) -> dict[str, Any]:
        """Backend-seconds per wait event, the tick count, and idle backend-seconds."""
        with self._lock:
            return {
                "ticks": self.ticks,
                "idle_backend_s": self.idle,
                "active_backend_s": dict(self.counts.most_common()),
            }

    def reset(self) -> None:
        with self._lock:
            self.counts.clear()
            self.ticks = 0
            self.idle = 0

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(5)


@contextlib.contextmanager
def running(*observers: Any) -> Iterator[None]:
    """Start the observers, yield, and stop them in reverse order."""
    # Any: anything with start() and stop().
    started: list[Any] = []
    try:
        for o in observers:
            o.start()
            started.append(o)
        yield
    finally:
        for o in reversed(started):
            o.stop()
