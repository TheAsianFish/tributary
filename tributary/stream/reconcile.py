"""``tributary stream reconcile``: the topic judged against ``raw.recentchange``.

The stream job counts the rows its watermark drops as late, but it counts them
before the dedup can tell whether a late row is a copy of an event that already
landed. A redelivered batch, a producer retry or a second replay of the sample
all look like loss to that counter, and a genuinely late event looks the same
and is never written. A pass settles both by reading the topic itself:

1. **How far to judge.** The warehouse query's committed end offsets, the
   largest per partition in ``ops.stream_progress``. A batch's progress report
   is posted only after its sink has written every partition, so every record
   below those offsets has either reached PostgreSQL or been dropped for good;
   a record above them may still be in flight and is never judged.
2. **Read and classify.** A Spark batch read of ``[start, end)`` per partition,
   through the stream job's own parser (:func:`~tributary.stream.parse.classify_events`,
   the rules and skew bound of :func:`~tributary.stream.parse.parse_events`), so a
   record is judged exactly as the job judged it: rejected as ``malformed`` or
   ``future``, skipped as a ``canary``, or a valid event.
3. **Anti-join.** Each Spark partition copies its valid ids into a temporary
   table and asks PostgreSQL which are absent from ``raw.recentchange``: an
   absent one is **missing**, the others **landed**.
4. **Write back.** The missing rows go through the sink's own idempotent write
   (:func:`~tributary.stream.sink.write_rows`: ``COPY`` into a staging table,
   ``INSERT ... ON CONFLICT DO NOTHING``), so a pass racing the stream, or a
   second pass, can never duplicate a row. ``--dry-run`` only reports.
5. **Record.** One row in ``ops.reconcile_runs`` (the offset window, counts by
   class, rows recovered, seconds) and one per missing event in
   ``ops.reconcile_missing``, which the ``stream_loss`` check reads: a dropped
   row is a loss only if its event is missing. A pass that fails records nothing.

``start`` is ``earliest`` (everything the topic still holds) or ``uncovered``
(from the first batch with dropped rows no pass has covered yet, what the
``raw_recentchange`` DAG runs). Either way a pass can only judge what Kafka
retains: once retention has deleted a batch's offsets, no pass can cover its
drops and the check stays red, which is the honest answer.

The lake query is not repaired: a batch append beside the streaming lake query
would be a second writer on the Iceberg table. The per-hour PostgreSQL-against-
Iceberg reconciliation reports what the lake still lacks.
"""

from __future__ import annotations

import itertools
import time
from collections.abc import Callable, Iterable, Iterator, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Literal, NamedTuple, Protocol

import psycopg
import structlog
from psycopg import sql
from psycopg.types.json import Jsonb

from tributary.events import COLUMNS
from tributary.stream.sink import (
    Offsets,
    _connection,
    _discard,
    adapt_row,
    ensure_schema,
    write_rows,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from tributary.config import Settings

log = structlog.get_logger("tributary.stream.reconcile")

QUERY = "warehouse"
CHUNK = 10_000
"""Valid records anti-joined per round trip, so a partition is never held whole."""
Start = Literal["earliest", "uncovered"]

COMMITTED_SQL = (
    "SELECT t.key, p.key, max(p.value::bigint) FROM ops.stream_progress s"
    " CROSS JOIN LATERAL jsonb_each(s.end_offsets) AS t"
    " CROSS JOIN LATERAL jsonb_each_text(t.value) AS p"
    " WHERE s.query = %s AND s.end_offsets IS NOT NULL GROUP BY 1, 2"
)
IDS = sql.Identifier("reconcile_ids")
CREATE_IDS = sql.SQL("CREATE TEMP TABLE {ids} (event_id uuid) ON COMMIT DROP").format(ids=IDS)
COPY_IDS = sql.SQL("COPY {ids} (event_id) FROM STDIN").format(ids=IDS)
ABSENT_SQL = sql.SQL(
    "SELECT DISTINCT i.event_id::text FROM {ids} AS i WHERE NOT EXISTS"
    " (SELECT 1 FROM raw.recentchange AS r WHERE r.event_id = i.event_id)"
).format(ids=IDS)
INSERT_RUN = (
    "INSERT INTO ops.reconcile_runs (run_id, query, topic, start_offsets, end_offsets, records,"
    " landed, malformed, future, canary, missing, recovered, left_missing, dry_run, seconds,"
    " started_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    " RETURNING id"
)
COPY_MISSING = (
    "COPY ops.reconcile_missing (pass_id, event_id, event_ts, kafka_partition, kafka_offset,"
    " left_missing) FROM STDIN"
)

_EVENT_ID = COLUMNS.index("event_id")
_EVENT_TS = COLUMNS.index("event_ts")
_PARTITION = COLUMNS.index("kafka_partition")
_OFFSET = COLUMNS.index("kafka_offset")


class MissingEvent(NamedTuple):
    """A valid event below the committed offsets whose id is not in the table."""

    event_id: str
    event_ts: datetime
    kafka_partition: int
    kafka_offset: int


@dataclass(frozen=True, slots=True)
class Verdict:
    """What one Spark partition of the topic held, by class."""

    records: int = 0
    landed: int = 0
    malformed: int = 0
    future: int = 0
    canary: int = 0
    missing_records: int = 0
    missing: tuple[MissingEvent, ...] = ()
    recovered: int = 0


@dataclass(frozen=True, slots=True)
class PassResult:
    """One pass: the window it read and every record in it, classed."""

    topic: str
    start_offsets: Offsets
    end_offsets: Offsets
    records: int
    landed: int
    malformed: int
    future: int
    canary: int
    missing_records: int
    missing: tuple[MissingEvent, ...]
    recovered: int
    left_missing: frozenset[str]
    dry_run: bool
    seconds: float
    started_at: datetime
    pass_id: int | None = None
    timings: dict[str, float] = field(default_factory=dict)

    def summary(self) -> dict[str, object]:
        """The pass as plain values (the CLI's output line and the benchmark's files)."""
        return {
            "pass_id": self.pass_id,
            "topic": self.topic,
            "start_offsets": self.start_offsets,
            "end_offsets": self.end_offsets,
            "records": self.records,
            "landed": self.landed,
            "malformed": self.malformed,
            "future": self.future,
            "canary": self.canary,
            "missing": len(self.missing),
            "missing_records": self.missing_records,
            "recovered": self.recovered,
            "left_missing": len(self.left_missing),
            "dry_run": self.dry_run,
            "seconds": round(self.seconds, 3),
            "timings": {k: round(v, 3) for k, v in self.timings.items()},
            "started_at": self.started_at.isoformat(),
        }


class Store(Protocol):
    """Where a pass looks events up and writes the missing ones (picklable: it runs
    on Spark's Python workers)."""

    def absent(self, ids: Sequence[str]) -> set[str]: ...

    def write(self, rows: Sequence[Sequence[object]]) -> int: ...


class PostgresStore:
    """``raw.recentchange``, through the sink's cached per-process connection."""

    def __init__(self, conninfo: str) -> None:
        self.conninfo = conninfo

    def absent(self, ids: Sequence[str]) -> set[str]:
        """The ids in ``ids`` that are not in the table (one transaction, a temp table)."""
        if not ids:
            return set()
        conn = _connection(self.conninfo)
        with conn.transaction(), conn.cursor() as cur:
            cur.execute(CREATE_IDS)
            with cur.copy(COPY_IDS) as copy:
                for event_id in ids:
                    copy.write_row((event_id,))
            cur.execute(ABSENT_SQL)
            return {str(r[0]) for r in cur.fetchall()}

    def write(self, rows: Sequence[Sequence[object]]) -> int:
        """Insert the rows idempotently (the sink's write); the rows actually inserted.
        Retried once on a fresh connection, as the sink retries: the write is idempotent."""
        try:
            return write_rows(_connection(self.conninfo), rows).inserted
        except psycopg.OperationalError:
            _discard(self.conninfo)
            return write_rows(_connection(self.conninfo), rows).inserted


def judge_partition(
    store: Store, *, repair: bool
) -> Callable[[Iterable[Sequence[object]]], Iterator[Verdict]]:
    """The ``mapPartitions`` function of a pass: classify, anti-join, write back.

    Rows are :func:`~tributary.stream.parse.classify_events` rows (the columns
    of ``raw.recentchange``, then the reject reason). Valid ids are looked up
    :data:`CHUNK` at a time; the missing rows (first copy of each id) are
    written once the whole partition is judged, so a later copy of a missing
    event in the same pass is counted as missing too, not as landed.
    Yields exactly one :class:`Verdict`.
    """

    def judge(rows: Iterable[Sequence[object]]) -> Iterator[Verdict]:
        counts = {"records": 0, "landed": 0, "malformed": 0, "future": 0, "canary": 0}
        missing_records = 0
        missing: dict[str, tuple[object, ...]] = {}
        it = iter(rows)
        while chunk := list(itertools.islice(it, CHUNK)):
            valid: list[tuple[object, ...]] = []
            for row in chunk:
                counts["records"] += 1
                reason = row[-1]
                if reason is not None:
                    counts[str(reason)] += 1
                    continue
                valid.append(adapt_row(row[:-1]))
            absent = store.absent(sorted({str(r[_EVENT_ID]) for r in valid}))
            for r in valid:
                event_id = str(r[_EVENT_ID])
                if event_id in absent:
                    missing_records += 1
                    missing.setdefault(event_id, r)
                else:
                    counts["landed"] += 1
        recovered = store.write(list(missing.values())) if repair and missing else 0
        yield Verdict(
            **counts,
            missing_records=missing_records,
            missing=tuple(
                MissingEvent(
                    event_id,
                    _timestamp(r[_EVENT_TS]),
                    int(str(r[_PARTITION])),
                    int(str(r[_OFFSET])),
                )
                for event_id, r in missing.items()
            ),
            recovered=recovered,
        )

    return judge


def _timestamp(value: object) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"event_ts is not a timestamp: {value!r}")
    return value


def merge(verdicts: Iterable[Verdict]) -> Verdict:
    """The verdicts of every partition as one (ids are keyed to one Kafka partition,
    but a missing id seen twice is still counted once)."""
    total = Verdict()
    seen: dict[str, MissingEvent] = {}
    for v in verdicts:
        for m in v.missing:
            seen.setdefault(m.event_id, m)
        total = Verdict(
            records=total.records + v.records,
            landed=total.landed + v.landed,
            malformed=total.malformed + v.malformed,
            future=total.future + v.future,
            canary=total.canary + v.canary,
            missing_records=total.missing_records + v.missing_records,
            recovered=total.recovered + v.recovered,
        )
    return Verdict(
        records=total.records,
        landed=total.landed,
        malformed=total.malformed,
        future=total.future,
        canary=total.canary,
        missing_records=total.missing_records,
        missing=tuple(sorted(seen.values(), key=lambda m: (m.kafka_partition, m.kafka_offset))),
        recovered=total.recovered,
    )


def judge_records(
    records: DataFrame, max_future_skew_s: int, store: Store, *, repair: bool
) -> Verdict:
    """Classify Kafka-shaped records (``value``, ``partition``, ``offset``, ``timestamp``)
    and judge them against ``store``: the Spark half of a pass, without Kafka."""
    from tributary.stream.parse import classify_events

    classified = classify_events(records, max_future_skew_s)
    verdicts: list[Verdict] = classified.rdd.mapPartitions(
        judge_partition(store, repair=repair)
    ).collect()
    return merge(verdicts)


# ------------------------------------------------------------------- offsets


def committed_offsets(conninfo: str, topic: str, query: str = QUERY) -> dict[str, int]:
    """The largest end offset per partition of the query's recorded batches (exclusive)."""
    ensure_schema(conninfo, "ops.sql")
    with psycopg.connect(conninfo, autocommit=True) as conn:
        rows = conn.execute(COMMITTED_SQL, (query,)).fetchall()
    return {str(p): int(o) for t, p, o in rows if t == topic}


def earliest_offsets(bootstrap: str, topic: str, partitions: Iterable[str]) -> dict[str, int]:
    """The oldest offset the broker still holds per partition (retention moves it)."""
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": "tributary-reconcile",
            "enable.auto.commit": False,
        }
    )
    try:
        out = {}
        for p in partitions:
            low, _high = consumer.get_watermark_offsets(TopicPartition(topic, int(p)), timeout=10)
            out[p] = int(low)
        return out
    finally:
        consumer.close()


def pass_window(
    committed: dict[str, int],
    earliest: dict[str, int],
    start: Start,
    uncovered: Sequence[Offsets | None] = (),
    topic: str = "",
) -> dict[str, int]:
    """Where a pass starts per partition (its end is ``committed``).

    ``earliest``: everything retained. ``uncovered``: from the first offset of
    the uncovered batches (``uncovered`` holds their start offsets; None is a
    batch with no start, taken as offset 0), or nothing in a partition none of
    them read. Never before what the broker holds, never past the end.
    """
    out = {}
    for p, end in committed.items():
        low = earliest.get(p, 0)
        if start == "earliest":
            begin = low
        else:
            starts = [0 if s is None else s.get(topic, {}).get(p, 0) for s in uncovered]
            begin = max(low, min(starts)) if starts else end
        out[p] = min(begin, end)
    return out


# ---------------------------------------------------------------- the pass


def build_session(settings: Settings) -> SparkSession:
    """A batch Spark session for passes: the stream job's time zone, no UI, no lake."""
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("tributary-reconcile")
        .master(settings.spark_master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(settings.topic_partitions))
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
    )
    if settings.spark_packages:
        builder = builder.config("spark.jars.packages", settings.spark_packages)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_topic(
    spark: SparkSession, settings: Settings, start: dict[str, int], end: dict[str, int]
) -> DataFrame:
    """The records of ``[start, end)`` per partition, as a batch (``failOnDataLoss``:
    offsets retention has deleted under the window fail the pass rather than skip)."""
    import json

    return (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
        .option("subscribe", settings.topic)
        .option("startingOffsets", json.dumps({settings.topic: start}))
        .option("endingOffsets", json.dumps({settings.topic: end}))
        .option("failOnDataLoss", "true")
        .load()
    )


class NothingToJudgeError(RuntimeError):
    """The warehouse query has recorded no batch offsets yet."""


def run_pass(
    spark: SparkSession,
    settings: Settings,
    run_id: str,
    *,
    start: Start = "earliest",
    dry_run: bool = False,
    store: Store | None = None,
    now: datetime | None = None,
) -> PassResult | None:
    """One pass (see the module docstring); None when ``start='uncovered'`` finds no
    uncovered batch. Recorded in ``ops.reconcile_runs`` unless it raises."""
    from tributary.warehouse.stream_health import uncovered_batches

    started_at = now or datetime.now(UTC)
    t0 = time.perf_counter()
    store = store or PostgresStore(settings.pg_conninfo)
    committed = committed_offsets(settings.pg_conninfo, settings.topic)
    if not committed:
        raise NothingToJudgeError(
            "no committed warehouse batch with offsets in ops.stream_progress"
        )
    batches = []
    if start == "uncovered":
        window = timedelta(hours=settings.stream_check_window_hours)
        batches = uncovered_batches(settings.pg_conninfo, started_at - window, started_at)
        if not batches:
            log.info("reconcile.nothing_uncovered", run_id=run_id)
            return None
    earliest = earliest_offsets(settings.kafka_bootstrap, settings.topic, committed)
    begin = pass_window(
        committed, earliest, start, [b.start_offsets for b in batches], settings.topic
    )
    t_plan = time.perf_counter()
    records = read_topic(spark, settings, begin, committed)
    verdict = judge_records(records, settings.future_skew_s, store, repair=not dry_run)
    t_judge = time.perf_counter()
    left = store.absent([m.event_id for m in verdict.missing])
    result = PassResult(
        topic=settings.topic,
        start_offsets={settings.topic: begin},
        end_offsets={settings.topic: committed},
        records=verdict.records,
        landed=verdict.landed,
        malformed=verdict.malformed,
        future=verdict.future,
        canary=verdict.canary,
        missing_records=verdict.missing_records,
        missing=verdict.missing,
        recovered=verdict.recovered,
        left_missing=frozenset(left),
        dry_run=dry_run,
        seconds=time.perf_counter() - t0,
        started_at=started_at,
        timings={"plan": t_plan - t0, "judge": t_judge - t_plan},
    )
    pass_id = record_pass(settings.pg_conninfo, run_id, result)
    done = replace(result, pass_id=pass_id)
    log.info("reconcile.pass", run_id=run_id, **done.summary())
    return done


def record_pass(conninfo: str, run_id: str, result: PassResult) -> int:
    """Write the pass and its missing events in one transaction; the pass's id."""
    ensure_schema(conninfo, "ops.sql")
    with psycopg.connect(conninfo) as conn, conn.transaction(), conn.cursor() as cur:
        cur.execute(
            INSERT_RUN,
            (
                run_id,
                QUERY,
                result.topic,
                Jsonb(result.start_offsets),
                Jsonb(result.end_offsets),
                result.records,
                result.landed,
                result.malformed,
                result.future,
                result.canary,
                len(result.missing),
                result.recovered,
                len(result.left_missing),
                result.dry_run,
                result.seconds,
                result.started_at,
            ),
        )
        row = cur.fetchone()
        if row is None:
            raise RuntimeError("INSERT ... RETURNING returned no row")
        pass_id = int(row[0])
        if result.missing:
            with cur.copy(COPY_MISSING) as copy:
                for m in result.missing:
                    copy.write_row(
                        (
                            pass_id,
                            m.event_id,
                            m.event_ts,
                            m.kafka_partition,
                            m.kafka_offset,
                            m.event_id in result.left_missing,
                        )
                    )
    return pass_id


class MemoryStore:
    """A store of landed ids held in memory: tests, and a pass judged without a database.
    Each Spark worker gets its own copy, so a write is counted but never seen again."""

    def __init__(self, landed: Iterable[str]) -> None:
        self.landed = frozenset(landed)

    def absent(self, ids: Sequence[str]) -> set[str]:
        return {i for i in ids if i not in self.landed}

    def write(self, rows: Sequence[Sequence[object]]) -> int:
        return len({str(r[_EVENT_ID]) for r in rows} - self.landed)
