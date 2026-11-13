"""``make bench-skew``: what one future-stamped event costs the events after it.

Spark local mode on the committed sample; no Compose, no Kafka. The 50,000
events are fed in stream (file) order through the stream job's own parser and
dedup operator (:func:`tributary.stream.parse.parse_events`,
:func:`~tributary.stream.parse.deduplicate`, 10-minute watermark) in fixed
micro-batches of :data:`BATCH_EVENTS`, one JSON file per trigger. Each record's
Kafka timestamp is its ``meta.dt``, the emit time, which is what the skew bound
would compare the event time with live (a replay stamped with the wall clock
would put every sample event days behind its record time, and no bound could
trip).

One poison, a copy of the real event at stream position :data:`POISON_AT`
with its own id, is inserted in front of it with ``timestamp`` set
:data:`SKEWS` seconds ahead of its own ``meta.dt``. Two arms: ``before`` is the
parser with the bound it had (:data:`OLD_BOUND_S`, one day), ``after`` with
the bound in force now (``Settings().future_skew_s``, half the delay).

``predict`` writes the expected loss per skew and arm from the sample alone,
before any Spark run, by replaying Spark's rule: the watermark after a batch is
the newest accepted event time so far minus the delay, and batch ``k`` drops
an event as late when its time is at or below the watermark computed after
batch ``k - 2`` (the late filter reads the previous batch's watermark; the
unit test ``test_deduplicate_semantics_across_micro_batches`` pins this).
``run`` measures: rows dropped by the watermark per batch, which real events
never reached the sink, whether the poison and the rest of its batch did, and
the records rejected by reason. ``incident`` reproduces the M5 incident: a
fault run after ``make test-integration`` on a stack that had already run the
benchmark.

Raw output under ``bench/results/m11-skew/``.
"""

from __future__ import annotations

import argparse
import copy
import json
import os
import platform
import shutil
import sys
import tempfile
import time
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

from tributary.config import DEFAULT_SAMPLE, Settings
from tributary.events import event_time
from tributary.ingest.sample import read_sample

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

RESULTS = Path(__file__).resolve().parent / "results" / "m11-skew"

DELAY_S = 600
"""The stream job's default watermark delay (``TRIBUTARY_WATERMARK_DELAY``)."""
BATCH_EVENTS = 500
"""Real events per micro-batch: 100 batches, about 22 s of the sample's stream each."""
POISON_AT = 10_250
"""Stream index of the real event the poison copies and precedes: the middle of batch 20."""
POISON_ID = "ffffffff-ffff-4fff-8fff-ffffffffffff"
SKEWS = (0, 60, 300, 450, 600, 900, 3_600, 82_800)
OLD_BOUND_S = 86_400
"""The bound before the fix: one day, a constant in ``tributary.events``."""
ARMS = ("before", "after")


@dataclass(frozen=True, slots=True)
class Record:
    """One Kafka record of the replay."""

    event_id: str
    event_ts: int
    """MediaWiki ``timestamp``: event time, whole seconds."""
    kafka_ms: int
    """The record's Kafka timestamp in ms: the event's ``meta.dt``."""
    payload: str
    poison: bool = False

    @property
    def kafka_s(self) -> int:
        """Whole seconds, as the parser compares them (``unix_timestamp`` truncates)."""
        return self.kafka_ms // 1000


def _ms(dt: str) -> int:
    return round(datetime.fromisoformat(dt).timestamp() * 1000)


def sample_records(path: Path = DEFAULT_SAMPLE) -> list[Record]:
    """The sample in stream order, each with its ``meta.dt`` as the record time."""
    out = []
    for e in read_sample(path):
        payload = json.dumps(e, ensure_ascii=False, separators=(",", ":"))
        out.append(Record(e["meta"]["id"], int(e["timestamp"]), _ms(e["meta"]["dt"]), payload))
    return out


def make_poison(model: Record, skew_s: int) -> Record:
    """A copy of ``model`` with its own id, stamped ``skew_s`` ahead of its ``meta.dt``."""
    event: dict[str, Any] = json.loads(model.payload)
    event = copy.deepcopy(event)
    event["meta"]["id"] = POISON_ID
    ts = model.kafka_s + skew_s
    event["timestamp"] = ts
    payload = json.dumps(event, ensure_ascii=False, separators=(",", ":"))
    return Record(POISON_ID, ts, model.kafka_ms, payload, poison=True)


def poisoned_batches(
    records: Sequence[Record], skew_s: int | None, *, at: int = POISON_AT, size: int = BATCH_EVENTS
) -> list[list[Record]]:
    """Fixed micro-batches of ``size`` real events; the poison (if any) joins its model's batch."""
    batches = [list(records[i : i + size]) for i in range(0, len(records), size)]
    if skew_s is not None:
        b, i = divmod(at, size)
        batches[b].insert(i, make_poison(records[at], skew_s))
    return batches


def accepted(record: Record, bound_s: int) -> bool:
    """The parser's rule: ``timestamp <= unix_timestamp(kafka_ts) + bound``."""
    return record.event_ts <= record.kafka_s + bound_s


# ---------------------------------------------------------------- prediction


@dataclass
class Predicted:
    """The loss Spark should report for one skew and arm, computed before the run."""

    skew_s: int
    bound_s: int
    poison_accepted: bool
    real_rejected: int
    lost: int
    """Real events at or below their batch's late threshold (Spark's ``<=``)."""
    lost_strict: int
    """The same with ``<``: differs only by events exactly on the threshold."""
    plan_formula: int
    """PLAN.md's first estimate: real events in batches after the poison's, stamped
    below ``t_poison + X - delay`` (``t_poison`` its ``meta.dt``)."""
    lost_by_batch: dict[int, int] = field(default_factory=dict)
    lost_ids: list[str] = field(default_factory=list)


def predict(batches: Sequence[Sequence[Record]], bound_s: int, delay_s: int = DELAY_S) -> Predicted:
    """Replay Spark's watermark rule over ``batches`` (see the module docstring)."""
    newest: list[int | None] = []  # newest accepted event time through batch k
    running: int | None = None
    lost = lost_strict = 0
    by_batch: dict[int, int] = {}
    ids: list[str] = []
    poison: Record | None = None
    poison_batch = -1
    real_rejected = 0
    for k, batch in enumerate(batches):
        threshold = None
        if k >= 2 and newest[k - 2] is not None:
            threshold = int(newest[k - 2] or 0) - delay_s
        for r in batch:
            if r.poison:
                poison, poison_batch = r, k
            if not accepted(r, bound_s):
                real_rejected += not r.poison
                continue
            running = r.event_ts if running is None else max(running, r.event_ts)
            if r.poison or threshold is None:
                continue
            if r.event_ts <= threshold:
                lost += 1
                by_batch[k] = by_batch.get(k, 0) + 1
                ids.append(r.event_id)
            if r.event_ts < threshold:
                lost_strict += 1
        newest.append(running)
    plan = 0
    if poison is not None:
        line = poison.kafka_ms / 1000 + (poison.event_ts - poison.kafka_s) - delay_s
        plan = sum(1 for batch in batches[poison_batch + 1 :] for r in batch if r.event_ts < line)
    return Predicted(
        skew_s=-1 if poison is None else poison.event_ts - poison.kafka_s,
        bound_s=bound_s,
        poison_accepted=poison is not None and accepted(poison, bound_s),
        real_rejected=real_rejected,
        lost=lost,
        lost_strict=lost_strict,
        plan_formula=plan if poison is not None and accepted(poison, bound_s) else 0,
        lost_by_batch=by_batch,
        lost_ids=sorted(ids),
    )


def first_lossy_skew(
    records: Sequence[Record],
    bound_s: int,
    limit: int = 1_200,
    *,
    at: int = POISON_AT,
    size: int = BATCH_EVENTS,
) -> int | None:
    """The smallest whole-second skew whose poison costs at least one real event."""
    for x in range(limit + 1):
        if predict(poisoned_batches(records, x, at=at, size=size), bound_s).lost:
            return x
    return None


def bound_after() -> int:
    return Settings(_env_file=None).future_skew_s


def arm_bound(arm: str) -> int:
    return OLD_BOUND_S if arm == "before" else bound_after()


def prediction_doc(records: Sequence[Record]) -> dict[str, Any]:
    """Everything ``predict`` says, per arm and skew, plus the sample facts it rests on."""
    late = [0]
    top = records[0].event_ts
    for r in records[1:]:
        top = max(top, r.event_ts)
        late.append(top - r.event_ts)
    arms: dict[str, Any] = {}
    for arm in ARMS:
        bound = arm_bound(arm)
        rows = []
        for x in SKEWS:
            p = predict(poisoned_batches(records, x), bound)
            row = asdict(p)
            row.pop("lost_ids")
            rows.append(row)
        clean = predict(poisoned_batches(records, None), bound)
        arms[arm] = {
            "bound_s": bound,
            "skews": rows,
            "no_poison_lost": clean.lost,
            "first_lossy_skew_s": first_lossy_skew(records, bound),
        }
    return {
        "made_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "rule": (
            "batch k drops a real event when event_ts <= max(accepted event_ts through"
            " batch k-2) - delay; the poison is accepted when timestamp <= floor(meta.dt) + bound"
        ),
        "sample": {
            "events": len(records),
            "max_lateness_s": max(late),
            "max_emit_delay_s": round(max(r.kafka_ms / 1000 - r.event_ts for r in records), 3),
            "max_ahead_of_emit_s": round(max(r.event_ts - r.kafka_ms / 1000 for r in records), 3),
        },
        "batch_events": BATCH_EVENTS,
        "batches": len(poisoned_batches(records, None)),
        "poison_at": POISON_AT,
        "poison_batch": POISON_AT // BATCH_EVENTS,
        "delay_s": DELAY_S,
        "arms": arms,
    }


# ---------------------------------------------------------------- Spark runs


def spark_session() -> SparkSession:
    from pyspark.sql import SparkSession

    from tributary.stream.job import ROCKSDB_PROVIDER

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    session = (
        SparkSession.builder.master("local[2]")
        .appName("tributary-bench-skew")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        # recentProgress keeps the last 100 reports by default; a run has 101 batches.
        .config("spark.sql.streaming.numRecentProgressUpdates", "1000")
        .config("spark.sql.streaming.stateStore.providerClass", ROCKSDB_PROVIDER)
        .config("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    return session


def write_batches(src: Path, batches: Sequence[Sequence[Record]]) -> None:
    """One JSON file per micro-batch, Kafka-shaped rows, modification times in order."""
    src.mkdir(parents=True)
    offset = 0
    for b, batch in enumerate(batches):
        path = src / f"batch-{b:05d}.json"
        with path.open("w", encoding="utf-8") as f:
            for r in batch:
                stamp = datetime.fromtimestamp(r.kafka_ms / 1000, UTC)
                row = {
                    "value": r.payload,
                    "partition": 0,
                    "offset": offset,
                    "timestamp": stamp.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
                }
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
                offset += 1
        os.utime(path, (1_000_000 + b, 1_000_000 + b))


class _Ids:
    """``foreachBatch``: the ids each batch delivered to the sink."""

    def __init__(self) -> None:
        self.by_batch: dict[int, list[str]] = {}

    def __call__(self, df: DataFrame, batch_id: int) -> None:
        self.by_batch[batch_id] = [str(r[0]) for r in df.select("event_id").collect()]


def run_stream(
    spark: SparkSession, batches: Sequence[Sequence[Record]], bound_s: int, work: Path
) -> tuple[dict[int, list[str]], list[Any]]:
    """Feed ``batches`` through parse + dedup, one file per trigger; (ids by batch, progress)."""
    from pyspark.sql import functions as F  # noqa: N812
    from pyspark.sql.types import (
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    from tributary.stream.parse import deduplicate, parse_events

    src = work / "src"
    write_batches(src, batches)
    schema = StructType(
        [
            StructField("value", StringType()),
            StructField("partition", IntegerType()),
            StructField("offset", LongType()),
            StructField("timestamp", TimestampType()),
        ]
    )
    source = (
        spark.readStream.schema(schema)
        .option("maxFilesPerTrigger", 1)
        .json(str(src))
        .withColumn("value", F.col("value").cast("binary"))
    )
    sink = _Ids()
    query = (
        deduplicate(parse_events(source, bound_s), f"{DELAY_S} seconds")
        .writeStream.foreachBatch(sink)
        .option("checkpointLocation", str(work / "checkpoint"))
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    return sink.by_batch, list(query.recentProgress)


def measure(
    spark: SparkSession, records: Sequence[Record], skew_s: int, bound_s: int
) -> dict[str, Any]:
    """One poisoned run: what reached the sink, what was dropped, what was rejected."""
    batches = poisoned_batches(records, skew_s)
    real = {r.event_id for r in records}
    poison_batch = POISON_AT // BATCH_EVENTS
    work = Path(tempfile.mkdtemp(prefix="tributary-skew-"))
    started = time.monotonic()
    try:
        ids_by_batch, progress = run_stream(spark, batches, bound_s, work)
    finally:
        shutil.rmtree(work, ignore_errors=True)
    seconds = time.monotonic() - started
    data = [p for p in progress if p.numInputRows > 0]
    if len(data) != len(batches):
        raise RuntimeError(f"{len(data)} progress reports for {len(batches)} batches")
    delivered = {i for ids in ids_by_batch.values() for i in ids}
    dropped_by_batch = {
        p.batchId: n
        for p in data
        if (n := sum(int(s.numRowsDroppedByWatermark) for s in p.stateOperators))
    }
    observed = [p.observedMetrics["parse"].asDict() for p in data if "parse" in p.observedMetrics]

    def total(name: str) -> int:
        return sum(int(o.get(name) or 0) for o in observed)

    poison_rows = ids_by_batch.get(poison_batch, [])
    model_batch = {r.event_id for r in batches[poison_batch] if not r.poison}
    lost_ids = sorted(real - delivered)
    return {
        "skew_s": skew_s,
        "bound_s": bound_s,
        "data_batches": len(data),
        "input_rows": total("input_rows"),
        "delivered_real": len(real & delivered),
        "lost_real": len(lost_ids),
        "dropped_by_watermark": sum(dropped_by_batch.values()),
        "dropped_by_batch": {str(k): v for k, v in sorted(dropped_by_batch.items())},
        "malformed_rows": total("malformed_rows"),
        "future_rows": total("future_rows"),
        "canary_rows": total("canary_rows"),
        "poison_delivered": POISON_ID in delivered,
        "poison_batch_real_delivered": len(model_batch & set(poison_rows)),
        "poison_batch_real": len(model_batch),
        "lost_ids": lost_ids,
        "seconds": round(seconds, 1),
    }


def run_arm(arm: str, records: Sequence[Record], prediction: Mapping[str, Any]) -> dict[str, Any]:
    """Every skew for one arm, each beside its prediction (made earlier, read from disk)."""
    bound = arm_bound(arm)
    predicted = {row["skew_s"]: row for row in prediction["arms"][arm]["skews"]}
    if prediction["arms"][arm]["bound_s"] != bound:
        raise RuntimeError(f"prediction was made for bound {prediction['arms'][arm]['bound_s']}")
    spark = spark_session()
    rows = []
    try:
        for x in SKEWS:
            m = measure(spark, records, x, bound)
            expected = predict(poisoned_batches(records, x), bound)
            m["predicted_lost"] = predicted[x]["lost"]
            m["predicted_lost_strict"] = predicted[x]["lost_strict"]
            m["plan_formula"] = predicted[x]["plan_formula"]
            m["same_ids_as_predicted"] = m["lost_ids"] == expected.lost_ids
            m["lost_ids_sample"] = m.pop("lost_ids")[:5]
            print(
                f"{arm} skew {x:>6} s: lost {m['lost_real']:>6} (predicted {m['predicted_lost']}),"
                f" dropped {m['dropped_by_watermark']}, future {m['future_rows']},"
                f" {m['seconds']} s",
                flush=True,
            )
            rows.append(m)
    finally:
        spark.stop()
    return {"arm": arm, "bound_s": bound, "delay_s": DELAY_S, "runs": rows}


# ---------------------------------------------------------------- the M5 incident

INCIDENT_START = datetime(2026, 11, 10, 16, 0, tzinfo=UTC)
"""A wall clock for the reproduction (the day of the M5 review); only differences matter."""
BENCH_RATE = 500
"""Events/s of a fault run (``FaultConfig.rate``); the job's 2 s trigger makes 1,000 per batch."""
TRIGGER_S = 2
TEST_EVENTS = 5_000
LAKEHOUSE_SPACING_S = 2


@dataclass
class Phase:
    """One step of the incident: what was sent, when, and with which event times."""

    name: str
    records: list[Record]
    batches: list[list[Record]]
    sent_at: datetime


def _render(event: Mapping[str, Any]) -> str:
    return json.dumps(event, ensure_ascii=False, separators=(",", ":"))


def bench_phase(
    name: str, prefix: int, start: datetime, events: Sequence[Mapping[str, Any]]
) -> Phase:
    """A fault run as ``bench.load`` sends it: event time = send time - the event's own
    lateness in the sample, ``meta.dt`` and the Kafka time = send time, 500 events/s."""
    from bench.load import lateness

    late = lateness([event_time(e).timestamp() for e in events])
    records = []
    for i, (e, lag) in enumerate(zip(events, late, strict=True)):
        sent = start.timestamp() + i / BENCH_RATE
        out = copy.deepcopy(dict(e))
        out["meta"] = {**out["meta"], "id": f"{prefix:08x}-0000-4000-8000-{i:012x}"}
        out["meta"]["dt"] = datetime.fromtimestamp(sent, UTC).isoformat(timespec="milliseconds")
        out["timestamp"] = int(sent) - lag
        records.append(
            Record(out["meta"]["id"], out["timestamp"], round(sent * 1000), _render(out))
        )
    per_batch = BENCH_RATE * TRIGGER_S
    batches = [records[i : i + per_batch] for i in range(0, len(records), per_batch)]
    return Phase(name, records, batches, start)


def replay_phase(name: str, sent_at: datetime, events: Sequence[Mapping[str, Any]]) -> Phase:
    """Events produced at full speed (``produce_events``): one batch, Kafka time = send time.

    Only ``meta.id`` and ``timestamp`` were rewritten by the tests; ``meta.dt``
    stays the sample's, so it is not what the record time is.
    """
    ms = round(sent_at.timestamp() * 1000)
    records = [Record(e["meta"]["id"], int(e["timestamp"]), ms, _render(e)) for e in events]
    return Phase(name, records, [records], sent_at)


def lakehouse_events(
    sample: Sequence[Mapping[str, Any]], landed: datetime, tag: int
) -> list[dict[str, Any]]:
    """``test_lakehouse.py``'s replay as M5 ran it: re-timed 2 s apart from just after
    the newest event either store holds."""
    start = int((landed + timedelta(seconds=1)).replace(microsecond=0).timestamp())
    out = []
    for i, e in enumerate(sample[:TEST_EVENTS]):
        event = copy.deepcopy(dict(e))
        event["meta"] = {**event["meta"], "id": f"{tag:08x}-0000-4000-8000-{i:012x}"}
        event["timestamp"] = start + i * LAKEHOUSE_SPACING_S
        out.append(event)
    return out


def pipeline_events(
    sample: Sequence[Mapping[str, Any]], landed: datetime, tag: int
) -> list[dict[str, Any]]:
    """``test_pipeline.py``'s replay as M5 ran it: the first 5,000 sample events shifted so
    the oldest lands one second after the newest landed row, plus its sentinel."""
    first = list(sample[:TEST_EVENTS])
    oldest = min(event_time(e) for e in first)
    shift = max(0, int((landed - oldest).total_seconds()) + 1)
    out = []
    for i, e in enumerate(first):
        event = copy.deepcopy(dict(e))
        event["meta"] = {**event["meta"], "id": f"{tag:08x}-0000-4000-8000-{i:012x}"}
        event["timestamp"] = int(e["timestamp"]) + shift
        out.append(event)
    sentinel = copy.deepcopy(out[-1])
    sentinel["meta"] = {**sentinel["meta"], "id": f"{tag:08x}-0000-4000-8000-{TEST_EVENTS:012x}"}
    sentinel["timestamp"] = max(int(e["timestamp"]) for e in out) + 1
    return [*out, sentinel]


def incident_phases(sample: Sequence[Mapping[str, Any]]) -> list[Phase]:
    """A fault run, then ``make test-integration`` (pytest runs ``test_lakehouse.py`` before
    ``test_pipeline.py``), then the next fault run. The tests start two minutes after the
    first run ends and take a minute each; the second run starts two minutes later."""
    phases = [bench_phase("fault run 1", 0xB1, INCIDENT_START, sample)]

    def newest() -> datetime:
        accepted_ts = max(r.event_ts for p in phases for r in p.records)
        return datetime.fromtimestamp(accepted_ts, UTC)

    end1 = INCIDENT_START + timedelta(seconds=len(sample) / BENCH_RATE)
    lake_at = end1 + timedelta(minutes=2)
    phases.append(replay_phase("test_lakehouse", lake_at, lakehouse_events(sample, newest(), 0x1A)))
    pipe_at = lake_at + timedelta(minutes=1)
    phases.append(replay_phase("test_pipeline", pipe_at, pipeline_events(sample, newest(), 0x91)))
    run2_at = pipe_at + timedelta(minutes=3)
    phases.append(bench_phase("fault run 2", 0xB2, run2_at, sample))
    return phases


def incident(
    spark: SparkSession, sample: Sequence[Mapping[str, Any]], bound_s: int
) -> dict[str, Any]:
    """Run the phases as one stream; per phase what was sent, how far ahead, what landed."""
    phases = incident_phases(sample)
    batches = [b for p in phases for b in p.batches]
    work = Path(tempfile.mkdtemp(prefix="tributary-incident-"))
    try:
        ids_by_batch, progress = run_stream(spark, batches, bound_s, work)
    finally:
        shutil.rmtree(work, ignore_errors=True)
    delivered = {i for ids in ids_by_batch.values() for i in ids}
    watermarks = {p.batchId: p.eventTime.get("watermark") for p in progress if p.numInputRows}
    out = []
    first_batch = 0
    for p in phases:
        ahead = [r.event_ts - r.kafka_ms / 1000 for r in p.records]
        last_batch = first_batch + len(p.batches) - 1
        out.append(
            {
                "phase": p.name,
                "sent_at": p.sent_at.isoformat(),
                "events": len(p.records),
                "event_time_min": datetime.fromtimestamp(
                    min(r.event_ts for r in p.records), UTC
                ).isoformat(),
                "event_time_max": datetime.fromtimestamp(
                    max(r.event_ts for r in p.records), UTC
                ).isoformat(),
                "ahead_of_record_time": sum(1 for a in ahead if a > 0),
                "max_ahead_s": round(max(ahead), 1),
                "beyond_bound": sum(1 for a in ahead if a > bound_s),
                "landed": sum(1 for r in p.records if r.event_id in delivered),
                "watermark_after": watermarks.get(last_batch + 1),
                "batches": [first_batch, last_batch],
            }
        )
        first_batch = last_batch + 1
    return {"bound_s": bound_s, "delay_s": DELAY_S, "phases": out}


# ---------------------------------------------------------------- command line


def environment() -> dict[str, Any]:
    import pyspark

    return {
        "made_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "python": platform.python_version(),
        "pyspark": pyspark.__version__,
        "java_home": os.environ.get("JAVA_HOME"),
        "cpus": os.cpu_count(),
        "machine": platform.machine(),
        "spark_master": "local[2]",
        "batch_events": BATCH_EVENTS,
    }


def save(path: Path, doc: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc, indent=1, default=str) + "\n", encoding="utf-8")


def load(path: Path) -> dict[str, Any]:
    doc: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return doc


def _iter_sample() -> Iterator[dict[str, Any]]:
    yield from read_sample(DEFAULT_SAMPLE)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("step", choices=("predict", "run", "incident", "all"))
    parser.add_argument("--arms", default=",".join(ARMS))
    parser.add_argument("--out", type=Path, default=RESULTS)
    args = parser.parse_args(argv)
    records = sample_records()
    prediction_path = args.out / "prediction.json"
    if args.step in ("predict", "all") and not (args.step == "all" and prediction_path.exists()):
        save(prediction_path, prediction_doc(records))
        print(f"wrote {prediction_path}")
    if args.step in ("run", "all"):
        prediction = load(prediction_path)
        save(args.out / "environment.json", environment())
        for arm in args.arms.split(","):
            save(args.out / f"sweep-{arm}.json", run_arm(arm, records, prediction))
    if args.step in ("incident", "all"):
        sample = list(_iter_sample())
        for arm in args.arms.split(","):
            spark = spark_session()
            try:
                doc = incident(spark, sample, arm_bound(arm))
            finally:
                spark.stop()
            save(args.out / f"incident-{arm}.json", doc)
            for row in doc["phases"]:
                print(arm, json.dumps(row))
    return 0


if __name__ == "__main__":
    sys.exit(main())
