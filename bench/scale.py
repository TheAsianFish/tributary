"""``make bench-scale``: sustained throughput and p95 at 1, 3 and 6 partitions, and why.

For each partition count P:

1. **Backlog.** A topic with P partitions is filled as fast as the load
   generator can (its own ceiling is recorded), then drained by the stream job
   three times from empty checkpoints: with Spark's ``noop`` sink, with the
   ``python`` sink (rows to the Python workers, then dropped) and with the real
   PostgreSQL sink. These are the per-stage ceilings (:func:`bench.rules.drain_rate`).
2. **Broker.** ``kafka-consumer-perf-test`` reads the same backlog and
   ``kafka-producer-perf-test`` writes sample events, inside the broker's
   container: the broker's ceiling, with no Spark at all.
3. **Sink.** The PostgreSQL drain's batch shapes are replayed from P processes
   with no Spark (:mod:`bench.sink_replay`).
4. **Ladder** (``reps`` times). A fresh topic and checkpoint, an open-loop rate
   ladder of 90 s steps from 0.75 times the drain rate (:func:`bench.rules.judge_step`),
   then 3 minutes at 80% of the sustained rate for latency.

Lake (Iceberg) disabled throughout, except the extra ``iceberg`` row at P = 3,
which repeats the ladder with the lake query running beside the warehouse.
Every step's raw progress lines and samples go to ``bench/results/m5-scale/``,
one JSON file per unit of work, and a rerun skips units already written.
"""

from __future__ import annotations

import argparse
import json
import re
import time
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import structlog

from bench import observe, rules, stack, verify
from bench.load import LoadGenerator, LoadPlan, Step, new_prefix
from bench.sink_replay import replay
from tributary.config import Settings
from tributary.ingest.producer import ensure_topic
from tributary.logging import configure_logging

log = structlog.get_logger("bench.scale")

RESULTS = Path("bench/results/m5-scale")
BOOTSTRAP = "localhost:9094"
INSIDE_BOOTSTRAP = "localhost:9092"
OPTIONAL_SERVICES = ("trino", "airflow-apiserver", "airflow-scheduler", "airflow-dag-processor")


@dataclass(frozen=True)
class Config:
    partitions: tuple[int, ...] = (1, 3, 6)
    reps: int = 3
    iceberg_reps: int = 3
    step_s: float = 90.0
    window_s: float = 60.0
    latency_s: float = 180.0
    latency_warmup_s: float = 30.0
    latency_load: float = 0.8
    backlog: int = 1_200_000
    cap: int = 200_000
    """``maxOffsetsPerTrigger`` for every scaling run: above trigger times any ceiling seen."""
    trigger: str = "5 seconds"
    lake_cap: int = 5_000_000
    ladder_start: float = 0.75
    ladder_factor: float = 1.15
    max_steps: int = 8
    out: Path = RESULTS


def stream_env(
    config: Config, topic: str, partitions: int, sink: str, run: str, *, lake: bool = False
) -> dict[str, str]:
    """The stream container's settings for one run: its own topic and checkpoints."""
    return {
        "TRIBUTARY_TOPIC": topic,
        "TRIBUTARY_TOPIC_PARTITIONS": str(partitions),
        "TRIBUTARY_STREAM_SINK": sink,
        "TRIBUTARY_LAKE_ENABLED": "true" if lake else "false",
        "TRIBUTARY_MAX_OFFSETS_PER_TRIGGER": str(config.cap),
        "TRIBUTARY_LAKE_MAX_OFFSETS_PER_TRIGGER": str(config.lake_cap),
        "TRIBUTARY_TRIGGER_INTERVAL": config.trigger,
        "TRIBUTARY_STREAM_CHECKPOINTS": f"/data/checkpoints/bench/{run}",
    }


def _save(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=1, default=str) + "\n")
    tmp.replace(path)


def _load(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    data: dict[str, Any] = json.loads(path.read_text())
    return data


def fill(topic: str, partitions: int, count: int, sample: Path) -> dict[str, Any]:
    """Create ``topic`` and fill it with ``count`` events as fast as one producer can."""
    ensure_topic(BOOTSTRAP, topic, partitions)
    prefix = new_prefix()
    gen = LoadGenerator(
        LoadPlan(BOOTSTRAP, topic, prefix, (Step(0, 600),), sample, workers=1, limit=count)
    )
    gen.start()
    t = time.monotonic()
    result = gen.join()
    seconds = time.monotonic() - t
    if result.failed or result.delivered != count:
        raise RuntimeError(f"backlog fill: {result}")
    return {
        "topic": topic,
        "prefix": prefix,
        "events": count,
        "seconds": round(seconds, 2),
        "events_per_s": round(count / seconds, 1),
    }


def delete_topic(topic: str) -> None:
    """Drop a finished run's topic (a ladder writes millions of events)."""
    from tributary.ingest.producer import make_admin

    admin = make_admin(BOOTSTRAP)  # keep a reference until the futures resolve
    futures = admin.delete_topics([topic], operation_timeout=30)
    for future in futures.values():
        try:
            future.result(timeout=60)
        except Exception as exc:  # cleanup only; a leftover topic costs disk, not results
            log.warning("bench.topic_not_deleted", topic=topic, error=str(exc))


def _containers() -> dict[str, str]:
    return {s: cid for s, cid in stack.running_services().items() if s != "lake-init"}


def _wait_for_offsets(tail: observe.LogTail, query: str, target: int, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        newest = tail.newest(query)
        if newest is not None and (observe.progress_offsets(newest) or 0) >= target:
            return True
        time.sleep(1.0)
    return False


def drain(
    config: Config, settings: Settings, topic: str, partitions: int, sink: str, backlog: int
) -> dict[str, Any]:
    """Start the job on a filled topic from empty checkpoints; time how fast it empties it."""
    run = f"drain-p{partitions}-{sink}-{new_prefix()[:8]}"
    if sink == "postgres":
        verify.truncate_landing(settings.pg_conninfo)
    since = time.time()
    stack.start_stream(stream_env(config, topic, partitions, sink, run))
    cid = stack.container_id("stream")
    tail = observe.LogTail(cid, since)
    sampler = observe.Sampler(BOOTSTRAP, topic, partitions, tail, _containers(), interval=1.0)
    pg = observe.PgActivity(settings.pg_conninfo)
    with observe.running(tail, sampler, pg):
        done = _wait_for_offsets(tail, "warehouse", backlog, timeout=900)
        time.sleep(2.0)
    progress = tail.get("stream.progress")
    batches = [b for p in progress if (b := rules.batch_from_progress(p)) is not None]
    data = [b for b in batches if b.rows > 0]
    d = rules.drain_rate(data, config.cap)
    measured = data[1:] if len(data) > 1 else data
    window = (measured[0].trigger_at, measured[-1].ended_at) if measured else (since, time.time())
    samples = sampler.between(*window)
    sinks = tail.get("sink.batch")
    return {
        "run": run,
        "sink": sink,
        "partitions": partitions,
        "completed": done,
        "rows_per_s": round(d.rows_per_s, 1) if d.rows_per_s else None,
        "measured_rows": d.rows,
        "measured_batches": d.batches,
        "batches": [asdict(b) for b in data],
        "sink_batches": [
            {k: s.get(k) for k in ("batch_id", "rows", "partitions", "sink_ms", "db_ms_max")}
            for s in sinks
        ],
        "cpu_cores": observe.cpu_cores(samples),
        "host_cores": observe.host_cores(samples, _ncpu()),
        "pg_activity": pg.snapshot(),
    }


def _ncpu() -> int:
    import os

    return os.cpu_count() or 1


PERF_LINE = re.compile(r"([\d.]+) records/sec")


def broker_ceiling(topic: str, partitions: int, count: int, sample: Path) -> dict[str, Any]:
    """Kafka's own consumer and producer perf tools, inside the broker container."""
    consumer_out = stack.exec_in(
        "kafka",
        [
            "/opt/kafka/bin/kafka-consumer-perf-test.sh",
            "--bootstrap-server",
            INSIDE_BOOTSTRAP,
            "--topic",
            topic,
            "--messages",
            str(count),
            "--timeout",
            "60000",
            "--reporting-interval",
            "600000",
        ],
    )
    consumer = _parse_consumer_perf(consumer_out)
    payloads = "/tmp/tributary-payloads.jsonl"  # noqa: S108 (inside the broker container)
    _copy_payloads(sample, payloads)
    perf_topic = f"{topic}.perf"
    ensure_topic(BOOTSTRAP, perf_topic, partitions)
    producer_out = stack.exec_in(
        "kafka",
        [
            "/opt/kafka/bin/kafka-producer-perf-test.sh",
            "--topic",
            perf_topic,
            "--num-records",
            str(count),
            "--throughput",
            "-1",
            "--payload-file",
            payloads,
            "--bootstrap-server",
            INSIDE_BOOTSTRAP,
            "--command-property",
            "acks=all",
            "--command-property",
            "enable.idempotence=true",
            "--command-property",
            "linger.ms=20",
            "--command-property",
            "compression.type=lz4",
        ],
    )
    last = [m for m in PERF_LINE.finditer(producer_out)]
    return {
        "consumer": consumer,
        "consumer_raw": consumer_out.strip().splitlines()[-2:],
        "producer_records_per_s": float(last[-1].group(1)) if last else None,
        "producer_raw": producer_out.strip().splitlines()[-1:],
    }


def _parse_consumer_perf(out: str) -> dict[str, Any]:
    """The CSV result line of ``kafka-consumer-perf-test`` (header, then values)."""
    lines = [ln for ln in out.strip().splitlines() if "," in ln]
    if len(lines) < 2:
        return {"raw": out[-500:]}
    header = [h.strip() for h in lines[-2].split(",")]
    values = [v.strip() for v in lines[-1].split(",")]
    row = dict(zip(header, values, strict=False))
    msgs = row.get("nMsg.sec")
    return {"records_per_s": float(msgs) if msgs else None, **row}


def _copy_payloads(sample: Path, target: str) -> None:
    import gzip
    import subprocess
    import tempfile

    with tempfile.NamedTemporaryFile("wb", suffix=".jsonl", delete=False) as tmp:
        tmp.write(gzip.decompress(sample.read_bytes()))
        local = tmp.name
    # NamedTemporaryFile is 0600; the broker image runs as another user.
    Path(local).chmod(0o644)
    cid = stack.container_id("kafka")
    subprocess.run(["docker", "cp", local, f"{cid}:{target}"], check=True, capture_output=True)
    Path(local).unlink()


@dataclass
class Rep:
    """One ladder plus latency run."""

    run: str
    topic: str
    partitions: int
    lake: bool
    start_rate: float
    steps: list[dict[str, Any]] = field(default_factory=list)
    sustained_per_s: float | None = None
    latency: dict[str, Any] | None = None


def ladder(
    config: Config,
    settings: Settings,
    partitions: int,
    drain_per_s: float,
    *,
    lake: bool,
    sample: Path,
) -> dict[str, Any]:
    """One repetition: fresh topic and checkpoints, the rate ladder, then the latency run."""
    tag = new_prefix()[:8]
    run = f"ladder-p{partitions}{'-iceberg' if lake else ''}-{tag}"
    topic = f"bench.scale.{run}"
    ensure_topic(BOOTSTRAP, topic, partitions)
    verify.truncate_landing(settings.pg_conninfo)
    since = time.time()
    stack.start_stream(stream_env(config, topic, partitions, "postgres", run, lake=lake))
    cid = stack.container_id("stream")
    tail = observe.LogTail(cid, since)
    queries = ("warehouse", "lake") if lake else ("warehouse",)
    sampler = observe.Sampler(BOOTSTRAP, topic, partitions, tail, _containers(), queries)
    pg = observe.PgActivity(settings.pg_conninfo)
    rep = Rep(run, topic, partitions, lake, round(drain_per_s * config.ladder_start, 1))
    with observe.running(tail, sampler, pg):
        _wait_first_progress(tail, timeout=180)
        history: list[tuple[float, bool]] = []
        rate: float | None = rep.start_rate
        while rate is not None and len(history) < config.max_steps:
            step = _run_step(config, topic, rate, sample, tail, sampler, pg, queries)
            rep.steps.append(step)
            history.append((rate, bool(step["sustained"])))
            log.info("bench.step", run=run, rate=rate, verdict=step["verdict"])
            rate = rules.next_rate(history, config.ladder_factor)
        rep.sustained_per_s = rules.sustained_rate(history)
        if rep.sustained_per_s is not None:
            _wait_drained(tail, sampler, queries, timeout=240)
            rep.latency = _latency_run(
                config,
                settings,
                topic,
                rep.sustained_per_s * config.latency_load,
                sample,
                sampler,
                pg,
            )
    return asdict(rep)


def _wait_first_progress(tail: observe.LogTail, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while tail.newest("warehouse") is None:
        if time.monotonic() > deadline:
            raise TimeoutError("the stream job reported no progress")
        time.sleep(1.0)


def _wait_drained(
    tail: observe.LogTail, sampler: observe.Sampler, queries: Sequence[str], timeout: float
) -> float:
    """Wait until every query has finished all offsets the broker holds."""
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        last = sampler.samples[-1] if sampler.samples else None
        if last is not None and all((last.lag(q) or 0) <= 0 for q in queries):
            return time.monotonic() - start
        time.sleep(1.0)
    return time.monotonic() - start


def _run_step(
    config: Config,
    topic: str,
    rate: float,
    sample: Path,
    tail: observe.LogTail,
    sampler: observe.Sampler,
    pg: observe.PgActivity,
    queries: Sequence[str],
) -> dict[str, Any]:
    prefix = new_prefix()
    gen = LoadGenerator(
        LoadPlan(BOOTSTRAP, topic, prefix, (Step(rate, config.step_s),), sample, workers=1)
    )
    gen.start()
    t0 = gen.started_at
    pg.reset()
    result = gen.join()
    end = t0 + config.step_s
    time.sleep(1.0)
    verdicts: dict[str, Any] = {}
    for q in queries:
        batches = [
            b
            for p in tail.get("stream.progress")
            if p.get("query") == q and (b := rules.batch_from_progress(p)) is not None
        ]
        window = config.window_s if q == "warehouse" else config.step_s
        min_batches = rules.MIN_BATCHES if q == "warehouse" else 1
        v = rules.judge_step(
            batches, rate, result.sent, end, config.step_s, window=window, min_batches=min_batches
        )
        in_step = [b for b in batches if t0 <= b.trigger_at <= end]
        verdicts[q] = {
            **asdict(v),
            "batches_in_step": [asdict(b) for b in in_step],
        }
    samples = sampler.between(end - config.window_s, end)
    warehouse = verdicts["warehouse"]
    # The lake triggers once a minute, so one 90 s step holds one or two of its batches:
    # its verdict is reported beside the warehouse's, and only the warehouse's decides.
    return {
        "rate": round(rate, 1),
        "sent": result.sent,
        "delivered": result.delivered,
        "failed": result.failed,
        "generator_cpu_s": result.cpu_s,
        "started_at": t0,
        "sustained": warehouse["sustained"],
        "verdict": warehouse["reason"],
        "queries": verdicts,
        "lag_samples": [{"t": round(s.t, 2), **{q: s.lag(q) for q in queries}} for s in samples],
        "cpu_cores": observe.cpu_cores(samples),
        "host_cores": observe.host_cores(samples, _ncpu()),
        "pg_activity": pg.snapshot(),
    }


def _latency_run(
    config: Config,
    settings: Settings,
    topic: str,
    rate: float,
    sample: Path,
    sampler: observe.Sampler,
    pg: observe.PgActivity,
) -> dict[str, Any]:
    prefix = new_prefix()
    gen = LoadGenerator(
        LoadPlan(BOOTSTRAP, topic, prefix, (Step(rate, config.latency_s),), sample, workers=1)
    )
    gen.start()
    t0 = gen.started_at
    pg.reset()
    result = gen.join()
    verify.wait_until(
        lambda: verify.pg_count(settings.pg_conninfo, prefix), result.delivered, 180, settle=30
    )
    lat = verify.latency(
        settings.pg_conninfo, prefix, t0 + config.latency_warmup_s, t0 + config.latency_s
    )
    samples = sampler.between(t0 + config.latency_warmup_s, t0 + config.latency_s)
    return {
        "rate": round(rate, 1),
        "sent": result.sent,
        "delivered": result.delivered,
        "landed": verify.pg_count(settings.pg_conninfo, prefix),
        "generator_cpu_s": result.cpu_s,
        "window_s": [config.latency_warmup_s, config.latency_s],
        **asdict(lat),
        "cpu_cores": observe.cpu_cores(samples),
        "host_cores": observe.host_cores(samples, _ncpu()),
        "pg_activity": pg.snapshot(),
    }


def sink_ceiling(
    settings: Settings, partitions: int, drain_result: Mapping[str, Any], sample: Path
) -> dict[str, Any]:
    """Replay the PostgreSQL drain's batches (rows per partition) from ``partitions`` processes."""
    sizes = [
        max(1, int(b["rows"]) // partitions) for b in drain_result["sink_batches"] if b.get("rows")
    ]
    verify.truncate_landing(settings.pg_conninfo)
    containers = _containers()
    sampler = observe.Sampler(BOOTSTRAP, "unused", 0, None, containers, interval=1.0)
    pg = observe.PgActivity(settings.pg_conninfo)
    with observe.running(sampler, pg):
        result = replay(settings.pg_conninfo, sample, new_prefix(), partitions, sizes)
        end = time.time()
        # Only the writes: each process builds its rows before the common start.
        samples = sampler.between(end - result.seconds - 1.0, end)
    return {
        "partitions": partitions,
        "batch_sizes": sizes,
        "rows": result.rows,
        "seconds": round(result.seconds, 3),
        "rows_per_s": round(result.rows_per_s, 1),
        "per_process_seconds": [round(x, 3) for x in result.per_process_seconds],
        "cpu_cores": observe.cpu_cores(samples),
        "host_cores": observe.host_cores(samples, _ncpu()),
        "pg_activity": pg.snapshot(),
    }


def run(config: Config, settings: Settings) -> None:
    sample = settings.sample_path
    out = config.out
    # Only Kafka, PostgreSQL, MinIO and the stream job: Trino and Airflow would take CPU.
    stack.compose("stop", *OPTIONAL_SERVICES, check=False)
    _save(out / "environment.json", {"config": asdict(config), "resources": stack.resources()})
    for p in config.partitions:
        stages_path = out / f"p{p}-stages.json"
        stages = _load(stages_path) or {}
        # Each stage is saved as soon as it is measured, so a rerun resumes after the last one.
        if "fill" not in stages:
            topic = f"bench.scale.backlog-p{p}-{new_prefix()[:8]}"
            log.info("bench.fill", partitions=p, events=config.backlog)
            stages["fill"] = fill(topic, p, config.backlog, sample)
            stages["drains"] = {}
            _save(stages_path, stages)
        topic = str(stages["fill"]["topic"])
        for sink in ("noop", "python", "postgres"):
            if sink not in stages["drains"]:
                stages["drains"][sink] = drain(config, settings, topic, p, sink, config.backlog)
                _save(stages_path, stages)
        if "broker" not in stages:
            stack.stop_stream()
            stages["broker"] = broker_ceiling(topic, p, config.backlog, sample)
            _save(stages_path, stages)
        if "sink" not in stages:
            stack.stop_stream()
            stages["sink"] = sink_ceiling(settings, p, stages["drains"]["postgres"], sample)
            _save(stages_path, stages)
        drain_rate = float(stages["drains"]["postgres"]["rows_per_s"])
        lakes = [False, True] if p == 3 else [False]
        for lake in lakes:
            reps = config.iceberg_reps if lake else config.reps
            for r in range(1, reps + 1):
                path = out / f"p{p}{'-iceberg' if lake else ''}-rep{r}.json"
                if path.exists():
                    continue
                log.info("bench.ladder", partitions=p, lake=lake, rep=r)
                doc = ladder(config, settings, p, drain_rate, lake=lake, sample=sample)
                _save(path, doc)
                stack.stop_stream()
                delete_topic(str(doc["topic"]))
    stack.stop_stream()


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("--partitions", default="1,3,6")
    parser.add_argument("--reps", type=int, default=3)
    parser.add_argument("--iceberg-reps", type=int, default=3)
    parser.add_argument("--step", type=float, default=90.0)
    parser.add_argument("--out", type=Path, default=RESULTS)
    args = parser.parse_args(argv)
    configure_logging("console")
    config = Config(
        partitions=tuple(int(x) for x in args.partitions.split(",")),
        reps=args.reps,
        iceberg_reps=args.iceberg_reps,
        step_s=args.step,
        out=args.out,
    )
    run(config, Settings())


if __name__ == "__main__":
    main()
