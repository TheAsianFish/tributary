"""``make bench``: exactly-once under faults, latency, trigger and backpressure tuning, freshness.

Phases (``--phase``, default all, in this order):

``tuning``
    *Trigger interval.* The stream job (default configuration, both queries)
    on a fresh topic and checkpoint, at 500 and 5,000 events/s, with
    ``triggerInterval`` 1, 2 and 5 s: end-to-end p50/p95 and the job's CPU.
    *Backpressure.* A 300,000-event backlog drained with
    ``maxOffsetsPerTrigger`` 20,000, 100,000 and 250,000 (lake off): drain
    rate, the longest batch, and the job's peak memory. The cap bounds a
    catch-up batch; ``cap / trigger`` is also the most the job can ever
    ingest, however fast it is.
``faults``
    :mod:`bench.faults`: the ``none``, ``stream`` and ``broker`` scenarios,
    ``--reps`` times each, on the platform's own topic and defaults.
``freshness``
    Every ``ops.quality_runs`` row recorded while the bench ran: the share of
    ``warehouse_refresh`` runs with every check passing, and the freshness
    lag the checks measured (needs the ``airflow`` profile up).

Each unit of work writes one JSON file under ``bench/results/m5-bench/`` and
is skipped on a rerun if its file exists.
"""

from __future__ import annotations

import argparse
import itertools
import json
import time
from collections.abc import Sequence
from dataclasses import asdict
from pathlib import Path
from typing import Any

import psycopg
import structlog

from bench import faults, observe, rules, stack, verify
from bench.load import LoadGenerator, LoadPlan, Step, new_prefix
from bench.scale import fill
from tributary.config import Settings
from tributary.ingest.producer import ensure_topic
from tributary.logging import configure_logging

log = structlog.get_logger("bench.ingest")

RESULTS = Path("bench/results/m5-bench")
BOOTSTRAP = "localhost:9094"
TRIGGERS = ("1 second", "2 seconds", "5 seconds")
TUNING_RATES = (500.0, 5000.0)
CAPS = (20_000, 100_000, 250_000)
TUNING_SECONDS = 90.0
TUNING_WARMUP = 20.0
BACKLOG = 300_000


def _slug(text: str) -> str:
    return text.replace(" ", "")


def _save(path: Path, doc: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc, indent=1, default=str) + "\n")


def memory_peak(cid: str) -> int | None:
    """Peak memory (bytes) of the container's cgroup since it was created."""
    try:
        return int((observe.CGROUP_ROOT / f"docker-{cid}.scope" / "memory.peak").read_text())
    except (OSError, ValueError):
        return None


def trigger_run(settings: Settings, trigger: str, rate: float) -> dict[str, Any]:
    """Latency and CPU at one rate with one trigger interval (defaults otherwise)."""
    run = f"trigger-{_slug(trigger)}-{int(rate)}-{new_prefix()[:8]}"
    topic = f"bench.tuning.{run}"
    ensure_topic(BOOTSTRAP, topic, settings.topic_partitions)
    env = {
        "TRIBUTARY_TOPIC": topic,
        "TRIBUTARY_TRIGGER_INTERVAL": trigger,
        "TRIBUTARY_STREAM_CHECKPOINTS": f"/data/checkpoints/bench/{run}",
    }
    since = time.time()
    stack.start_stream(env)
    cid = stack.container_id("stream")
    tail = observe.LogTail(cid, since)
    containers = {"stream": cid, "postgres": stack.container_id("postgres")}
    sampler = observe.Sampler(BOOTSTRAP, topic, settings.topic_partitions, tail, containers)
    with observe.running(tail, sampler):
        while tail.newest("warehouse") is None:
            time.sleep(1.0)
        prefix = new_prefix()
        gen = LoadGenerator(
            LoadPlan(BOOTSTRAP, topic, prefix, (Step(rate, TUNING_SECONDS),), settings.sample_path)
        )
        gen.start()
        t0 = gen.started_at
        result = gen.join()
        verify.wait_until(
            lambda: verify.pg_count(settings.pg_conninfo, prefix), result.delivered, 120, settle=30
        )
        samples = sampler.between(t0 + TUNING_WARMUP, t0 + TUNING_SECONDS)
    lat = verify.latency(settings.pg_conninfo, prefix, t0 + TUNING_WARMUP, t0 + TUNING_SECONDS)
    batches = [
        b
        for p in tail.get("stream.progress")
        if p.get("query") == "warehouse" and (b := rules.batch_from_progress(p)) is not None
        if t0 + TUNING_WARMUP <= b.trigger_at <= t0 + TUNING_SECONDS
    ]
    overhead = [
        sum(v for k, v in b.durations.items() if k not in ("addBatch", "triggerExecution"))
        for b in batches
    ]
    return {
        "trigger": trigger,
        "rate": rate,
        "sent": result.sent,
        "delivered": result.delivered,
        "latency": asdict(lat),
        "batches": len(batches),
        "batch_ms_p50": verify.quantile([b.trigger_ms for b in batches], 0.5),
        "overhead_ms_p50": verify.quantile(overhead, 0.5),
        "cpu_cores": observe.cpu_cores(samples),
    }


def cap_run(settings: Settings, cap: int, topic: str) -> dict[str, Any]:
    """Drain the backlog topic with one ``maxOffsetsPerTrigger`` (lake off, fresh checkpoint)."""
    run = f"cap-{cap}-{new_prefix()[:8]}"
    env = {
        "TRIBUTARY_TOPIC": topic,
        "TRIBUTARY_MAX_OFFSETS_PER_TRIGGER": str(cap),
        "TRIBUTARY_LAKE_ENABLED": "false",
        "TRIBUTARY_STREAM_CHECKPOINTS": f"/data/checkpoints/bench/{run}",
    }
    since = time.time()
    stack.start_stream(env)
    cid = stack.container_id("stream")
    tail = observe.LogTail(cid, since)
    with observe.running(tail):
        deadline = time.monotonic() + 900
        while time.monotonic() < deadline:
            newest = tail.newest("warehouse")
            if newest is not None and (observe.progress_offsets(newest) or 0) >= BACKLOG:
                break
            time.sleep(1.0)
        finished = time.time()
    batches = [
        b
        for p in tail.get("stream.progress")
        if (b := rules.batch_from_progress(p)) is not None and b.rows > 0
    ]
    d = rules.drain_rate(batches, cap)
    first = min((b.trigger_at for b in batches), default=since)
    return {
        "cap": cap,
        "trigger": settings.trigger_interval,
        "drain_rows_per_s": round(d.rows_per_s, 1) if d.rows_per_s else None,
        "measured_batches": d.batches,
        "batches": len(batches),
        "batch_ms_max": max((b.trigger_ms for b in batches), default=None),
        "batch_ms_p50": verify.quantile([b.trigger_ms for b in batches], 0.5),
        "backlog_s": round(finished - first, 1),
        "memory_peak_bytes": memory_peak(cid),
        "ceiling_per_s": cap / _seconds(settings.trigger_interval),
    }


def _seconds(interval: str) -> float:
    value, unit = interval.split()
    return float(value) * {"second": 1, "seconds": 1, "minute": 60, "minutes": 60}[unit]


FRESHNESS_SQL = """
SELECT run_id, check_type, target, status, (details->>'lag_seconds')::float, finished_at
FROM ops.quality_runs
WHERE recorded_at >= to_timestamp(%s) AND recorded_at <= to_timestamp(%s)
ORDER BY finished_at
"""

REFRESH_SQL = """
SELECT run_id, max(finished_at)
FROM ops.refresh_runs
WHERE relation = 'marts.fct_edits' AND recorded_at >= to_timestamp(%s)
  AND recorded_at <= to_timestamp(%s)
GROUP BY run_id ORDER BY 2
"""


def freshness(settings: Settings, start: float, end: float) -> dict[str, Any]:
    """What ``ops.quality_runs`` recorded while the bench ran."""
    with psycopg.connect(settings.pg_conninfo) as conn:
        rows = conn.execute(FRESHNESS_SQL, (start, end)).fetchall()
        refreshes = conn.execute(REFRESH_SQL, (start, end)).fetchall()
    runs: dict[str, list[str]] = {}
    for run_id, _type, _target, status, _lag, _at in rows:
        runs.setdefault(str(run_id), []).append(str(status))
    full_runs = {r: s for r, s in runs.items() if len(s) > 2}  # a refresh, not a freshness poll
    lags: dict[str, list[float]] = {}
    for _run, check_type, target, _status, lag, _at in rows:
        if check_type == "freshness" and lag is not None:
            lags.setdefault(str(target), []).append(float(lag))
    times = [r[1].timestamp() for r in refreshes]
    gaps = [b - a for a, b in itertools.pairwise(times)]
    return {
        "window": [start, end],
        "quality_rows": len(rows),
        "refresh_runs": len(full_runs),
        "refresh_runs_all_pass": sum(1 for s in full_runs.values() if set(s) <= {"pass"}),
        "freshness_polls": len(runs) - len(full_runs),
        "statuses": {s: sum(1 for r in rows if r[3] == s) for s in {r[3] for r in rows}},
        "lag_seconds": {
            target: {
                "checks": len(values),
                "p50": verify.quantile(values, 0.5),
                "p95": verify.quantile(values, 0.95),
                "max": max(values) if values else None,
            }
            for target, values in lags.items()
        },
        "fct_edits_rebuilds": len(times),
        "rebuild_gap_s": {
            "p50": verify.quantile(gaps, 0.5),
            "p95": verify.quantile(gaps, 0.95),
            "max": max(gaps) if gaps else None,
        },
    }


def run(phases: Sequence[str], reps: int, out: Path, settings: Settings) -> None:
    # The freshness window is the fault phase: the platform running as deployed.
    window_path = out / "window.json"
    window = json.loads(window_path.read_text()) if window_path.exists() else {}
    env_path = out / "environment.json"
    if not env_path.exists():
        _save(env_path, {"resources": stack.resources()})
    if "tuning" in phases:
        for rate in TUNING_RATES:
            for trigger in TRIGGERS:
                path = out / f"tuning-trigger-{_slug(trigger)}-{int(rate)}.json"
                if not path.exists():
                    _save(path, trigger_run(settings, trigger, rate))
        caps_needed = [c for c in CAPS if not (out / f"tuning-cap-{c}.json").exists()]
        if caps_needed:
            stack.stop_stream()
            topic = f"bench.tuning.backlog-{new_prefix()[:8]}"
            filled = fill(topic, settings.topic_partitions, BACKLOG, settings.sample_path)
            for cap in caps_needed:
                _save(
                    out / f"tuning-cap-{cap}.json",
                    {"fill": filled, **cap_run(settings, cap, topic)},
                )
        # Back to the platform's own topic and defaults for the fault runs.
        stack.start_stream({})
    if "faults" in phases:
        config = faults.FaultConfig()
        stack.start_stream({})
        if "start" not in window:
            window["start"] = time.time()
            _save(window_path, window)
        for rep in range(1, reps + 1):
            for scenario in faults.SCENARIOS:
                path = out / f"fault-{scenario}-rep{rep}.json"
                if path.exists():
                    continue
                log.info("bench.scenario", scenario=scenario, rep=rep)
                faults.save(path, faults.run_scenario(config, settings, scenario, rep))
    if "freshness" in phases and "start" in window:
        window["end"] = time.time()
        _save(window_path, window)
        _save(out / "freshness.json", freshness(settings, window["start"], window["end"]))


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("--phase", default="tuning,faults,freshness")
    parser.add_argument("--reps", type=int, default=3)
    parser.add_argument("--out", type=Path, default=RESULTS)
    args = parser.parse_args(argv)
    configure_logging("console")
    run(args.phase.split(","), args.reps, args.out, Settings())


if __name__ == "__main__":
    main()
