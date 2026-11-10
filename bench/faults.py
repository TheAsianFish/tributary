"""``make bench``, part 1: exactly-once under SIGKILL of the stream job and of the broker.

Each run produces 50,000 events with fresh, known ids at a fixed rate into the
platform's own topic, with the stream job in its default configuration
(both queries, warehouse and lake). Scenario ``stream`` kills the job at three
points (when 20%, 50% and 80% of the events have been sent), scenario
``broker`` kills Kafka at the same points, and ``none`` is the control. After
each kill the service stays down for ``downtime`` seconds and is then started
again on the same volumes.

A kill between two micro-batches tests little: the restarted job finds every
batch committed and replays nothing. So a stream kill waits (up to 75 s, more
than the lake's one-minute trigger) for a batch to be *in flight*: the query's
checkpoint has an entry in ``offsets/`` (the batch's planned offsets, written
before it runs) with no matching entry in ``commits/`` (written after the
sink returns). The first and third kills
target the warehouse query, the second the lake query, whose commits are
rarer (one a minute). On restart Spark re-runs that batch: the warehouse's
primary key absorbs rows written before the kill, and Iceberg's sink skips
the append if the snapshot for that batch id was already committed.

Afterwards every acknowledged id must be in ``raw.recentchange`` and in the
Iceberg table exactly once, and no Iceberg snapshot may share a streaming
batch id with another (a batch appended twice).
"""

from __future__ import annotations

import time
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import structlog

from bench import observe, stack, verify
from bench.load import LoadGenerator, LoadPlan, Step, new_prefix
from tributary.config import Settings
from tributary.ingest.producer import ensure_topic

log = structlog.get_logger("bench.faults")

BOOTSTRAP = "localhost:9094"
SCENARIOS = ("none", "stream", "broker")
CHECKPOINTS = "/data/checkpoints"


@dataclass(frozen=True)
class FaultConfig:
    events: int = 50_000
    rate: float = 500.0
    points: tuple[float, ...] = (0.2, 0.5, 0.8)
    downtime_s: float = 10.0
    in_flight_timeout_s: float = 75.0
    """Longer than the lake's one-minute trigger, so a lake batch is always caught."""
    land_timeout_s: float = 600.0
    targets: tuple[str, ...] = ("warehouse", "lake", "warehouse")


def checkpoint_state(cid: str, query: str) -> tuple[int | None, int | None]:
    """Newest batch id in the query's ``offsets/`` and ``commits/`` logs (None if empty)."""
    base = f"{CHECKPOINTS}/{query}"
    out = stack.subprocess_run(
        ["docker", "exec", cid, "sh", "-c", f"ls {base}/offsets; echo --; ls {base}/commits"]
    )
    offsets_part, _, commits_part = out.partition("--")
    return _max_id(offsets_part), _max_id(commits_part)


def _max_id(listing: str) -> int | None:
    ids = [int(name) for name in listing.split() if name.isdigit()]
    return max(ids) if ids else None


def in_flight(offsets: int | None, commits: int | None) -> bool:
    """A batch has been planned (offsets written) and not committed."""
    return offsets is not None and (commits is None or offsets > commits)


def wait_in_flight(cid: str, query: str, timeout: float) -> tuple[bool, int | None]:
    """Poll the checkpoint until a batch of ``query`` is in flight; (found, batch id)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            offsets, commits = checkpoint_state(cid, query)
        except Exception:  # the container may be starting; keep polling
            offsets, commits = None, None
        if in_flight(offsets, commits):
            return True, offsets
        time.sleep(0.05)
    return False, None


def _restart_count(cid: str) -> int:
    out = stack.subprocess_run(["docker", "inspect", "-f", "{{.RestartCount}}", cid])
    return int(out.strip() or 0)


def run_scenario(
    config: FaultConfig, settings: Settings, scenario: str, rep: int
) -> dict[str, Any]:
    """One 50,000-event run; returns the result document for ``bench/results/``."""
    if scenario not in SCENARIOS:
        raise ValueError(f"unknown scenario {scenario}")
    ensure_topic(BOOTSTRAP, settings.topic, settings.topic_partitions)
    stack.wait_healthy("stream")
    stream_cid = stack.container_id("stream")
    restarts_before = _restart_count(stream_cid)
    prefix = new_prefix()
    since = time.time()
    # The step is long enough for the whole run even through broker outages; the limit ends it.
    seconds = config.events / config.rate + 900
    gen = LoadGenerator(
        LoadPlan(
            BOOTSTRAP,
            settings.topic,
            prefix,
            (Step(config.rate, seconds),),
            settings.sample_path,
            limit=config.events,
        )
    )
    gen.start()
    faults: list[dict[str, Any]] = []
    if scenario != "none":
        for i, point in enumerate(config.points):
            target = int(point * config.events)
            while gen.sent < target:
                time.sleep(0.05)
            faults.append(_inject(config, scenario, i, gen.sent))
    result = gen.join()
    produced_s = time.time() - gen.started_at
    pg_rows, pg_wait = verify.wait_until(
        lambda: verify.pg_count(settings.pg_conninfo, prefix),
        result.delivered,
        config.land_timeout_s,
        settle=180,
    )
    lake_rows, lake_wait = verify.wait_until(
        lambda: verify.lake_count(settings, prefix),
        result.delivered,
        config.land_timeout_s,
        settle=240,
        poll=5.0,
    )
    until = time.time()
    pg = verify.compare(prefix, result.delivered, verify.pg_ids(settings.pg_conninfo, prefix))
    lake = verify.compare(prefix, result.delivered, verify.lake_ids(settings, prefix))
    logs = observe.read_log(stack.container_id("stream"), since, until)
    return {
        "scenario": scenario,
        "rep": rep,
        "prefix": prefix,
        "config": config.__dict__,
        "produced": {
            "sent": result.sent,
            "delivered": result.delivered,
            "failed": result.failed,
            "seconds": round(produced_s, 1),
        },
        "faults": faults,
        "postgres": {**pg.as_dict(), "wait_s": round(pg_wait, 1), "count": pg_rows},
        "iceberg": {
            **lake.as_dict(),
            "wait_s": round(lake_wait, 1),
            "count": lake_rows,
            "batches_committed_twice": iceberg_double_commits(settings, since),
        },
        "latency": verify.latency(settings.pg_conninfo, prefix).__dict__,
        "lake_latency": lake_latency(settings, prefix),
        "stream_log": summarise_log(logs),
        "stream_restarts_by_docker": _restart_count(stack.container_id("stream")) - restarts_before,
    }


def _inject(config: FaultConfig, scenario: str, index: int, sent: int) -> dict[str, Any]:
    service = "stream" if scenario == "stream" else "kafka"
    fault: dict[str, Any] = {"service": service, "point": config.points[index], "sent": sent}
    if service == "stream":
        stack.wait_healthy("stream")
        cid = stack.container_id("stream")
        query = config.targets[index % len(config.targets)]
        found, batch = wait_in_flight(cid, query, config.in_flight_timeout_s)
        fault.update(target=query, in_flight=found, batch_id=batch)
    else:
        stack.wait_healthy("kafka")
    fault["killed_at"] = stack.kill(service)
    time.sleep(config.downtime_s)
    started = stack.start(service, wait=False)
    healthy_s = stack.wait_healthy(service, timeout=300)
    fault.update(
        started_at=started, healthy_after_s=round(time.time() - started, 1), waited_s=healthy_s
    )
    log.info("bench.fault", **{k: v for k, v in fault.items() if k != "killed_at"})
    return fault


def iceberg_double_commits(settings: Settings, since: float) -> list[dict[str, Any]]:
    """Streaming batch ids that more than one Iceberg snapshot carries (must be empty)."""
    from tributary.lake.trino_client import fetch

    table = settings.trino_lake_table.rsplit(".", 1)
    snapshots = f'{table[0]}."{table[1]}$snapshots"'
    rows = fetch(
        settings,
        f"SELECT summary['spark.sql.streaming.queryId'], summary['spark.sql.streaming.epochId'],"
        f" count(*) FROM {snapshots}"
        " WHERE operation = 'append' AND committed_at >= from_unixtime(?)"
        " GROUP BY 1, 2 HAVING count(*) > 1",
        [since - 600],
    )
    return [{"query_id": r[0], "epoch": r[1], "snapshots": r[2]} for r in rows]


def lake_latency(settings: Settings, prefix: str) -> dict[str, Any]:
    """Lake latency: the micro-batch's start time (``ingested_at``) minus ``kafka_ts``."""
    from bench.load import id_range
    from tributary.lake.trino_client import fetch

    low, high = id_range(prefix)
    rows = fetch(
        settings,
        "SELECT count(*), approx_percentile(ms, ARRAY[0.5, 0.95, 0.99]), max(ms) FROM ("
        " SELECT to_milliseconds(ingested_at - kafka_ts) AS ms"
        f" FROM {settings.trino_lake_table} WHERE event_id BETWEEN ? AND ?) t",
        [low, high],
    )
    n, quantiles, worst = rows[0]
    q = list(quantiles) if isinstance(quantiles, list) else [None, None, None]
    return {"rows": n, "p50_ms": q[0], "p95_ms": q[1], "p99_ms": q[2], "max_ms": worst}


def summarise_log(records: Sequence[dict[str, Any]]) -> dict[str, Any]:
    """What the stream job's log says happened: starts, failures, replays."""
    sinks = [r for r in records if r.get("event") == "sink.batch"]
    ids = Counter(int(r["batch_id"]) for r in sinks)
    progress = [r for r in records if r.get("event") == "stream.progress"]
    return {
        "starts": sum(1 for r in records if r.get("event") == "stream.started"),
        "terminated_with_error": [
            str(r.get("exception", ""))[:300]
            for r in records
            if r.get("event") == "stream.terminated" and r.get("exception")
        ],
        "sink_batches": len(sinks),
        "sink_rows": sum(int(r.get("rows", 0)) for r in sinks),
        "sink_inserted": sum(int(r.get("inserted", 0)) for r in sinks),
        "duplicates_skipped": sum(int(r.get("duplicates_skipped", 0)) for r in sinks),
        "batches_written_twice": sorted(b for b, n in ids.items() if n > 1),
        "lake_batches": sum(1 for r in progress if r.get("query") == "lake" and r["input_rows"]),
        "dropped_by_watermark": sum(
            int(s.get("rows_dropped_by_watermark") or 0) for r in progress for s in r["state"]
        ),
    }


def save(path: Path, doc: dict[str, Any]) -> None:
    import json

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc, indent=1, default=str) + "\n")
