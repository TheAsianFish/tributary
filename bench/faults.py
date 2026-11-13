"""``make bench``, part 1: exactly-once under SIGKILL of the stream job and of the broker.

Each run produces 50,000 events with fresh, known ids at a fixed rate into the
platform's own topic, with the stream job in its default configuration
(both queries, warehouse and lake). Scenario ``stream`` kills the job at three
points (when 20%, 50% and 80% of the events have been sent), scenario
``broker`` kills Kafka at the same points, and ``none`` is the control. After
each kill the service stays down for ``downtime`` seconds and is then started
again on the same volumes.

A kill between two micro-batches tests little: the restarted job finds every
batch committed and replays nothing. Before batch N runs, Spark writes its
planned offsets to ``offsets/N``; after the sink returns it writes
``commits/N``; a restart re-runs any N without a commit. So each stream kill
is aimed at a point inside that window (:data:`KILL_MODES`):

``warehouse-written``
    Right after the warehouse sink has written batch N: its row in
    ``ops.stream_progress`` appears once every partition has committed, and
    the sink returns to Spark only after that. The rows are in PostgreSQL,
    the commit is not, so the restarted job writes them again and the primary
    key must absorb every one.
``lake-committed``
    Right after the Iceberg table's metadata pointer moves in the JDBC
    catalog: the lake batch's snapshot is committed, Spark's commit is not.
    The replay must find the snapshot carrying its batch id and skip the
    append; afterwards exactly one snapshot may carry that id.
``warehouse``
    Anywhere inside a warehouse batch (``offsets/N`` without ``commits/N``),
    usually before the write.

The detections poll PostgreSQL every 2 ms and the kill is a write to the
container's ``cgroup.kill`` (:class:`bench.stack.CgroupKiller`; ``docker
kill`` where sudo is not available, which is slow enough to lose the race).
Each fault records whether its window was hit: the checkpoint read right
after the kill shows the batch planned but not committed.

Afterwards every acknowledged id must be in ``raw.recentchange`` and in the
Iceberg table exactly once, and no Iceberg snapshot may share a streaming
batch id with another (a batch appended twice).

The ``poison`` scenario (:func:`run_poison`, ``make bench-poison``) is not part
of ``make bench``: it leaves the stack's watermark a day ahead when the job
accepts it. One run sends 50,000 events at 200 events/s with event
:data:`PoisonConfig.seq` stamped 23 hours past its send time, then counts what
reached each sink, reads the batches' loss counters from
``ops.stream_progress`` and records the ``stream_loss`` checks.
"""

from __future__ import annotations

import json
import sys
import time
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import timedelta
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
    targets: tuple[str, ...] = ("warehouse-written", "lake-committed", "warehouse")
    """Where each stream kill lands; see :data:`KILL_MODES`."""


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


KILL_MODES = ("warehouse-written", "lake-committed", "warehouse", "lake")

# rows > 0: the sink's own rows, not the loss counters a progress report adds after commit.
PROGRESS_SQL = (
    "SELECT coalesce(max(batch_id), -1) FROM ops.stream_progress"
    " WHERE query = 'warehouse' AND rows > 0"
)
METADATA_SQL = (
    "SELECT metadata_location FROM iceberg_catalog.iceberg_tables"
    " WHERE table_namespace = 'raw' AND table_name = 'recentchange'"
)


def wait_for_change(conninfo: str, sql: str, timeout: float) -> tuple[bool, object]:
    """Poll ``sql`` (one value) every 2 ms until it differs from its first value."""
    import psycopg

    deadline = time.monotonic() + timeout
    with psycopg.connect(conninfo, autocommit=True) as conn:
        row = conn.execute(sql).fetchone()
        first = row[0] if row else None
        while time.monotonic() < deadline:
            row = conn.execute(sql).fetchone()
            value = row[0] if row else None
            if value != first:
                return True, value
            time.sleep(0.002)
    return False, None


FUTURE_EVENTS_SQL = (
    "SELECT count(*), max(event_ts) FROM raw.recentchange WHERE event_ts > clock_timestamp() + %s"
)


def refuse_future_watermark(conninfo: str, slack_s: float = 300.0) -> None:
    """Raise if the landing table holds events stamped further ahead than the skew bound.

    The job's watermark follows the newest event time it has seen. Before M11
    the parser accepted event times up to a day ahead, and ``make
    test-integration`` replayed events hours ahead of the clock (it anchored
    them after the newest landed row), so the watermark ran past the wall clock
    and every benchmark event, timed by the wall clock, was dropped as late: a
    fault run reported all 50,000 missing. The job now rejects anything more
    than the bound (300 s) ahead of its record time, so it cannot get there
    itself; the check stays because a stack is state that outlives the code: a
    volume from before the fix, or a job run with the old bound (``make
    bench-poison ARM=before`` leaves exactly that), would lose a run the same
    way. Refusing costs one query; recording that run would cost an hour.
    """
    import psycopg

    with psycopg.connect(conninfo) as conn:
        row = conn.execute(FUTURE_EVENTS_SQL, (timedelta(seconds=slack_s),)).fetchone()
    count, newest = (int(row[0]), row[1]) if row is not None else (0, None)
    if count:
        raise RuntimeError(
            f"{count} landed events are stamped in the future (newest {newest}); the stream's "
            "watermark is ahead of the wall clock and would drop every benchmark event. "
            "Run `make down` and start a fresh stack."
        )


@dataclass(frozen=True)
class PoisonConfig:
    events: int = 50_000
    rate: float = 200.0
    """Slow enough that the lake (one batch a minute) sees several batches after the poison's."""
    seq: int = 5_000
    ahead_s: int = 82_800
    """23 h: under the old one-day bound, so the job before the fix accepted it."""
    land_timeout_s: float = 600.0
    settle_s: float = 150.0
    """Longer than two lake triggers: a count that has not moved for this long is final."""


STREAM_PROGRESS_SQL = (
    "SELECT query, count(*), coalesce(sum(input_rows), 0), coalesce(sum(dropped_by_watermark), 0),"
    " coalesce(sum(malformed_rows), 0), coalesce(sum(future_rows), 0),"
    " coalesce(sum(canary_rows), 0), max(watermark)"
    " FROM ops.stream_progress WHERE input_rows IS NOT NULL AND committed_at >= to_timestamp(%s)"
    " GROUP BY query ORDER BY query"
)
LATEST_QUALITY_SQL = (
    "SELECT check_name, target, status, failures, details->>'message' FROM ops.latest_quality"
    " WHERE check_type = 'stream_loss' ORDER BY check_name"
)


def stream_progress_since(conninfo: str, since: float) -> dict[str, dict[str, Any]]:
    """Per query, the loss counters of the batches committed since ``since``."""
    import psycopg

    with psycopg.connect(conninfo, autocommit=True) as conn:
        rows = conn.execute(STREAM_PROGRESS_SQL, (since,)).fetchall()
    keys = ("batches", "input_rows", "dropped_by_watermark", "malformed_rows", "future_rows")
    return {
        str(r[0]): {
            **dict(zip((*keys, "canary_rows"), (int(v) for v in r[1:7]), strict=True)),
            "max_watermark": r[7],
        }
        for r in rows
    }


def stream_bound(cid: str) -> str:
    """The stream container's ``TRIBUTARY_MAX_FUTURE_SKEW_S`` (empty: the default)."""
    env = stack.subprocess_run(
        ["docker", "inspect", "-f", "{{range .Config.Env}}{{println .}}{{end}}", cid]
    )
    for line in env.splitlines():
        if line.startswith("TRIBUTARY_MAX_FUTURE_SKEW_S="):
            return line.partition("=")[2]
    return ""


def run_poison(config: PoisonConfig, settings: Settings, arm: str) -> dict[str, Any]:
    """One poisoned 50,000-event run on the Compose stack (see the module docstring)."""
    import psycopg

    from bench.load import bench_id
    from tributary.warehouse.stream_health import check_stream, summary

    refuse_future_watermark(settings.pg_conninfo, slack_s=settings.future_skew_s)
    ensure_topic(BOOTSTRAP, settings.topic, settings.topic_partitions)
    stack.wait_healthy("stream")
    cid = stack.container_id("stream")
    prefix = new_prefix()
    since = time.time()
    gen = LoadGenerator(
        LoadPlan(
            BOOTSTRAP,
            settings.topic,
            prefix,
            (Step(config.rate, config.events / config.rate + 900),),
            settings.sample_path,
            limit=config.events,
            poison=(config.seq, config.ahead_s),
        )
    )
    gen.start()
    result = gen.join()
    sent_s = time.time() - gen.started_at
    pg_rows, pg_wait = verify.wait_until(
        lambda: verify.pg_count(settings.pg_conninfo, prefix),
        result.delivered,
        config.land_timeout_s,
        settle=config.settle_s,
    )
    lake_rows, lake_wait = verify.wait_until(
        lambda: verify.lake_count(settings, prefix),
        result.delivered,
        config.land_timeout_s,
        settle=config.settle_s,
        poll=5.0,
    )
    poison_id = bench_id(prefix, config.seq)
    sinks: dict[str, Any] = {}
    for name, ids in (
        ("postgres", verify.pg_ids(settings.pg_conninfo, prefix)),
        ("iceberg", verify.lake_ids(settings, prefix)),
    ):
        delivery = verify.compare(prefix, result.delivered, ids)
        present = {i.lower() for i in ids}
        poison_landed = poison_id in present
        sinks[name] = {
            **delivery.as_dict(),
            "poison_landed": poison_landed,
            "lost_real": delivery.missing - (0 if poison_landed else 1),
            "landed_before_poison": sum(
                1 for s in range(config.seq) if bench_id(prefix, s) in present
            ),
        }
    sinks["postgres"].update(count=pg_rows, wait_s=round(pg_wait, 1))
    sinks["iceberg"].update(count=lake_rows, wait_s=round(lake_wait, 1))
    run_id = f"bench-poison-{arm}-{prefix}"
    checks = check_stream(settings, run_id, raise_on_fail=False)
    with psycopg.connect(settings.pg_conninfo, autocommit=True) as conn:
        latest = conn.execute(LATEST_QUALITY_SQL).fetchall()
    until = time.time()
    logs = observe.read_log(cid, since, until)
    return {
        "scenario": "poison",
        "arm": arm,
        "prefix": prefix,
        "config": config.__dict__,
        "stream_max_future_skew_s": stream_bound(cid) or "default",
        "skew_bound_warning": any(
            r.get("event") == "stream.skew_bound_unsafe" for r in observe.read_log(cid, 0, until)
        ),
        "produced": {
            "sent": result.sent,
            "delivered": result.delivered,
            "failed": result.failed,
            "seconds": round(sent_s, 1),
        },
        "poison": {"seq": config.seq, "id": poison_id, "ahead_s": config.ahead_s},
        **sinks,
        "stream_progress": stream_progress_since(settings.pg_conninfo, since),
        "checks": summary(checks),
        "latest_quality": [list(r) for r in latest],
        "stream_log": summarise_log(logs),
    }


def _restart_count(cid: str) -> int:
    out = stack.subprocess_run(["docker", "inspect", "-f", "{{.RestartCount}}", cid])
    return int(out.strip() or 0)


def run_scenario(
    config: FaultConfig, settings: Settings, scenario: str, rep: int
) -> dict[str, Any]:
    """One 50,000-event run; returns the result document for ``bench/results/``."""
    if scenario not in SCENARIOS:
        raise ValueError(f"unknown scenario {scenario}")
    refuse_future_watermark(settings.pg_conninfo, slack_s=settings.future_skew_s)
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
            faults.append(_inject(config, settings, scenario, i, gen.sent))
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
    for fault in faults:
        if fault.get("target") == "lake-committed" and fault.get("batch_id") is not None:
            fault["snapshots_with_batch_id"] = lake_epoch_snapshots(
                settings, int(fault["batch_id"]), since
            )
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
        "latency_after_30s": verify.steady_latency(settings.pg_conninfo, prefix).__dict__,
        "lake_latency": lake_latency(settings, prefix),
        "stream_log": summarise_log(logs),
        "stream_restarts_by_docker": _restart_count(stack.container_id("stream")) - restarts_before,
    }


def _inject(
    config: FaultConfig, settings: Settings, scenario: str, index: int, sent: int
) -> dict[str, Any]:
    service = "stream" if scenario == "stream" else "kafka"
    fault: dict[str, Any] = {"service": service, "point": config.points[index], "sent": sent}
    killer: stack.CgroupKiller | None = None
    try:
        if service == "stream":
            stack.wait_healthy("stream")
            cid = stack.container_id("stream")
            mode = config.targets[index % len(config.targets)]
            timeout = config.in_flight_timeout_s
            killer = stack.CgroupKiller(cid) if stack.CgroupKiller.available(cid) else None
            fault["kill_method"] = "cgroup.kill" if killer is not None else "docker kill"
            if mode == "warehouse-written":
                found, value = wait_for_change(settings.pg_conninfo, PROGRESS_SQL, timeout)
                fault.update(target=mode, in_flight=found, batch_id=value)
            elif mode == "lake-committed":
                # The snapshot's batch id is read from the offsets log just after the kill.
                found, _ = wait_for_change(settings.pg_conninfo, METADATA_SQL, timeout)
                fault.update(target=mode, in_flight=found)
            else:
                found, batch = wait_in_flight(cid, mode, timeout)
                fault.update(target=mode, in_flight=found, batch_id=batch)
        else:
            stack.wait_healthy("kafka")
        fault["killed_at"] = killer.kill() if killer is not None else stack.kill(service)
        if service == "stream":
            # Every stream kill, the untargeted ones included, is checked after the
            # fact: what was seen in flight before the kill proves nothing about it.
            query = "lake" if fault["target"] in ("lake-committed", "lake") else "warehouse"
            offsets, commits = _checkpoint_after_kill(stack.container_id("stream"), query)
            fault.update(offsets_after_kill=offsets, commits_after_kill=commits)
            if fault["target"] == "lake-committed":
                fault["batch_id"] = offsets
        time.sleep(config.downtime_s)
        started = stack.start(service, wait=False)
    finally:
        if killer is not None:
            killer.restore()
    healthy_s = stack.wait_healthy(service, timeout=300)
    fault.update(
        started_at=started, healthy_after_s=round(time.time() - started, 1), waited_s=healthy_s
    )
    log.info("bench.fault", **{k: v for k, v in fault.items() if k != "killed_at"})
    return fault


def _checkpoint_after_kill(cid: str, query: str) -> tuple[int | None, int | None]:
    """The query's newest offsets and commits entries, read from the stopped container.

    ``docker exec`` needs a running container, so the volume is read through
    a throwaway one: offsets ahead of commits means the restart will re-run
    that batch.
    """
    image = stack.subprocess_run(["docker", "inspect", "-f", "{{.Config.Image}}", cid]).strip()
    base = f"{CHECKPOINTS}/{query}"
    out = stack.subprocess_run(
        [
            "docker",
            "run",
            "--rm",
            "--volumes-from",
            cid,
            "--entrypoint",
            "sh",
            image,
            "-c",
            f"ls {base}/offsets; echo --; ls {base}/commits",
        ]
    )
    offsets_part, _, commits_part = out.partition("--")
    return _max_id(offsets_part), _max_id(commits_part)


def lake_epoch_snapshots(settings: Settings, epoch: int, since: float) -> int:
    """How many Iceberg append snapshots since ``since`` carry streaming batch ``epoch``."""
    from tributary.lake.trino_client import fetch

    table = settings.trino_lake_table.rsplit(".", 1)
    snapshots = f'{table[0]}."{table[1]}$snapshots"'
    rows = fetch(
        settings,
        f"SELECT count(*) FROM {snapshots} WHERE operation = 'append'"
        " AND summary['spark.sql.streaming.epochId'] = ? AND committed_at >= from_unixtime(?)",
        [str(epoch), since - 600],
    )
    return int(str(rows[0][0])) if rows else 0


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
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc, indent=1, default=str) + "\n")


def main(argv: Sequence[str] | None = None) -> int:
    """``python -m bench.faults poison --arm before|after``: the poisoned run alone."""
    import argparse

    parser = argparse.ArgumentParser(description=main.__doc__)
    parser.add_argument("scenario", choices=("poison",))
    parser.add_argument("--arm", choices=("before", "after"), required=True)
    parser.add_argument("--out", type=Path, default=Path("bench/results/m11-skew"))
    args = parser.parse_args(argv)
    doc = run_poison(PoisonConfig(), Settings(), args.arm)
    save(args.out / f"compose-{args.arm}.json", doc)
    print(json.dumps({k: doc[k] for k in ("postgres", "iceberg", "checks")}, indent=1, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
