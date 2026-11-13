"""``make bench-late``: can the pipeline tell a late copy from a lost event, and recover a lost one?

One run on a fresh Compose stack (``make down`` first), in four phases:

1. **First replay.** The committed sample (50,000 events, event times of
   2026-10-16) through ``tributary produce``; wait until all 50,000 have landed
   and the stream has settled.
2. **Second replay.** The same sample again. Every event is a copy of one that
   landed, so nothing can be lost; the dedup operator drops the copies above the
   watermark from its state and the watermark drops the rest as late, which
   counts them in ``rows_dropped_by_watermark``. The prediction
   (:func:`predict`) is committed before the run.
3. **Load.** 50,000 benchmark events at 200 events/s, timed by the wall clock
   (:mod:`bench.load`), of which :data:`LATE_SEQS` (100) are stamped
   :data:`LATE_BEHIND_S` behind their send time (15 minutes behind the
   watermark, which trails the newest event by 10), one (:data:`MALFORMED_SEQ`)
   is not JSON and one (:data:`FUTURE_SEQ`) is stamped an hour ahead. In arm
   ``after`` a reconciliation pass (``tributary stream reconcile``) runs every
   :data:`PASS_EVERY_S` during the load, and one more once it has landed.
4. **Read-out.** Per arm: rows dropped by the watermark, events missing from
   PostgreSQL (sample and benchmark ids alike), every pass's verdict and the
   ids it reported missing, the ``stream_loss`` checks, ``dbt build`` and the
   recovered events in ``marts.fct_edits``, the provenance line of one mart
   answer, and the per-hour PostgreSQL-against-Iceberg reconciliation.

Arm ``before`` runs no pass: what the platform did before M12 (with no pass on
record the check fails on the drop counter alone, exactly as it did).

``predict`` writes ``prediction.json`` from the sample and this module's
constants alone, before any run. Raw output under ``bench/results/m12-late/``.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from tributary.config import DEFAULT_SAMPLE
from tributary.events import event_time
from tributary.ingest.sample import read_sample

RESULTS = Path(__file__).resolve().parent / "results" / "m12-late"

DELAY_S = 600
"""The stream job's default watermark delay (``TRIBUTARY_WATERMARK_DELAY``)."""
EVENTS = 50_000
RATE = 200.0
LATE_SEQS = tuple(range(10_000, 50_000, 400))
"""100 benchmark events, spread over the last four fifths of the load."""
LATE_BEHIND_S = DELAY_S + 900
"""Stamped this far behind their send time: 15 minutes behind the watermark."""
MALFORMED_SEQ = 20_200
FUTURE_SEQ = 30_200
FUTURE_AHEAD_S = 3_600
PASS_EVERY_S = 30.0
PASS_LIMIT_S = 60.0
"""The hypothesis for one pass over the whole topic."""


def predict_second_replay(times: Sequence[int], delay_s: int = DELAY_S) -> dict[str, int]:
    """Rows the second replay loses to the watermark, from the sample's event times.

    After the first replay has settled, the watermark is the newest event time
    minus the delay, and the second replay's copies cannot move it (the same
    times). Spark drops a row as late when its event time is at or below the
    watermark; the rest of the copies are still keys in the dedup state (a key
    stays until the watermark passes its event time plus the delay) and are
    dropped there, which the drop counter does not count.
    """
    watermark = max(times) - delay_s
    dropped = sum(1 for t in times if t <= watermark)
    return {
        "events": len(times),
        "watermark": watermark,
        "dropped_by_watermark": dropped,
        "caught_by_dedup_state": len(times) - dropped,
        "lost": 0,
    }


def prediction(sample: Path = DEFAULT_SAMPLE) -> dict[str, Any]:
    """Everything the run is expected to show, before it runs."""
    times = [int(event_time(e).timestamp()) for e in read_sample(sample)]
    second = predict_second_replay(times)
    return {
        "made_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "rule": (
            "after the first replay settles the watermark is max(event_ts) - delay; the second"
            " replay drops a copy as late when event_ts <= watermark, and the dedup state"
            " catches every other copy"
        ),
        "sample": {
            "events": len(times),
            "first": datetime.fromtimestamp(min(times), UTC).isoformat(),
            "last": datetime.fromtimestamp(max(times), UTC).isoformat(),
        },
        "delay_s": DELAY_S,
        "second_replay": {
            **second,
            "watermark": datetime.fromtimestamp(second["watermark"], UTC).isoformat(),
            "check_before": "fail",
            "check_after": "pass",
        },
        "load": {
            "events": EVENTS,
            "rate": RATE,
            "late": {"seqs": len(LATE_SEQS), "behind_send_s": LATE_BEHIND_S},
            "malformed_seq": MALFORMED_SEQ,
            "future": {"seq": FUTURE_SEQ, "ahead_s": FUTURE_AHEAD_S},
        },
        "hypotheses": {
            "in_flight_reported_missing": 0,
            "rejected_counted_not_landed": {"malformed": 1, "future": 1},
            "late_missing_before": len(LATE_SEQS),
            "late_missing_after": 0,
            "pass_over_topic_s_under": PASS_LIMIT_S,
        },
    }


# ----------------------------------------------------------------------- the run

BOOTSTRAP = "localhost:9094"
RECONCILER = "tributary-bench-reconcile"
LAND_TIMEOUT_S = 900.0
SETTLE_S = 20.0
"""How long the warehouse must hold every offset of the topic before a phase is over."""
LAKE_SETTLE_S = 150.0
"""Longer than two lake triggers: a lake count that has not moved for this long is final."""


def log(event: str, **fields: Any) -> None:
    print(
        json.dumps(
            {"t": datetime.now(UTC).isoformat(timespec="seconds"), "event": event, **fields},
            default=str,
        ),
        flush=True,
    )


_consumer: Any = None


def high_watermarks(topic: str) -> dict[str, int]:
    """The broker's next offset per partition of the topic (one consumer for the run)."""
    from confluent_kafka import Consumer, TopicPartition

    global _consumer
    if _consumer is None:
        _consumer = Consumer(
            {"bootstrap.servers": BOOTSTRAP, "group.id": "bench-late", "enable.auto.commit": False}
        )
    meta = _consumer.list_topics(topic, timeout=10)
    out = {}
    for p in sorted(meta.topics[topic].partitions):
        _low, high = _consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
        out[str(p)] = int(high)
    return out


def query_offsets(conninfo: str, topic: str, query: str) -> dict[str, int]:
    """The largest end offset per partition the query has recorded."""
    from tributary.stream.reconcile import committed_offsets

    return committed_offsets(conninfo, topic, query)


def wait_settled(conninfo: str, topic: str, query: str, timeout: float = LAND_TIMEOUT_S) -> float:
    """Wait until the query has committed every offset the broker holds, and held it for
    :data:`SETTLE_S` (so its watermark has seen every record); seconds waited."""
    import time

    start = time.monotonic()
    caught_up_at: float | None = None
    while time.monotonic() - start < timeout:
        try:
            high = high_watermarks(topic)
        except Exception as exc:  # a broker hiccup: poll again
            log("watermarks_unavailable", error=str(exc)[:200])
            time.sleep(2.0)
            continue
        done = query_offsets(conninfo, topic, query)
        if all(done.get(p, 0) >= h for p, h in high.items()):
            caught_up_at = caught_up_at or time.monotonic()
            if time.monotonic() - caught_up_at >= SETTLE_S:
                return time.monotonic() - start
        else:
            caught_up_at = None
        time.sleep(2.0)
    raise TimeoutError(f"{query} did not commit the topic's offsets within {timeout:.0f} s")


def replay_sample(settings: Any, rate: float) -> dict[str, Any]:
    """The committed sample through the platform's own producer, as `tributary produce`."""
    import time

    from tributary.ingest.producer import make_producer, produce_events

    start = time.monotonic()
    stats = produce_events(
        read_sample(settings.sample_path), make_producer(BOOTSTRAP), settings.topic, rate=rate
    )
    return {
        "sent": stats.sent,
        "delivered": stats.delivered,
        "failed": stats.failed,
        "seconds": round(time.monotonic() - start, 1),
    }


PROGRESS_SQL = (
    "SELECT query, count(*), coalesce(sum(input_rows), 0), coalesce(sum(dropped_by_watermark), 0),"
    " coalesce(sum(malformed_rows), 0), coalesce(sum(future_rows), 0)"
    " FROM ops.stream_progress WHERE input_rows IS NOT NULL"
    " AND reported_at >= to_timestamp(%s) AND reported_at < to_timestamp(%s)"
    " GROUP BY query ORDER BY query"
)


def progress_between(conninfo: str, since: float, until: float) -> dict[str, dict[str, int]]:
    """Per query, the loss counters of the batches reported in ``[since, until)``."""
    import psycopg

    with psycopg.connect(conninfo, autocommit=True) as conn:
        rows = conn.execute(PROGRESS_SQL, (since, until)).fetchall()
    keys = ("batches", "input_rows", "dropped_by_watermark", "malformed_rows", "future_rows")
    return {str(r[0]): dict(zip(keys, (int(v) for v in r[1:]), strict=True)) for r in rows}


def missing_ids(conninfo: str, ids: Sequence[str]) -> list[str]:
    """The ids of ``ids`` that are not in ``raw.recentchange``."""
    import psycopg

    with psycopg.connect(conninfo) as conn:
        rows = conn.execute(
            "SELECT i::text FROM unnest(%s::uuid[]) AS i WHERE NOT EXISTS"
            " (SELECT 1 FROM raw.recentchange r WHERE r.event_id = i)",
            (list(ids),),
        ).fetchall()
    return sorted(str(r[0]) for r in rows)


def checks_now(settings: Any, run_id: str) -> list[dict[str, object]]:
    from tributary.warehouse.stream_health import check_stream, summary

    return summary(check_stream(settings, run_id, raise_on_fail=False))


def compose_run(
    args: Sequence[str], *, detach: bool = False, timeout: float = 900
) -> tuple[int, str, str]:
    """``docker compose run`` of the stream image (no dependencies started): exit code,
    stdout, the tail of stderr. Never raises on a non-zero exit, so a failed pass is
    recorded rather than losing the run's results."""
    from bench import stack

    flags = ["--no-deps", "-T"] + (["-d", "--name", RECONCILER] if detach else ["--rm"])
    proc = stack.compose("run", *flags, "stream", *args, check=False, timeout=timeout)
    return proc.returncode, proc.stdout, proc.stderr[-2000:]


def pass_lines(text: str) -> list[dict[str, Any]]:
    """The JSON line each pass prints (the CLI's stdout), in order."""
    out = []
    for line in text.splitlines():
        if line.startswith("{") and '"session_s"' in line:
            out.append(json.loads(line))
    return out


PASSES_SQL = (
    "SELECT id, run_id, records, landed, malformed, future, canary, missing, recovered,"
    " left_missing, seconds, started_at, finished_at, start_offsets, end_offsets"
    " FROM ops.reconcile_runs ORDER BY id"
)
MISSING_BY_RUN_SQL = "SELECT pass_id, event_id::text, left_missing FROM ops.reconcile_missing"


def recorded_passes(conninfo: str, late: set[str]) -> list[dict[str, Any]]:
    """Every pass in ``ops.reconcile_runs`` with its missing ids split into the late events
    the load sent (expected) and anything else (an in-flight event judged too early)."""
    import psycopg

    with psycopg.connect(conninfo) as conn:
        runs = conn.execute(PASSES_SQL).fetchall()
        missing = conn.execute(MISSING_BY_RUN_SQL).fetchall()
    by_pass: dict[int, list[str]] = {}
    for pass_id, event_id, _left in missing:
        by_pass.setdefault(int(pass_id), []).append(str(event_id))
    keys = (
        "id",
        "run_id",
        "records",
        "landed",
        "malformed",
        "future",
        "canary",
        "missing",
        "recovered",
        "left_missing",
        "seconds",
        "started_at",
        "finished_at",
        "start_offsets",
        "end_offsets",
    )
    out = []
    for r in runs:
        doc = dict(zip(keys, r, strict=True))
        ids = by_pass.get(int(r[0]), [])
        doc["missing_late"] = sum(1 for i in ids if i in late)
        doc["missing_other"] = sorted(i for i in ids if i not in late)
        out.append(doc)
    return out


def fct_edits_counts(conninfo: str, ids: Sequence[str]) -> dict[str, int]:
    """Of ``ids``: how many are in raw.recentchange, how many of those are edits (what
    fct_edits keeps), and how many are in marts.fct_edits."""
    import psycopg

    with psycopg.connect(conninfo) as conn:
        row = conn.execute(
            "SELECT count(r.event_id), count(r.event_id) FILTER (WHERE r.event_type IN"
            " ('edit', 'new')), count(f.event_id) FROM unnest(%s::uuid[]) AS i"
            " LEFT JOIN raw.recentchange r ON r.event_id = i"
            " LEFT JOIN marts.fct_edits f ON f.event_id = i",
            (list(ids),),
        ).fetchone()
    assert row is not None
    return {"in_raw": int(row[0]), "edits_in_raw": int(row[1]), "in_fct_edits": int(row[2])}


def warehouse_refresh(label: str) -> dict[str, Any]:
    """``tributary warehouse refresh`` (dbt seed, run and test; the catalog), timed."""
    import subprocess
    import time

    start = time.monotonic()
    proc = subprocess.run(
        ["uv", "run", "tributary", "warehouse", "refresh"],
        capture_output=True,
        text=True,
        check=False,
        timeout=1800,
    )
    tail = (proc.stdout + proc.stderr).strip().splitlines()[-1:] or [""]
    log("refresh", label=label, exit=proc.returncode, line=tail[0][:300])
    return {
        "exit": proc.returncode,
        "seconds": round(time.monotonic() - start, 1),
        "last_line": tail[0][:500],
    }


def mart_answer(sql: str) -> dict[str, Any]:
    """``tributary sql`` on a mart: the agent's guarded path, with its provenance."""
    import subprocess

    text = subprocess.run(
        ["uv", "run", "tributary", "sql", sql],
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    as_json = subprocess.run(
        ["uv", "run", "tributary", "sql", "--json", sql],
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    doc: dict[str, Any] = {"sql": sql, "exit": text.returncode, "text": text.stdout.strip()}
    try:
        answer = json.loads(as_json.stdout)
        doc["provenance"] = answer.get("provenance")
        doc["rows"] = answer.get("rows")
    except ValueError:
        doc["stderr"] = (text.stderr or as_json.stderr)[-500:]
    return doc


def lake_hours(settings: Any, ids: Sequence[str]) -> list[dict[str, Any]]:
    """The per-hour PostgreSQL-against-Iceberg comparison over every hour the run wrote,
    whether or not the watermark has closed it yet (the hourly DAG compares only closed
    hours; the recovered events' hours may still be open when the run ends)."""
    import psycopg

    from tributary.lake.reconcile import compare_hours, warehouse_hours
    from tributary.lake.reconcile import lake_hours as lake_side

    with psycopg.connect(settings.pg_conninfo) as conn:
        row = conn.execute(
            "SELECT date_trunc('hour', min(event_ts), 'UTC'),"
            " date_trunc('hour', max(event_ts), 'UTC') + interval '1 hour' FROM raw.recentchange"
        ).fetchone()
    assert row is not None
    start, end = row
    checks = compare_hours(
        warehouse_hours(settings.pg_conninfo, start, end),
        lake_side(settings, start, end),
        datetime.now(UTC),
    )
    with psycopg.connect(settings.pg_conninfo) as conn:
        late_hours = {
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT date_trunc('hour', event_ts, 'UTC') FROM raw.recentchange"
                " WHERE event_id = ANY(%s::uuid[])",
                (list(ids),),
            ).fetchall()
        }
    return [
        {
            "check": c.check_name,
            "status": c.status,
            "failures": c.failures,
            "late_events_hour": any(
                h == datetime.fromisoformat(str(c.details["hour"])) for h in late_hours
            ),
            **{
                k: c.details[k]
                for k in (
                    "warehouse_distinct",
                    "lake_distinct",
                    "missing_in_lake",
                    "extra_in_lake",
                    "lake_duplicates",
                )
            },
        }
        for c in checks
    ]


def run(arm: str, out: Path) -> dict[str, Any]:
    """One arm on a fresh stack (``make bench-late`` brings it up); see the module docstring."""
    import subprocess
    import time

    from bench import stack, verify
    from bench.faults import refuse_future_watermark
    from bench.load import LoadGenerator, LoadPlan, Step, bench_id, new_prefix
    from tributary.config import Settings
    from tributary.ingest.producer import ensure_topic

    settings = Settings()
    conninfo, topic = settings.pg_conninfo, settings.topic
    refuse_future_watermark(conninfo, slack_s=settings.future_skew_s)
    ensure_topic(BOOTSTRAP, topic, settings.topic_partitions)
    stack.wait_healthy("stream")
    sample_ids = [str(e["meta"]["id"]) for e in read_sample(settings.sample_path)]
    doc: dict[str, Any] = {"arm": arm, "started_at": datetime.now(UTC).isoformat()}
    marks: dict[str, float] = {"start": time.time()}

    # 1. First replay, settled; the marts built once so fct_edits is incremental from here.
    log("replay", n=1)
    doc["replay_1"] = replay_sample(settings, settings.produce_rate)
    doc["replay_1"]["settle_s"] = round(wait_settled(conninfo, topic, "warehouse"), 1)
    doc["replay_1"]["missing_in_postgres"] = len(missing_ids(conninfo, sample_ids))
    doc["refresh_after_replay_1"] = warehouse_refresh("after replay 1")
    marks["replay_2"] = time.time()

    # 2. Second replay: every event a copy of one that landed.
    log("replay", n=2)
    doc["replay_2"] = replay_sample(settings, settings.produce_rate)
    doc["replay_2"]["settle_s"] = round(wait_settled(conninfo, topic, "warehouse"), 1)
    marks["load"] = time.time()
    doc["replay_2"]["progress"] = progress_between(conninfo, marks["replay_2"], marks["load"])
    doc["replay_2"]["missing_in_postgres"] = len(missing_ids(conninfo, sample_ids))
    doc["replay_2"]["checks"] = checks_now(settings, f"bench-late-{arm}-replay-2")

    # 3. The load, with passes every 30 s in arm `after`.
    prefix = new_prefix()
    late = {bench_id(prefix, s) for s in LATE_SEQS}
    rejected = {
        "malformed": bench_id(prefix, MALFORMED_SEQ),
        "future": bench_id(prefix, FUTURE_SEQ),
    }
    plan = LoadPlan(
        BOOTSTRAP,
        topic,
        prefix,
        (Step(RATE, EVENTS / RATE + 900),),
        settings.sample_path,
        limit=EVENTS,
        shifts=(*((s, -LATE_BEHIND_S) for s in LATE_SEQS), (FUTURE_SEQ, FUTURE_AHEAD_S)),
        malformed=(MALFORMED_SEQ,),
    )
    if arm == "after":
        subprocess.run(["docker", "rm", "-f", RECONCILER], capture_output=True, check=False)
        code, _out, err = compose_run(
            ["stream", "reconcile", "--every", str(int(PASS_EVERY_S))], detach=True
        )
        if code:
            raise RuntimeError(f"the reconciler did not start: {err}")
    log("load", prefix=prefix)
    gen = LoadGenerator(plan)
    gen.start()
    result = gen.join()
    sent_s = time.time() - gen.started_at
    marks["load_end"] = time.time()
    reconciler_log = ""
    if arm == "after":  # the passes during the load end with it; the final pass follows
        subprocess.run(
            ["docker", "stop", "-t", "180", RECONCILER],
            capture_output=True,
            check=False,
            timeout=240,
        )
        reconciler_log = subprocess.run(
            ["docker", "logs", RECONCILER], capture_output=True, text=True, check=False
        ).stdout
        subprocess.run(["docker", "rm", "-f", RECONCILER], capture_output=True, check=False)
    marks["passes_end"] = time.time()
    pg_settle = wait_settled(conninfo, topic, "warehouse")
    marks["settled"] = time.time()
    doc["load"] = {
        "prefix": prefix,
        "sent": result.sent,
        "delivered": result.delivered,
        "failed": result.failed,
        "seconds": round(sent_s, 1),
        "settle_s": round(pg_settle, 1),
        "late_ids": sorted(late),
        "rejected_ids": rejected,
        "progress": progress_between(conninfo, marks["load"], marks["settled"]),
        "missing_late_before_final_pass": len(missing_ids(conninfo, sorted(late))),
    }
    doc["passes_during_load"] = pass_lines(reconciler_log)

    # 4. One more pass once everything has landed (arm `after`), timed from outside too.
    if arm == "after":
        t0 = time.monotonic()
        code, stdout, stderr = compose_run(["stream", "reconcile"])
        doc["final_pass"] = {
            "exit": code,
            "wall_s": round(time.monotonic() - t0, 1),
            "pass": pass_lines(stdout),
            **({"stderr": stderr} if code else {}),
        }
    marks["final_pass_end"] = time.time()

    # 5. Read-out.
    lake_rows, lake_wait = verify.wait_until(
        lambda: verify.lake_count(settings, prefix),
        result.delivered,
        LAND_TIMEOUT_S,
        settle=LAKE_SETTLE_S,
        poll=5.0,
    )
    stores: dict[str, Any] = {}
    for name, ids in (
        ("postgres", verify.pg_ids(conninfo, prefix)),
        ("iceberg", verify.lake_ids(settings, prefix)),
    ):
        present = {i.lower() for i in ids}
        delivery = verify.compare(prefix, EVENTS, ids)
        stores[name] = {
            **delivery.as_dict(),
            "late_present": len(late & present),
            "malformed_present": rejected["malformed"] in present,
            "future_present": rejected["future"] in present,
            "missing_other_than_late_and_rejected": sorted(
                {bench_id(prefix, s) for s in range(EVENTS)}
                - present
                - late
                - set(rejected.values())
            )[:20],
        }
    stores["iceberg"]["count"], stores["iceberg"]["wait_s"] = lake_rows, round(lake_wait, 1)
    stores["postgres"]["sample_missing"] = len(missing_ids(conninfo, sample_ids))
    doc["stores"] = stores
    doc["progress_total"] = progress_between(conninfo, marks["start"], time.time() + 1)
    doc["passes"] = recorded_passes(conninfo, late)
    doc["checks_final"] = checks_now(settings, f"bench-late-{arm}")
    doc["refresh_final"] = warehouse_refresh("final")
    doc["fct_edits_late"] = fct_edits_counts(conninfo, sorted(late))
    doc["mart_answer"] = mart_answer("SELECT count(*) AS edits FROM marts.fct_edits")
    doc["lake_hours"] = lake_hours(settings, sorted(late))
    doc["marks"] = marks
    doc["environment"] = stack.resources()
    doc["finished_at"] = datetime.now(UTC).isoformat()
    out.mkdir(parents=True, exist_ok=True)
    (out / f"run-{arm}.json").write_text(json.dumps(doc, indent=1, default=str) + "\n")
    if reconciler_log:
        (out / f"reconcile-{arm}.log").write_text(
            "\n".join(line for line in reconciler_log.splitlines() if line.startswith("{")) + "\n"
        )
    return doc


def main(argv: Sequence[str] | None = None) -> int:
    """``python -m bench.late predict`` or ``run --arm before|after``."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    pred = sub.add_parser("predict", help="write prediction.json from the sample alone")
    pred.add_argument("--out", type=Path, default=RESULTS)
    one = sub.add_parser("run", help="one arm on the stack `make bench-late` brought up")
    one.add_argument("--arm", choices=("before", "after"), required=True)
    one.add_argument("--out", type=Path, default=RESULTS)
    args = parser.parse_args(argv)
    if args.command == "run":
        doc = run(args.arm, args.out)
        summary = {k: doc[k] for k in ("stores", "checks_final", "fct_edits_late")}
        print(json.dumps(summary, indent=1, default=str))
        return 0
    if args.command == "predict":
        doc = prediction()
        args.out.mkdir(parents=True, exist_ok=True)
        (args.out / "prediction.json").write_text(json.dumps(doc, indent=1) + "\n")
        print(json.dumps(doc["second_replay"], indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
