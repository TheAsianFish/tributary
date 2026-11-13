"""Render RESULTS.md's benchmark tables from ``bench/results/`` (``make bench-results``).

Every number in the tables is read from a committed raw file; ``--check``
fails if RESULTS.md differs from what the files say (a unit test runs it).
Tables sit between ``<!-- bench:<name> -->`` and ``<!-- /bench:<name> -->``.
"""

from __future__ import annotations

import argparse
import json
import re
import statistics
import sys
from collections.abc import Mapping, Sequence
from functools import partial
from pathlib import Path
from typing import Any

from bench import query_report, rules, skew_report

ROOT = Path(__file__).resolve().parent.parent
RESULTS_MD = ROOT / "RESULTS.md"
BENCH = ROOT / "bench/results/m5-bench"
SCALE = ROOT / "bench/results/m5-scale"
CPU_BOUND = 0.85
"""A host this busy (share of nproc) at the top sustained step is reported as CPU-bound."""
DASH = "\u2013"  # en dash, for empty cells and ranges


def _read(path: Path) -> dict[str, Any]:
    data: dict[str, Any] = json.loads(path.read_text())
    return data


def _n(value: float | None, digits: int = 0) -> str:
    if value is None:
        return DASH
    return f"{value:,.{digits}f}"


def _ms_as_s(value: float | None) -> str:
    return DASH if value is None else f"{value / 1000:.2f} s"


def _median(values: Sequence[float]) -> float | None:
    return statistics.median(values) if values else None


# ---------------------------------------------------------------- make bench


def window_hit(fault: Mapping[str, Any]) -> bool | None:
    """Did a stream kill land where it was aimed? None for a broker kill, or if unchecked.

    The checkpoint read after the kill must show a batch planned and not
    committed (so the restart re-runs it). A ``warehouse-written`` kill must
    have caught the very batch whose write it saw: a kill that missed it and
    landed in the next batch is a miss. A batch seen in flight *before* the kill
    says nothing about where the kill landed, so a fault without an after-kill
    checkpoint is unchecked (None), not a hit.
    """
    if fault.get("service") != "stream" or "offsets_after_kill" not in fault:
        return None
    offsets, commits = fault.get("offsets_after_kill"), fault.get("commits_after_kill")
    if offsets is None or (commits is not None and offsets <= commits):
        return False
    if fault.get("target") == "warehouse-written":
        return offsets == fault.get("batch_id") and commits in (None, offsets - 1)
    return True


def faults_table(results: Path = BENCH) -> str:
    lines = [
        "| Scenario | Run | Kills (window hit) | Acked | PostgreSQL missing / dup | "
        "Iceberg missing / dup | Iceberg batches committed twice | Rows the key absorbed | "
        "Latency p50 / p95 / max, whole run | p50 / p95 after the first 30 s |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    for path in sorted(results.glob("fault-*-rep*.json"), key=_fault_order):
        doc = _read(path)
        faults = doc["faults"]
        kills = DASH
        if faults:
            hits = [h for f in faults if (h := window_hit(f)) is not None]
            if not hits:
                kills = str(len(faults))
            elif len(hits) == len(faults):
                kills = f"{len(faults)} ({sum(hits)})"
            else:
                kills = f"{len(faults)} ({sum(hits)} of {len(hits)} checked)"
        pg, lake, lat = doc["postgres"], doc["iceberg"], doc["latency"]
        steady = doc.get("latency_after_30s") or {}
        lines.append(
            f"| {doc['scenario']} | {doc['rep']} | {kills} | {doc['produced']['delivered']:,} | "
            f"{pg['missing']} / {pg['duplicates']} | {lake['missing']} / {lake['duplicates']} | "
            f"{len(lake['batches_committed_twice'])} | "
            f"{doc['stream_log']['duplicates_skipped']:,} | "
            f"{_ms_as_s(lat['p50_ms'])} / {_ms_as_s(lat['p95_ms'])} / {_ms_as_s(lat['max_ms'])} | "
            f"{_ms_as_s(steady.get('p50_ms'))} / {_ms_as_s(steady.get('p95_ms'))} |"
        )
    return "\n".join(lines)


def kills_table(results: Path = BENCH) -> str:
    """Every stream kill: where it was aimed, what the checkpoint showed, what the stores did."""
    lines = [
        "| Run | Aimed at | Method | Batch | Checkpoint after the kill: offsets / commits | "
        "Window hit | Iceberg snapshots carrying that batch id |",
        "|---|---|---|---|---|---|---|",
    ]
    for path in sorted(results.glob("fault-stream-rep*.json"), key=_fault_order):
        doc = _read(path)
        for fault in doc["faults"]:
            offsets = fault.get("offsets_after_kill")
            commits = fault.get("commits_after_kill")
            ckpt = DASH if offsets is None and commits is None else f"{offsets} / {commits}"
            hit = window_hit(fault)
            lines.append(
                f"| {doc['rep']} | {fault.get('target')} | {fault.get('kill_method', DASH)} | "
                f"{fault.get('batch_id', DASH)} | {ckpt} | {_hit_cell(hit)} | "
                f"{fault.get('snapshots_with_batch_id', DASH)} |"
            )
    return "\n".join(lines)


def _hit_cell(hit: bool | None) -> str:
    return "not checked" if hit is None else ("yes" if hit else "no")


def _fault_order(path: Path) -> tuple[int, int]:
    m = re.match(r"fault-(\w+)-rep(\d+)", path.stem)
    assert m is not None
    return (["none", "stream", "broker"].index(m.group(1)), int(m.group(2)))


def trigger_table(results: Path = BENCH) -> str:
    lines = [
        "| Rate (events/s) | Trigger | p50 | p95 | p99 | Batch time p50 | Stream job CPU (cores) |",
        "|---|---|---|---|---|---|---|",
    ]
    docs = [_read(p) for p in results.glob("tuning-trigger-*.json")]
    for doc in sorted(docs, key=lambda d: (d["rate"], _seconds(d["trigger"]))):
        lat = doc["latency"]
        lines.append(
            f"| {doc['rate']:,.0f} | {doc['trigger']} | {_ms_as_s(lat['p50_ms'])} | "
            f"{_ms_as_s(lat['p95_ms'])} | {_ms_as_s(lat['p99_ms'])} | "
            f"{_n(doc['batch_ms_p50'])} ms | {_n(doc['cpu_cores'].get('stream'), 2)} |"
        )
    return "\n".join(lines)


def _seconds(interval: str) -> float:
    value, unit = interval.split()
    return float(value) * (60 if unit.startswith("minute") else 1)


def cap_table(results: Path = BENCH) -> str:
    lines = [
        "| `maxOffsetsPerTrigger` | Ceiling at a 5 s trigger | Batches | Longest batch | "
        "300,000-event backlog cleared in | Effective rate | Stream job peak memory |",
        "|---|---|---|---|---|---|---|",
    ]
    docs = [_read(p) for p in results.glob("tuning-cap-*.json")]
    for doc in sorted(docs, key=lambda d: d["cap"]):
        peak = doc.get("memory_peak_bytes")
        events = int(doc["fill"]["events"])
        lines.append(
            f"| {doc['cap']:,} | {doc['ceiling_per_s']:,.0f} events/s | {doc['batches']} | "
            f"{_ms_as_s(doc['batch_ms_max'])} | {doc['backlog_s']} s | "
            f"{events / float(doc['backlog_s']):,.0f} events/s | "
            f"{DASH if peak is None else f'{peak / 2**30:.2f} GiB'} |"
        )
    return "\n".join(lines)


def freshness_table(results: Path = BENCH) -> str:
    path = results / "freshness.json"
    if not path.exists():
        return "No freshness run recorded yet."
    doc = _read(path)
    lines = ["| | |", "|---|---|"]
    lines.append(
        f"| `warehouse_refresh` runs with every check passing | "
        f"{doc['refresh_runs_all_pass']} of {doc['refresh_runs']} |"
    )
    lines.append(f"| Freshness-only polls (no new rows) | {doc['freshness_polls']} |")
    for target, lag in sorted(doc["lag_seconds"].items()):
        lines.append(
            f"| Freshness lag of `{target}` at check time, p50 / p95 / max | "
            f"{_n(lag['p50'], 1)} / {_n(lag['p95'], 1)} / {_n(lag['max'], 1)} s "
            f"({lag['checks']} checks) |"
        )
    gap = doc["rebuild_gap_s"]
    lines.append(
        f"| Time between rebuilds of `marts.fct_edits`, p50 / p95 / max | "
        f"{_n(gap['p50'], 0)} / {_n(gap['p95'], 0)} / {_n(gap['max'], 0)} s "
        f"({doc['fct_edits_rebuilds']} rebuilds) |"
    )
    statuses = ", ".join(f"{k} {v}" for k, v in sorted(doc["statuses"].items()))
    lines.append(f"| Check rows recorded | {doc['quality_rows']} ({statuses}) |")
    return "\n".join(lines)


# ---------------------------------------------------------- make bench-scale


def _reps(results: Path, p: int, iceberg: bool = False) -> list[dict[str, Any]]:
    tag = "-iceberg" if iceberg else ""
    return [_read(path) for path in sorted(results.glob(f"p{p}{tag}-rep*.json"))]


def _partitions(results: Path) -> list[int]:
    found = {
        int(m.group(1))
        for p in results.glob("p*-stages.json")
        if (m := re.match(r"p(\d+)-", p.name))
    }
    return sorted(found)


def _top_step(rep: Mapping[str, Any]) -> dict[str, Any] | None:
    """The highest sustained step of a repetition."""
    ok = [s for s in rep["steps"] if s["sustained"]]
    return max(ok, key=lambda s: s["rate"]) if ok else None


def _spread(values: Sequence[float]) -> str:
    if not values:
        return DASH
    med = statistics.median(values)
    if len(values) == 1:
        return f"{med:,.0f}"
    return f"{med:,.0f} ({min(values):,.0f}{DASH}{max(values):,.0f})"


def attribution(stages: Mapping[str, Any]) -> rules.Attribution:
    drains = stages["drains"]
    return rules.attribute(
        noop=float(drains["noop"]["rows_per_s"]),
        python=float(drains["python"]["rows_per_s"]),
        sink=float(stages["sink"]["rows_per_s"]),
        broker=stages["broker"]["consumer"].get("records_per_s"),
        pipeline=float(drains["postgres"]["rows_per_s"]),
    )


def scale_table(results: Path = SCALE) -> str:
    nproc = int(_read(results / "environment.json")["resources"]["nproc"])
    lines = [
        "| P | Sustained events/s, median (range) | p95 at 80% of it, median | "
        "Full drain | Spark `noop` | Spark + Python | PostgreSQL sink alone | "
        "Broker (consume / produce) | Bottleneck |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for p in _partitions(results):
        stages = _read(results / f"p{p}-stages.json")
        lines.append(_scale_row(str(p), stages, _reps(results, p), nproc))
        if p == 3 and _reps(results, p, iceberg=True):
            lines.append(_scale_row("3 + Iceberg", stages, _reps(results, p, iceberg=True), nproc))
    return "\n".join(lines)


def _scale_row(
    label: str, stages: Mapping[str, Any], reps: Sequence[Mapping[str, Any]], nproc: int
) -> str:
    drains = stages["drains"]
    sustained = [float(r["sustained_per_s"]) for r in reps if r["sustained_per_s"]]
    p95 = [
        float(r["latency"]["p95_ms"])
        for r in reps
        if r.get("latency") and r["latency"].get("p95_ms") is not None
    ]
    att = attribution(stages)
    broker = stages["broker"]
    tops = [s for r in reps if (s := _top_step(r)) is not None]
    host = _median([s["host_cores"] for s in tops if s.get("host_cores") is not None])
    bottleneck = att.bottleneck
    if host is not None and host >= CPU_BOUND * nproc:
        bottleneck += f", CPU-bound (host {host:.1f} of {nproc} cores busy)"
    if "Iceberg" in label:
        stage_cells = f"{DASH} | {DASH} | {DASH} | {DASH} | {DASH} | see below"
    else:
        stage_cells = (
            f"{_n(drains['postgres']['rows_per_s'])} | {_n(drains['noop']['rows_per_s'])} | "
            f"{_n(drains['python']['rows_per_s'])} | {_n(stages['sink']['rows_per_s'])} | "
            f"{_n(broker['consumer'].get('records_per_s'))} / "
            f"{_n(broker.get('producer_records_per_s'))} | {bottleneck}"
        )
    return f"| {label} | {_spread(sustained)} | {_ms_as_s(_median(p95))} | {stage_cells} |"


def evidence_table(results: Path = SCALE) -> str:
    lines = [
        "| P | Per-event time: JVM / transfer / database (µs) | Predicted vs measured drain | "
        "Database share of the sink's time | CPU at the top sustained step: "
        "stream / PostgreSQL / whole host (cores of nproc) | PostgreSQL backends, "
        "busy share and top wait |",
        "|---|---|---|---|---|---|",
    ]
    for p in _partitions(results):
        stages = _read(results / f"p{p}-stages.json")
        lines.append(_evidence_row(str(p), stages, _reps(results, p)))
        if p == 3 and _reps(results, p, iceberg=True):
            lines.append(_evidence_row("3 + Iceberg", stages, _reps(results, p, iceberg=True)))
    return "\n".join(lines)


def _evidence_row(label: str, stages: Mapping[str, Any], reps: Sequence[Mapping[str, Any]]) -> str:
    att = attribution(stages)
    sinks = [s for s in stages["drains"]["postgres"]["sink_batches"] if s.get("rows")][1:]
    db_share = (
        sum(float(s["db_ms_max"]) for s in sinks) / sum(float(s["sink_ms"]) for s in sinks)
        if sinks
        else None
    )
    tops = [s for r in reps if (s := _top_step(r)) is not None]
    stream = _median([s["cpu_cores"].get("stream", 0.0) for s in tops])
    postgres = _median([s["cpu_cores"].get("postgres", 0.0) for s in tops])
    host = _median([s["host_cores"] for s in tops if s.get("host_cores") is not None])
    activity = _pg_summary(tops)
    measured = float(stages["drains"]["postgres"]["rows_per_s"])
    att_cell = (
        f"{att.jvm_us:.1f} / {att.transfer_us:.1f} / {att.database_us:.1f}"
        if "Iceberg" not in label
        else DASH
    )
    pred_cell = f"{att.predicted_per_s:,.0f} vs {measured:,.0f}" if "Iceberg" not in label else DASH
    share_cell = DASH if db_share is None or "Iceberg" in label else f"{db_share:.0%}"
    return (
        f"| {label} | {att_cell} | {pred_cell} | {share_cell} | "
        f"{_n(stream, 2)} / {_n(postgres, 2)} / {_n(host, 2)} | {activity} |"
    )


def _pg_summary(steps: Sequence[Mapping[str, Any]]) -> str:
    ticks = sum(int(s["pg_activity"]["ticks"]) for s in steps)
    if not ticks:
        return DASH
    waits: dict[str, int] = {}
    for s in steps:
        for k, v in s["pg_activity"]["active_backend_s"].items():
            waits[k] = waits.get(k, 0) + int(v)
    active = sum(waits.values())
    idle = sum(int(s["pg_activity"]["idle_backend_s"]) for s in steps)
    busy = active / (active + idle) if active + idle else 0.0
    top = max(waits, key=lambda k: waits[k]) if waits else DASH
    return f"{busy:.0%} busy; {top} {waits.get(top, 0) / active:.0%}" if active else "idle"


def iceberg_table(results: Path = SCALE) -> str:
    plain, lake = _reps(results, 3), _reps(results, 3, iceberg=True)
    if not lake:
        return "No Iceberg run recorded yet."
    lines = [
        "| P = 3 | Sustained events/s, median | p95 at 80% | Stream job CPU at the top step | "
        "Lake batch time at the top step, max |",
        "|---|---|---|---|---|",
    ]
    for label, reps in (("warehouse only", plain), ("warehouse + Iceberg", lake)):
        sustained = [float(r["sustained_per_s"]) for r in reps if r["sustained_per_s"]]
        p95 = [float(r["latency"]["p95_ms"]) for r in reps if r.get("latency")]
        tops = [s for r in reps if (s := _top_step(r)) is not None]
        cpu = _median([s["cpu_cores"].get("stream", 0.0) for s in tops])
        lake_ms = [
            float(b["trigger_ms"])
            for s in tops
            for b in s["queries"].get("lake", {}).get("batches_in_step", [])
        ]
        lines.append(
            f"| {label} | {_spread(sustained)} | {_ms_as_s(_median(p95))} | {_n(cpu, 2)} | "
            f"{_ms_as_s(max(lake_ms)) if lake_ms else DASH} |"
        )
    return "\n".join(lines)


def environment_table(results: Path = SCALE) -> str:
    path = results / "environment.json"
    if not path.exists():
        return "No run recorded yet."
    env = _read(path)["resources"]
    limits = ", ".join(
        f"{name} {c['mem_limit_bytes'] / 2**30:g} GiB"
        for name, c in sorted(env["containers"].items())
        if c.get("mem_limit_bytes")
    )
    cpus = [n for n, c in env["containers"].items() if c.get("nano_cpus")]
    cpu_limits = ", ".join(cpus) or "none (every container may use all cores)"
    return (
        f"{env['nproc']} vCPU ({env['cpu_model']}), {env['mem_total_kb'] / 2**20:.1f} GiB RAM, "
        f"Linux {env['kernel']}, Docker {env['docker']}, Compose {env['compose']}. "
        f"Memory limits: {limits}. CPU limits: {cpu_limits}."
    )


def render_all() -> dict[str, str]:
    tables: dict[str, str] = {}
    if BENCH.exists():
        tables.update(
            faults=faults_table(),
            kills=kills_table(),
            trigger=trigger_table(),
            cap=cap_table(),
            freshness=freshness_table(),
        )
    if SCALE.exists():
        tables.update(
            scale=scale_table(),
            evidence=evidence_table(),
            iceberg=iceberg_table(),
            environment=environment_table(),
        )
    tables.update(query_report.render())
    tables.update(skew_report.render())
    return tables


def _splice(table: str, match: re.Match[str]) -> str:
    return match.group(1) + table + "\n" + match.group(2)


def apply(text: str, tables: Mapping[str, str]) -> str:
    for name, table in tables.items():
        pattern = re.compile(
            rf"(<!-- bench:{name} -->\n).*?(<!-- /bench:{name} -->)", flags=re.DOTALL
        )
        if not pattern.search(text):
            raise ValueError(f"RESULTS.md has no <!-- bench:{name} --> block")
        text = pattern.sub(partial(_splice, table), text)
    return text


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--check", action="store_true", help="fail if RESULTS.md is stale")
    args = parser.parse_args(argv)
    current = RESULTS_MD.read_text(encoding="utf-8")
    rendered = apply(current, render_all())
    if args.check:
        if rendered != current:
            print("RESULTS.md is stale: run `make bench-results`", file=sys.stderr)
            return 1
        print("RESULTS.md matches bench/results/")
        return 0
    RESULTS_MD.write_text(rendered, encoding="utf-8")
    print("RESULTS.md benchmark tables rendered from bench/results/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
