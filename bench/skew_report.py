"""RESULTS.md's M11 tables, rendered from ``bench/results/m11-skew/`` by ``bench.report``."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

RESULTS = Path(__file__).resolve().parent / "results" / "m11-skew"


def _read(path: Path) -> dict[str, Any]:
    doc: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return doc


def _n(value: int) -> str:
    return f"{value:,}"


def _yes(flag: bool) -> str:
    return "yes" if flag else "no"


def skew_table(results: Path = RESULTS) -> str:
    """Loss against skew: prediction beside measurement, before and after the fix."""
    prediction = _read(results / "prediction.json")
    before = {r["skew_s"]: r for r in _read(results / "sweep-before.json")["runs"]}
    after = {r["skew_s"]: r for r in _read(results / "sweep-after.json")["runs"]}
    planned = {r["skew_s"]: r for r in prediction["arms"]["before"]["skews"]}
    lines = [
        "| Poison skew X | PLAN estimate | Before: predicted lost | Before: measured lost"
        " (dropped by watermark) | Same ids | After: poison | After: lost |",
        "|---:|---:|---:|---:|:-:|:--|---:|",
    ]
    for x, b in before.items():
        a = after[x]
        verdict = "rejected (future)" if a["future_rows"] else "accepted"
        lines.append(
            f"| {_n(x)} s | {_n(planned[x]['plan_formula'])} | {_n(b['predicted_lost'])}"
            f" | {_n(b['lost_real'])} ({_n(b['dropped_by_watermark'])})"
            f" | {_yes(b['same_ids_as_predicted'] and a['same_ids_as_predicted'])}"
            f" | {verdict} | {_n(a['lost_real'])} |"
        )
    return "\n".join(lines)


def _clock(iso: str | None) -> str:
    return (
        "-"
        if iso is None
        else datetime.fromisoformat(iso.replace("Z", "+00:00")).strftime("%H:%M:%S")
    )


def incident_table(results: Path = RESULTS) -> str:
    """The M5 incident replayed: each phase with the old bound and the new one."""
    before = _read(results / "incident-before.json")["phases"]
    after = _read(results / "incident-after.json")["phases"]
    lines = [
        "| Phase | Sent at | Events | Event times | Ahead of record time (max)"
        " | Landed, 1-day bound | Watermark after | Landed, 300 s bound |",
        "|:--|:-:|---:|:-:|---:|---:|:-:|---:|",
    ]
    for b, a in zip(before, after, strict=True):
        lines.append(
            f"| {b['phase']} | {_clock(b['sent_at'])} | {_n(b['events'])}"
            f" | {_clock(b['event_time_min'])} to {_clock(b['event_time_max'])}"
            f" | {_n(b['ahead_of_record_time'])} ({b['max_ahead_s'] / 3600:.2f} h)"
            f" | {_n(b['landed'])} | {_clock(b['watermark_after'])} | {_n(a['landed'])} |"
        )
    return "\n".join(lines)


def poison_table(results: Path = RESULTS) -> str:
    """The Compose run, both sinks, before and after."""
    lines = [
        "| Arm | Bound | Sink | Landed of 50,000 | Real events lost | Poison landed"
        " | Dropped by watermark | Rejected as future | `stream_loss` check |",
        "|:--|---:|:--|---:|---:|:-:|---:|---:|:--|",
    ]
    for arm in ("before", "after"):
        path = results / f"compose-{arm}.json"
        if not path.exists():
            continue
        doc = _read(path)
        bound = doc["stream_max_future_skew_s"]
        bound = "300 (default)" if bound == "default" else bound
        checks = {c["check"]: c for c in doc["checks"]}
        for sink, query in (("postgres", "warehouse"), ("iceberg", "lake")):
            s = doc[sink]
            progress = doc["stream_progress"].get(query, {})
            check = checks.get(f"stream_loss_{query}")
            status = "-" if check is None else check["status"]
            lines.append(
                f"| {arm} | {bound} | {'PostgreSQL' if sink == 'postgres' else 'Iceberg'}"
                f" | {_n(s['distinct'])} | {_n(s['lost_real'])} | {_yes(s['poison_landed'])}"
                f" | {_n(progress.get('dropped_by_watermark', 0))}"
                f" | {_n(progress.get('future_rows', 0))} | {status} |"
            )
    return "\n".join(lines)


def render(results: Path = RESULTS) -> dict[str, str]:
    if not (results / "sweep-after.json").exists():
        return {}
    tables = {"skew": skew_table(results)}
    if (results / "incident-after.json").exists():
        tables["incident"] = incident_table(results)
    if (results / "compose-before.json").exists() or (results / "compose-after.json").exists():
        tables["poison"] = poison_table(results)
    return tables
