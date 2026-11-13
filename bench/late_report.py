"""RESULTS.md's M12 tables, rendered from ``bench/results/m12-late/`` (``make bench-results``)."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from bench.late import RESULTS

ARMS = ("before", "after")


def _read(path: Path) -> dict[str, Any]:
    doc: dict[str, Any] = json.loads(path.read_text())
    return doc


def _n(value: object) -> str:
    return f"{value:,}" if isinstance(value, int) and not isinstance(value, bool) else str(value)


def _check(checks: list[Mapping[str, Any]], name: str) -> str:
    for c in checks:
        if c["check"] == name:
            return str(c["status"])
    return "-"


def _provenance(doc: Mapping[str, Any]) -> str:
    """The answer's provenance line for its table and any warning on it, as
    ``tributary sql`` prints them."""
    text = str(doc["mart_answer"].get("text", ""))
    lines = [ln.strip().removeprefix("- ") for ln in text.splitlines()]
    out = [ln for ln in lines if ln.startswith("marts.fct_edits:")]
    return " / ".join(f"`{ln}`" for ln in out) or "-"


def _arm(doc: Mapping[str, Any]) -> dict[str, str]:
    """One arm's column of the main table."""
    second = doc["replay_2"]
    load = doc["load"]
    pg, lake = doc["stores"]["postgres"], doc["stores"]["iceberg"]
    passes = doc["passes"]
    during = [p for p in passes if p["id"] not in {f["pass_id"] for f in _final(doc)}]
    final = _final(doc)
    in_flight = sum(len(p["missing_other"]) for p in passes)
    hours = [h for h in doc["lake_hours"] if h["status"] != "pass"]
    gap = (
        "; ".join(
            f"{h['check'].split(':', 1)[1]}: {h['missing_in_lake']} missing, "
            f"{h['extra_in_lake']} extra"
            for h in hours
        )
        or "every hour equal"
    )
    edits = doc["fct_edits_late"]
    return {
        "dropped_2": _n(second["progress"]["warehouse"]["dropped_by_watermark"]),
        "dropped_2_lake": _n(second["progress"].get("lake", {}).get("dropped_by_watermark", 0)),
        "lost_2": _n(second["missing_in_postgres"]),
        "check_2": _check(second["checks"], "stream_loss_warehouse"),
        "dropped_load": _n(load["progress"]["warehouse"]["dropped_by_watermark"]),
        "rejected": (
            f"{load['progress']['warehouse']['malformed_rows']} malformed,"
            f" {load['progress']['warehouse']['future_rows']} future"
        ),
        "rejected_landed": _n(
            sum(int(pg[k]) + int(lake[k]) for k in ("malformed_present", "future_present"))
        ),
        "passes": f"{len(during)} during, {len(final)} after" if passes else "none",
        "in_flight": _n(in_flight) if passes else "-",
        "found": _n(sum(int(p["missing_late"]) for p in passes)) if passes else "-",
        "late_pg": f"{len(load['late_ids']) - pg['late_present']} of {len(load['late_ids'])}",
        "other_lost": _n(len(pg["missing_other_than_late_and_rejected"]) + pg["sample_missing"]),
        "check_end": _check(doc["checks_final"], "stream_loss_warehouse"),
        "check_lake": _check(doc["checks_final"], "stream_loss_lake"),
        "fct": f"{edits['in_fct_edits']} of {edits['edits_in_raw']}",
        "provenance": _provenance(doc),
        "pass_s": (
            f"{final[0]['seconds']:.1f} s over {_n(final[0]['records'])} records"
            f" ({doc['final_pass']['wall_s']:.1f} s with the container and JVM start)"
            if final
            else "-"
        ),
        "late_lake": f"{lake['late_present']} of {len(load['late_ids'])}",
        "lake_gap": gap,
    }


def _final(doc: Mapping[str, Any]) -> list[dict[str, Any]]:
    final = doc.get("final_pass")
    return list(final["pass"]) if final else []


ROWS = (
    ("Second replay: rows dropped by the watermark (warehouse)", "dropped_2", "35,554"),
    ("Second replay: rows dropped by the watermark (lake)", "dropped_2_lake", "-"),
    ("Second replay: sample events missing from PostgreSQL", "lost_2", "0"),
    ("`stream_loss_warehouse` after the second replay, before any pass", "check_2", "fail"),
    ("Load: rows dropped by the watermark (warehouse)", "dropped_load", "100"),
    ("Load: records rejected", "rejected", "1 malformed, 1 future"),
    ("Rejected records landed (both stores)", "rejected_landed", "0"),
    ("Reconciliation passes", "passes", "-"),
    ("In-flight events reported missing, over every pass", "in_flight", "0"),
    ("Late events found missing, over every pass", "found", "100"),
    ("Late events missing from PostgreSQL at the end", "late_pg", "100 before, 0 after"),
    ("Any other event missing from PostgreSQL", "other_lost", "0"),
    ("`stream_loss_warehouse` at the end", "check_end", "fail before, pass after"),
    ("`stream_loss_lake` at the end", "check_lake", "-"),
    ("Late edits in `marts.fct_edits` after the next refresh", "fct", "all, after"),
    ("Provenance of `SELECT count(*) FROM marts.fct_edits`", "provenance", "-"),
    ("The final pass, whole topic", "pass_s", "under 60 s"),
    ("Late events in Iceberg (never repaired)", "late_lake", "-"),
    ("Per-hour PostgreSQL against Iceberg", "lake_gap", "-"),
)


def late_table(results: Path = RESULTS) -> str:
    docs = {arm: _read(results / f"run-{arm}.json") for arm in ARMS}
    cols = {arm: _arm(doc) for arm, doc in docs.items()}
    lines = ["| | Predicted | before | after |", "|:--|:--|:--|:--|"]
    for label, key, predicted in ROWS:
        lines.append(f"| {label} | {predicted} | {cols['before'][key]} | {cols['after'][key]} |")
    return "\n".join(lines)


def passes_table(results: Path = RESULTS) -> str:
    doc = _read(results / "run-after.json")
    final = {f["pass_id"] for f in _final(doc)}
    lines = [
        "| Pass | When | Records read | Landed | Rejected | Missing (late / other)"
        " | Written back | Left missing | Seconds |",
        "|--:|:--|--:|--:|--:|:--|--:|--:|--:|",
    ]
    for p in doc["passes"]:
        when = "after the load" if p["id"] in final else "during the load"
        lines.append(
            f"| {p['id']} | {when} | {_n(p['records'])} | {_n(p['landed'])}"
            f" | {p['malformed'] + p['future']} | {p['missing_late']} / {len(p['missing_other'])}"
            f" | {p['recovered']} | {p['left_missing']} | {p['seconds']:.1f} |"
        )
    return "\n".join(lines)


def render(results: Path = RESULTS) -> dict[str, str]:
    if not all((results / f"run-{arm}.json").exists() for arm in ARMS):
        return {}
    return {"late": late_table(results), "late-passes": passes_table(results)}
