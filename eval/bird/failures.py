"""Failure analysis: wrong answers of one variant, and the taxonomy they were sorted into.

    uv run python -m eval.bird.failures --variant NAME [--dump FILE]

``--dump`` writes every wrong answer of the variant (question, evidence, gold
SQL, predicted SQL, status, error, row counts, and the rows each query
returned, from the recorded executions) as JSON, which is what a person reads
to label them. The labels live in ``eval/results/bird-failures.json``: one
entry per (track, question id) with a category from :data:`CATEGORIES` and a
one-line note saying what went wrong. :func:`failures_table` renders the
counts into RESULTS.md and refuses labels that do not match the run (a
labelled answer that is not wrong in the report, or a category not in the
list), so the taxonomy cannot drift away from the verdicts it describes.

The categories were fixed before labelling. Each wrong answer gets the one
that explains it first: a query that both joins badly and returns an extra
column is a join error, because fixing the column alone would not make it
right.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from eval.bird import data
from eval.bird.recorded import execution_key, load_executions
from eval.bird.run import EXECUTIONS as EXECUTIONS_PATH
from tributary.config import REPO_ROOT

LABELS = REPO_ROOT / "eval" / "results" / "bird-failures.json"

# (key, label, what it covers), in the order the table prints them.
CATEGORIES: tuple[tuple[str, str, str], ...] = (
    ("refusal", "Refused", "set answerable to false on an answerable question"),
    ("no_query", "No query ran", "every attempt was blocked or failed in PostgreSQL"),
    ("schema", "Wrong table or column", "read a column or table that does not hold the asked fact"),
    (
        "value",
        "Wrong literal",
        "filtered on a value not stored in that form (case, spelling, code)",
    ),
    ("join", "Wrong join", "a missing, extra or wrongly keyed join"),
    ("evidence", "Evidence not followed", "ignored or misapplied the formula or mapping given"),
    ("aggregation", "Wrong aggregation", "wrong aggregate, grouping, DISTINCT or count target"),
    ("arithmetic", "Arithmetic or type", "integer division, percentage scale, rounding, casts"),
    ("ordering", "Ordering or limit", "wrong sort direction, tie handling or row limit"),
    ("shape", "Output shape", "the right facts with extra, missing or reordered columns"),
    ("gold", "Gold is questionable", "the gold answer is wrong or the question is ambiguous"),
)
CATEGORY_KEYS = tuple(c[0] for c in CATEGORIES)


def wrong_answers(report: dict[str, Any], variant: str) -> list[dict[str, Any]]:
    """Every verdict of ``variant`` (with repairs) that is not correct, by track then id."""
    # Any: verdicts are JSON objects written by eval.bird.run.
    out = []
    tracks = report["variants"][variant]
    for track in ("official", "corrected"):
        if track not in tracks:
            continue
        for v in tracks[track]["verdicts"]:
            if not v["correct"]:
                out.append({"track": track, **v})
    return out


def dump(report: dict[str, Any], variant: str, executions_path: Path) -> list[dict[str, Any]]:
    """The wrong answers with what a labeller needs: question, gold, both results."""
    executions = load_executions(executions_path)
    questions = {(t, q.id): q for t in data.TRACKS for q in data.questions(t)}
    items = []
    for w in wrong_answers(report, variant):
        q = questions[(w["track"], w["id"])]
        gold = executions.get(execution_key(f"gold/{q.db_id}", q.sql), {})
        predicted = executions.get(execution_key(q.db_id, w["sql"] or ""), {})
        items.append(
            {
                "track": w["track"],
                "id": w["id"],
                "db_id": q.db_id,
                "difficulty": q.difficulty,
                "question": q.question,
                "evidence": q.evidence,
                "gold_sql": q.sql,
                "predicted_sql": w["sql"],
                "status": w["status"],
                "attempts": w["attempts"],
                "error": w["error"],
                "gold_rows": _rows(gold),
                "predicted_rows": _rows(predicted),
            }
        )
    return items


def _rows(record: dict[str, Any], limit: int = 8) -> Any:
    # Any: a recorded execution document (eval.bird.recorded).
    if not record:
        return None
    if "error" in record:
        return {"error": record["error"]}
    result = record["result"]
    return {"columns": result["columns"], "rows": result["rows"][:limit], "n": len(result["rows"])}


def load_labels(path: Path = LABELS) -> dict[str, Any]:
    # Any: the labels document (see the module doc).
    loaded: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return loaded


def check_labels(labels: dict[str, Any], report: dict[str, Any]) -> list[str]:
    """Labels that do not describe a wrong answer of the labelled variant."""
    problems = []
    wrong = {(w["track"], w["id"]) for w in wrong_answers(report, labels["variant"])}
    seen = set()
    for item in labels["labels"]:
        key = (item["track"], item["id"])
        if key in seen:
            problems.append(f"{key}: labelled twice")
        seen.add(key)
        if key not in wrong:
            problems.append(f"{key}: not a wrong answer of {labels['variant']} in the report")
        if item["category"] not in CATEGORY_KEYS:
            problems.append(f"{key}: unknown category {item['category']!r}")
        if not str(item.get("note", "")).strip():
            problems.append(f"{key}: no note")
    return problems


def failures_table(labels: dict[str, Any], report: dict[str, Any]) -> str:
    problems = check_labels(labels, report)
    if problems:
        raise ValueError("failure labels do not match the run: " + "; ".join(problems[:5]))
    items = labels["labels"]
    lines = [
        "| Category | What it covers | Wrong answers | Share | Questions |",
        "|---|---|---:|---:|---|",
    ]
    total = len(items)
    for key, label, covers in CATEGORIES:
        hits = [i for i in items if i["category"] == key]
        if not hits:
            continue
        ids = ", ".join(
            f"{i['id']}" + ("" if i["track"] == "official" else " (c)")
            for i in sorted(hits, key=lambda i: (i["track"] != "official", i["id"]))
        )
        lines.append(
            f"| {label} | {covers} | {len(hits)} | {100 * len(hits) / total:.0f}% | {ids} |"
        )
    lines.append(f"| **Total** | | **{total}** | 100% | |")
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--variant", required=True)
    parser.add_argument("--executions", type=Path, default=EXECUTIONS_PATH)
    parser.add_argument("--dump", type=Path, default=None)
    args = parser.parse_args(argv)
    report = json.loads(args.report.read_text(encoding="utf-8"))
    items = dump(report, args.variant, args.executions)
    if args.dump:
        args.dump.write_text(json.dumps(items, indent=2, ensure_ascii=False, default=str) + "\n")
    print(f"{len(items)} wrong answers of {args.variant}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
