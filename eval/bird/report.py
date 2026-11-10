"""RESULTS.md's BIRD tables, rendered from the committed reports under ``eval/results/``.

    uv run python -m eval.bird.report [--check]

Each table sits between ``<!-- bird:NAME -->`` and ``<!-- /bird:NAME -->`` in
RESULTS.md and is replaced wholesale, so a number in those tables is always
the one in a raw report, never typed by hand. ``--check`` exits 1 if
RESULTS.md is not what the reports render to (CI runs it).

Tables:

- ``gold``: the load and the gold verification (``bird-gold.json``);
- ``link``: schema-linking recall and precision (``bird-link.json``);
- ``ex``: execution accuracy per model, variant and track
  (``bird-<subset>-<provider>-<model>.json``);
- ``ledger``: what each recorded run cost, and the estimated cost of every
  live command that has not been run, priced from the measured token counts.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Sequence
from functools import partial
from pathlib import Path
from typing import Any

from eval.bird.run import ESTIMATE_MODELS, estimate_cost
from tributary.config import REPO_ROOT

RESULTS_MD = REPO_ROOT / "RESULTS.md"
RESULTS = REPO_ROOT / "eval" / "results"
_RUN_REPORT = re.compile(r"^bird-(?P<subset>[a-z0-9]+)-(?P<provider>ollama|anthropic)-.+\.json$")


def _load(name: str) -> dict[str, Any]:
    # Any: a JSON report written by this package.
    loaded: dict[str, Any] = json.loads((RESULTS / name).read_text(encoding="utf-8"))
    return loaded


def run_reports(results: Path = RESULTS) -> list[tuple[str, dict[str, Any]]]:
    return [
        (p.name, json.loads(p.read_text(encoding="utf-8")))
        for p in sorted(results.glob("bird-*.json"))
        if _RUN_REPORT.match(p.name)
    ]


DASH = "\u2013"  # en dash, for ranges and empty cells


def _pct(x: float) -> str:
    return f"{100 * x:.1f}%"


def gold_table(gold: dict[str, Any]) -> str:
    o, c = gold["official"], gold["corrected"]
    status = c["by_status"]
    dropped = ", ".join(
        f"{label} {status[key]}"
        for key, label in (
            ("postgres_error", "fails on PostgreSQL"),
            ("mismatch", "different rows from SQLite"),
            ("sqlite_error", "SQLite timeout or error"),
            ("transpile_error", "does not transpile"),
        )
        if status.get(key)
    )
    verified, differ = c["verified"], c["differs_from_official"]
    return "\n".join(
        [
            "| | |",
            "|---|---|",
            f"| Tables loaded | {sum(gold['load']['tables'].values())} in "
            f"{len(gold['load']['tables'])} schemas (source: {gold['load']['source']}) |",
            f"| Official gold queries that execute | **{o['executed']} / {o['total']}** "
            f"({o['empty']} return no rows) |",
            f"| Corrected gold transpiled and verified on both engines | **{verified} / "
            f"{c['total']}**; dropped: {dropped} |",
            f"| Verified corrected gold whose rows differ from the official gold's | "
            f"**{differ} / {verified} ({_pct(differ / verified)})** |",
        ]
    )


def link_table(link: dict[str, Any]) -> str:
    lines = [
        "| Strategy | k | Recall | All gold tables found | Precision | Tables shown |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for r in link["results"]:
        k = "all" if r["k"] is None else str(r["k"])
        lines.append(
            f"| `{r['strategy']}` | {k} | {r['recall']:.3f} | {_pct(r['all_found'])} "
            f"({r['all_found_count']}/{r['questions']}) | {r['precision']:.3f} | "
            f"{r['tables_shown']:.2f} |"
        )
    for strategy in link["unavailable"]:
        lines.append(f"| `{strategy}` | | not implemented yet | | | |")
    return "\n".join(lines)


def ex_table(reports: Sequence[tuple[str, dict[str, Any]]]) -> str:
    lines = [
        "| Model | Variant | Track | EX | 95% interval | Simple | Moderate | Challenging "
        "| Model calls | Tokens in / out | Model time per question, p50 / p95 |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for _, report in reports:
        model = f"{report['model']} ({report['provider']})"
        for variant, tracks in report["variants"].items():
            for repairs in ("r0", "r2"):
                for track, entry in tracks.items():
                    s = entry[repairs]
                    lo, hi = s["ex_wilson95"]
                    d = s["by_difficulty"]

                    def cell(key: str, d: dict[str, Any] = d) -> str:
                        e = d.get(key)
                        return f"{e['correct']}/{e['total']}" if e else DASH

                    lines.append(
                        f"| {model} | `{variant}-{repairs}` | {track} | "
                        f"**{s['correct']}/{s['total']}** ({_pct(s['ex'])}) | "
                        f"{_pct(lo)}{DASH}{_pct(hi)} | {cell('simple')} | {cell('moderate')} | "
                        f"{cell('challenging')} | {s['model_calls']} | "
                        f"{s['input_tokens']:,} / {s['output_tokens']:,} | "
                        f"{s['model_ms_p50'] / 1000:.1f} s / {s['model_ms_p95'] / 1000:.1f} s |"
                    )
    return "\n".join(lines)


# Live commands that have not been run, sized in questions and variants.
LIVE_COMMANDS: tuple[tuple[str, str, int, int], ...] = (
    (
        "dev50, both variants, both tracks",
        "make eval PROVIDER=anthropic MODEL={model} RECORD=1",
        50,
        2,
    ),
    (
        "all 500, both variants, both tracks",
        "make eval PROVIDER=anthropic MODEL={model} SUBSET=all RECORD=1",
        500,
        2,
    ),
)


def ledger_table(reports: Sequence[tuple[str, dict[str, Any]]]) -> str:
    lines = [
        "| Run | Variant | Model calls | Tokens in / out | Cost | Same tokens on "
        + " / ".join(f"`{m}`" for m in ESTIMATE_MODELS)
        + " |",
        "|---|---|---:|---:|---:|---:|",
    ]
    per_question: list[tuple[int, int]] = []
    for name, report in reports:
        questions = sum(
            sum(1 for v in entry["verdicts"] if not v["reused"])
            for tracks in report["variants"].values()
            for entry in tracks.values()
        )
        tokens = [0, 0]
        for row in report["ledger"]:
            if row["variant"].endswith("-r2"):
                tokens[0] += row["input_tokens"]
                tokens[1] += row["output_tokens"]
            estimates = " / ".join(
                DASH if row["estimate_usd"][m] is None else f"${row['estimate_usd'][m]:.2f}"
                for m in ESTIMATE_MODELS
            )
            lines.append(
                f"| `{name}` | `{row['variant']}` | {row['model_calls']} | "
                f"{row['input_tokens']:,} / {row['output_tokens']:,} | "
                f"${row['cost_usd']:.2f} | {estimates} |"
            )
        if questions:
            per_question.append((tokens[0] // questions, tokens[1] // questions))
    if not per_question:
        return "\n".join(lines)
    tin = max(p[0] for p in per_question)
    tout = max(p[1] for p in per_question)
    lines += [
        "",
        f"Estimated cost of the live runs, from the largest measured tokens per question "
        f"({tin:,} in / {tout:,} out, repairs included) at list price with no cache hits:",
        "",
        "| What | Command | Questions answered | "
        + " | ".join(f"`{m}`" for m in ESTIMATE_MODELS)
        + " |",
        "|---|---|---:|" + "---:|" * len(ESTIMATE_MODELS),
    ]
    for what, command, questions, variants in LIVE_COMMANDS:
        # The corrected track re-asks about a quarter of the questions in other words.
        answered = round(questions * variants * 1.3)
        costs = []
        for m in ESTIMATE_MODELS:
            cost = estimate_cost(tin * answered, tout * answered, m)
            costs.append(DASH if cost is None else f"${cost:.2f}")
        cmd = command.format(model="claude-haiku-5-5")
        lines.append(f"| {what} | `{cmd}` | {answered} | " + " | ".join(costs) + " |")
    return "\n".join(lines)


def render_all(results: Path = RESULTS) -> dict[str, str]:
    reports = run_reports(results)
    tables = {
        "gold": gold_table(json.loads((results / "bird-gold.json").read_text())),
        "link": link_table(json.loads((results / "bird-link.json").read_text())),
    }
    if reports:
        tables["ex"] = ex_table(reports)
        tables["ledger"] = ledger_table(reports)
    return tables


def _splice(table: str, match: re.Match[str]) -> str:
    return match.group(1) + table + match.group(2)


def apply(text: str, tables: dict[str, str]) -> str:
    for name, table in tables.items():
        pattern = re.compile(
            rf"(<!-- bird:{name} -->\n).*?(\n<!-- /bird:{name} -->)", flags=re.DOTALL
        )
        if not pattern.search(text):
            raise ValueError(f"RESULTS.md has no <!-- bird:{name} --> block")
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
            print("RESULTS.md is stale: run `uv run python -m eval.bird.report`", file=sys.stderr)
            return 1
        print("RESULTS.md matches the reports")
        return 0
    RESULTS_MD.write_text(rendered, encoding="utf-8")
    print("RESULTS.md tables rendered from eval/results/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
