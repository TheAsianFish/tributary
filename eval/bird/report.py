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

from eval.bird.failures import failures_table, load_labels
from eval.bird.paired import (
    CANDIDATE,
    CHALLENGER,
    DECIDING_MODEL,
    DECIDING_PROVIDER,
    DECIDING_SUBSET,
    PAIRINGS,
    PairingError,
    accuracy,
    decide,
    deciding_run,
    mcnemar_exact,
    pair,
)
from eval.bird.run import ESTIMATE_MODELS, estimate_cost, wilson
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
    same, same_differ = c["same_wording"], c["differs_with_same_wording"]
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
            f"| Verified corrected questions worded exactly as in the official release | "
            f"{same} / {verified} |",
            f"| ... whose corrected gold returns different rows from the official gold | "
            f"**{same_differ} / {same} ({_pct(same_differ / same)})** |",
            f"| Reworded questions whose gold rows differ (partly a different question) | "
            f"{differ - same_differ} / {verified - same} |",
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


def _ordered(items: dict[str, Any], first: Sequence[str] = ("full",)) -> list[tuple[str, Any]]:
    """Report keys in reading order: the baseline (or the official track) first."""
    return sorted(
        items.items(),
        key=lambda kv: (kv[0] not in first, first.index(kv[0]) if kv[0] in first else 0, kv[0]),
    )


def ex_table(reports: Sequence[tuple[str, dict[str, Any]]]) -> str:
    lines = [
        "| Model | Subset | Variant | Track | EX | 95% interval | Simple | Moderate | Challenging "
        "| Model calls | Tokens in / out | Model time per question, p50 / p95 |",
        "|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for _, report in reports:
        model = f"{report['model']} ({report['provider']})"
        for variant, tracks in _ordered(report["variants"]):
            for repairs in ("r0", "r2"):
                for track, entry in _ordered(tracks, ("official", "corrected")):
                    s = entry[repairs]
                    lo, hi = s["ex_wilson95"]
                    d = s["by_difficulty"]

                    def cell(key: str, d: dict[str, Any] = d) -> str:
                        e = d.get(key)
                        return f"{e['correct']}/{e['total']}" if e else DASH

                    lines.append(
                        f"| {model} | {report['subset']} | `{variant}-{repairs}` | {track} | "
                        f"**{s['correct']}/{s['total']}** ({_pct(s['ex'])}) | "
                        f"{_pct(lo)}{DASH}{_pct(hi)} | {cell('simple')} | {cell('moderate')} | "
                        f"{cell('challenging')} | {s['model_calls']} | "
                        f"{s['input_tokens']:,} / {s['output_tokens']:,} | "
                        f"{s['model_ms_p50'] / 1000:.1f} s / {s['model_ms_p95'] / 1000:.1f} s |"
                    )
    return "\n".join(lines)


def paired(baseline: Sequence[dict[str, Any]], other: Sequence[dict[str, Any]]) -> tuple[int, int]:
    """Questions right only with ``other`` and right only with ``baseline`` (same questions)."""
    base = {v["id"]: v["correct"] for v in baseline}
    gained = sum(v["correct"] and not base.get(v["id"], False) for v in other)
    lost = sum(base.get(v["id"], False) and not v["correct"] for v in other)
    return gained, lost


BASELINE = "full"


def ablation_table(report: dict[str, Any]) -> str:
    """One row per variant (with repairs): EX on both tracks, the paired change against
    the baseline on each, refusals, what it cost and how long a question took."""
    variants = report["variants"]
    base = variants.get(BASELINE)
    tracks = [t for t in ("official", "corrected") if t in next(iter(variants.values()))]
    header = (
        "| Variant | "
        + " | ".join(f"{t.capitalize()} EX" for t in tracks)
        + " | "
        + " | ".join(f"vs `{BASELINE}`, {t}: +gained -lost (p)" for t in tracks)
        + " | Official EX, no repairs | Refused | Model calls | Tokens in / out | "
        + " / ".join(f"`{m}`" for m in ESTIMATE_MODELS)
        + " | Model time per question, p50 / p95 |"
    )
    lines = [header, "|---|" + "---:|" * (2 * len(tracks) + 6)]
    ledger = {row["variant"]: row for row in report["ledger"]}
    for name, entry in _ordered(variants, (BASELINE,)):
        cells = []
        for t in tracks:
            s = entry[t]["r2"]
            lo, hi = s["ex_wilson95"]
            cells.append(
                f"**{s['correct']}/{s['total']}** ({_pct(s['ex'])}, {_pct(lo)}{DASH}{_pct(hi)})"
            )
        for t in tracks:
            if base is None or name == BASELINE:
                cells.append(DASH)
                continue
            gained, lost = paired(base[t]["verdicts"], entry[t]["verdicts"])
            cells.append(f"+{gained} -{lost} ({mcnemar_exact(gained, lost):.2f})")
        r0 = entry[tracks[0]]["r0"]
        cells.append(DASH if r0 is None else f"{r0['correct']}/{r0['total']}")
        refused = (entry[tracks[0]]["r2"]["by_status"] or {}).get("unanswerable", 0)
        cells.append(str(refused))
        row = ledger.get(f"{name}-r2")
        s0 = entry[tracks[0]]["r2"]
        if row is None:  # pragma: no cover (the ledger has a row for every variant)
            cells += [DASH, DASH, DASH]
        else:
            estimates = " / ".join(
                DASH if row["estimate_usd"][m] is None else f"${row['estimate_usd'][m]:.2f}"
                for m in ESTIMATE_MODELS
            )
            cells += [
                str(row["model_calls"]),
                f"{row['input_tokens']:,} / {row['output_tokens']:,}",
                estimates,
            ]
        cells.append(f"{s0['model_ms_p50'] / 1000:.1f} s / {s0['model_ms_p95'] / 1000:.1f} s")
        lines.append(f"| `{name}` | " + " | ".join(cells) + " |")
    return "\n".join(lines)


# Live commands that have not been run: (what, command, multiple of the measured dev50 run).
LIVE_COMMANDS: tuple[tuple[str, str, int], ...] = (
    (
        "dev50, the recorded run's variants, both tracks",
        "make eval PROVIDER=anthropic MODEL={model} VARIANTS={variants} RECORD=1",
        50,
    ),
    (
        "all 500, the same variants, both tracks",
        "make eval PROVIDER=anthropic MODEL={model} VARIANTS={variants} SUBSET=all RECORD=1",
        500,
    ),
)


def _count_words(n: int) -> str:
    words = ("no", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine")
    return words[n] if 0 <= n < len(words) else str(n)


def estimate_basis(reports: Sequence[tuple[str, dict[str, Any]]]) -> tuple[str, dict[str, Any]]:
    """The run the live estimates scale from: a dev50 run, never whichever name sorts first."""
    dev50 = [(name, r) for name, r in reports if r.get("subset") == "dev50"]
    return (dev50 or list(reports))[0]


def question_count(report: dict[str, Any]) -> int:
    """Questions a run asked: its largest track (the official one keeps every question)."""
    first = next(iter(report["variants"].values()))
    return max(len(entry["verdicts"]) for entry in first.values()) or 1


def ledger_table(reports: Sequence[tuple[str, dict[str, Any]]]) -> str:
    lines = [
        "| Run | Variant | Model calls | Tokens in / out | Cost | Same tokens on "
        + " / ".join(f"`{m}`" for m in ESTIMATE_MODELS)
        + " |",
        "|---|---|---:|---:|---:|---:|",
    ]
    for name, report in reports:
        for row in sorted(
            report["ledger"], key=lambda r: (not r["variant"].startswith("full"), r["variant"])
        ):
            estimates = " / ".join(
                DASH if row["estimate_usd"][m] is None else f"${row['estimate_usd'][m]:.2f}"
                for m in ESTIMATE_MODELS
            )
            lines.append(
                f"| `{name}` | `{row['variant']}` | {row['model_calls']} | "
                f"{row['input_tokens']:,} / {row['output_tokens']:,} | "
                f"${row['cost_usd']:.2f} | {estimates} |"
            )
    if not reports:
        return "\n".join(lines)
    # The live runs are the recorded run's variants with repairs: its measured tokens,
    # scaled by question count for a larger subset (same questions per database mix).
    name, report = estimate_basis(reports)
    questions = question_count(report)
    tokens_in = sum(r["input_tokens"] for r in report["ledger"] if r["variant"].endswith("-r2"))
    tokens_out = sum(r["output_tokens"] for r in report["ledger"] if r["variant"].endswith("-r2"))
    lines += [
        "",
        f"Estimated cost of the live runs: the token counts `{name}` measured for "
        f"its {_count_words(sum(r['variant'].endswith('-r2') for r in report['ledger']))} "
        f"variants with repairs ({tokens_in:,} in / {tokens_out:,} out, both tracks), "
        "scaled by question count, at list price with no cache hits (an upper bound on "
        "input: the planner caches its schema prefix), Claude's own tokenizer and "
        "thinking tokens not modelled:",
        "",
        "| What | Command | " + " | ".join(f"`{m}`" for m in ESTIMATE_MODELS) + " |",
        "|---|---|" + "---:|" * len(ESTIMATE_MODELS),
    ]
    for what, command, target in LIVE_COMMANDS:
        scale = target / questions
        costs = []
        for m in ESTIMATE_MODELS:
            cost = estimate_cost(tokens_in * scale, tokens_out * scale, m)
            costs.append(DASH if cost is None else f"${cost:.2f}")
        cmd = command.format(model="<model>", variants=",".join(report["variants"]))
        lines.append(f"| {what} | `{cmd}` | " + " | ".join(costs) + " |")
    return "\n".join(lines)


# --- M10: the held-out sample, the pooled view and the default ---------------------------


TRACK_ORDER = ("official", "corrected")


def _ex_cell(correct: int, total: int) -> str:
    lo, hi = wilson(correct, total)
    ex = correct / total if total else 0.0
    return f"**{correct}/{total}** ({_pct(ex)}, {_pct(lo)}{DASH}{_pct(hi)})"


def holdout_table(report: dict[str, Any]) -> str:
    """Each variant on the held-out subset: EX with its interval on both tracks, refusals,
    calls, tokens and model time (with repairs)."""
    variants = report["variants"]
    tracks = [t for t in TRACK_ORDER if t in next(iter(variants.values()))]
    lines = [
        "| Variant | "
        + " | ".join(f"{t.capitalize()} EX (95% interval)" for t in tracks)
        + " | Official EX, no repairs | Refused | Model calls | Tokens in / out "
        "| Model time per question, p50 / p95 |",
        "|---|" + "---:|" * (len(tracks) + 5),
    ]
    ledger = {row["variant"]: row for row in report["ledger"]}
    for name, entry in _ordered(variants, ("full", CHALLENGER, CANDIDATE)):
        cells = [_ex_cell(entry[t]["r2"]["correct"], entry[t]["r2"]["total"]) for t in tracks]
        r0 = entry[tracks[0]]["r0"]
        cells.append(DASH if r0 is None else f"{r0['correct']}/{r0['total']}")
        s0 = entry[tracks[0]]["r2"]
        cells.append(str((s0["by_status"] or {}).get("unanswerable", 0)))
        row = ledger[f"{name}-r2"]
        cells += [
            str(row["model_calls"]),
            f"{row['input_tokens']:,} / {row['output_tokens']:,}",
            f"{s0['model_ms_p50'] / 1000:.1f} s / {s0['model_ms_p95'] / 1000:.1f} s",
        ]
        lines.append(f"| `{name}` | " + " | ".join(cells) + " |")
    return "\n".join(lines)


def _pair_cells(reports: Sequence[dict[str, Any]], baseline: str, other: str, track: str) -> str:
    try:
        p = pair(reports, baseline, other, track)
    except PairingError:
        return f"{DASH} | {DASH} | {DASH} | {DASH}"
    return (
        f"{p.n} | {p.baseline_correct} {chr(0x2192)} {p.other_correct} | "
        f"+{p.gained} -{p.lost} | {p.p:.3f}"
    )


def pairs_table(reports: Sequence[dict[str, Any]]) -> str:
    """M10's three pairings, on each subset alone and pooled over all of them."""
    columns = [*[[r] for r in reports], list(reports)] if len(reports) > 1 else [list(reports)]
    labels = [" + ".join(str(r["subset"]) for r in group) for group in columns]
    header = "| Pairing | Track | " + " | ".join(
        f"{label}: n | {label}: correct | {label}: +gained -lost | {label}: p" for label in labels
    )
    lines = [header + " |", "|---|---|" + "---:|" * (4 * len(columns))]
    for baseline, other in PAIRINGS:
        for track in TRACK_ORDER:
            cells = [_pair_cells(group, baseline, other, track) for group in columns]
            lines.append(f"| `{other}` vs `{baseline}` | {track} | " + " | ".join(cells) + " |")
    return "\n".join(lines)


def pooled_table(reports: Sequence[dict[str, Any]]) -> str:
    """Each M10 variant's EX pooled over every subset, with its interval."""
    labels = " + ".join(str(r["subset"]) for r in reports)
    lines = [
        f"| Variant | Official EX, {labels} | Corrected EX, {labels} |",
        "|---|---:|---:|",
    ]
    for name in ("full", CHALLENGER, CANDIDATE):
        cells = []
        for track in TRACK_ORDER:
            try:
                cells.append(_ex_cell(*accuracy(reports, name, track)))
            except PairingError:
                cells.append(DASH)
        lines.append(f"| `{name}` | " + " | ".join(cells) + " |")
    return "\n".join(lines)


def decision_text(report: dict[str, Any]) -> str:
    """The pre-stated rule applied to the held-out run, in one paragraph."""
    d = decide(report)
    counts = "; ".join(
        f"{track} {c} against {ch} for `{CHALLENGER}`" for track, c, ch in d.by_track
    )
    if d.kept:
        verdict = (
            f"`{CANDIDATE}` is at least level with `{CHALLENGER}` on both tracks, so it "
            f"stays `make eval`'s live default (`full,{CANDIDATE}`)."
        )
    else:
        verdict = (
            f"`{CANDIDATE}` is behind `{CHALLENGER}` on at least one track, so under the rule "
            f"the live default becomes `full,{CHALLENGER}`: the M7 gain of value hints and "
            "worked examples did not replicate on questions that did not pick it."
        )
    return (
        f"**The rule, applied.** On {DECIDING_SUBSET} alone, `{CANDIDATE}` answers {counts}. "
        + verdict
    )


def m10_reports(
    reports: Sequence[tuple[str, dict[str, Any]]],
    model: str = DECIDING_MODEL,
    provider: str = DECIDING_PROVIDER,
) -> tuple[dict[str, Any], list[dict[str, Any]]] | None:
    """The held-out run the rule was stated for and every run of the same model to pool with
    it (dev50 first). A run of another model, whatever its file name, takes no part."""
    holdout = deciding_run([r for _, r in reports], model, provider)
    if holdout is None:
        return None
    same = [
        r
        for _, r in reports
        if r.get("model") == model and r.get("provider") == provider and r.get("subset") != "all"
    ]
    same.sort(key=lambda r: (r["subset"] != "dev50", r["subset"]))
    return holdout, same


def render_all(results: Path = RESULTS) -> dict[str, str]:
    reports = run_reports(results)
    tables = {
        "gold": gold_table(json.loads((results / "bird-gold.json").read_text())),
        "link": link_table(json.loads((results / "bird-link.json").read_text())),
    }
    none = "No recorded run yet: `make eval PROVIDER=ollama RECORD=1` after `make bird`."
    tables["ex"] = ex_table(reports) if reports else none
    tables["ledger"] = ledger_table(reports) if reports else none
    # The ablation and the taxonomy describe one run, the one the labels were read from;
    # a later run (another model, another subset) must not take their place by its name.
    labels_path = results / "bird-failures.json"
    labels = load_labels(labels_path) if labels_path.exists() else None
    basis: dict[str, Any] | None = None
    if labels is not None and reports:
        by_name = dict(reports)
        if labels["report"] not in by_name:
            raise ValueError(
                f"bird-failures.json labels {labels['report']}, which is not in {results}"
            )
        basis = by_name[labels["report"]]
    elif reports:
        _, basis = estimate_basis(reports)
    tables["ablation"] = ablation_table(basis) if basis is not None else none
    if labels is not None and basis is not None:
        tables["failures"] = failures_table(labels, basis)
    m10 = m10_reports(reports)
    pending = f"No {DECIDING_SUBSET} run recorded yet."
    if m10 is None:
        tables.update(holdout=pending, pairs=pending, pooled=pending, decision=pending)
    else:
        holdout, pooled = m10
        tables["holdout"] = holdout_table(holdout)
        tables["pairs"] = pairs_table(pooled)
        tables["pooled"] = pooled_table(pooled)
        tables["decision"] = decision_text(holdout)
    return tables


def _splice(table: str, match: re.Match[str]) -> str:
    return match.group(1) + table + "\n" + match.group(2)


def apply(text: str, tables: dict[str, str]) -> str:
    """Each table between its markers; an empty block (markers on adjacent lines) works too."""
    for name, table in tables.items():
        pattern = re.compile(
            rf"(<!-- bird:{name} -->\n).*?(<!-- /bird:{name} -->)", flags=re.DOTALL
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
