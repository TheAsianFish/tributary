"""M13: does agreement between two prompt variants say when an answer is probably wrong?

    uv run python -m eval.agree [--check]

Offline: no model and no database. The 40 warehouse questions are re-run
through the agent twice, each pass replaying its recorded model calls
(:class:`~tributary.agent.providers.fake.FakeProvider`) and its recorded
database executions (:class:`~eval.bird.recorded.ReplayExecutor`) over the
committed catalog (``eval/warehouse_catalog.json``):

- the plain pass, the graded answer (``tests/recordings/warehouse.jsonl``);
- the second pass, value hints and three worked examples
  (``tests/recordings/warehouse-vf.jsonl``).

Each replayed verdict must equal its recorded run's (status, correctness,
attempts, SQL): the replay is exact or the command fails. A question is
*agreed* when both passes answered and their rows are the same set under
BIRD's rule (:func:`eval.bird.score.execution_match`), *differed* otherwise.
The plain pass's correctness against agreement is the 2 x 2 table read by
the rule fixed before the run (:mod:`eval.agree_stats`).

The same split on BIRD (dev50 and holdout50, official track, ``full`` against
``full+v+f+a``) comes from the committed BIRD reports and their recorded
executions; it is exploratory, since those runs suggested the idea.

Writes ``eval/results/agree.json`` and re-renders RESULTS.md's tables between
the ``agree:*`` markers; ``--check`` fails instead if either would change.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from collections import Counter
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from eval.agree_stats import GAP_POINTS, Table, needed, verdict
from eval.bird.paired import mcnemar_exact
from eval.bird.recorded import ReplayExecutor, execution_key, from_record, load_executions
from eval.bird.score import execution_match
from eval.warehouse_eval import (
    DATABASE,
    EXECUTIONS,
    Graded,
    Question,
    grade_question,
    load_questions,
)
from tributary.agent.catalog import Catalog
from tributary.agent.catalog import Table as CatalogTable
from tributary.agent.provenance import TableProvenance
from tributary.agent.providers.fake import FakeProvider
from tributary.agent.service import Agent, Limits, StaticCatalog
from tributary.config import REPO_ROOT
from tributary.logging import configure_logging

RESULTS_DIR = REPO_ROOT / "eval" / "results"
OUT = RESULTS_DIR / "agree.json"
RESULTS_MD = REPO_ROOT / "RESULTS.md"
CATALOG = REPO_ROOT / "eval" / "warehouse_catalog.json"
RECORDINGS = REPO_ROOT / "tests" / "recordings"


@dataclass(frozen=True, slots=True)
class Pass:
    name: str
    recordings: Path
    report: Path
    limits: Limits


PLAIN = Pass(
    "plain",
    RECORDINGS / "warehouse.jsonl",
    RESULTS_DIR / "warehouse-ollama-qwen2.5-coder-7b.json",
    Limits(),
)
SECOND = Pass(
    "second",
    RECORDINGS / "warehouse-vf.jsonl",
    RESULTS_DIR / "warehouse-ollama-qwen2.5-coder-7b-vf.json",
    Limits(values=True, examples=3),
)
BIRD_SUBSETS = ("dev50", "holdout50")
BIRD_PAIR = ("full", "full+v+f+a")
BIRD_TRACK = "official"


class ReplayMismatch(Exception):  # noqa: N818 (reads as the condition it reports)
    """A replayed verdict differs from the recorded run's."""


class NoProvenance:
    """The replay grades rows; it never reads freshness."""

    def lookup(self, tables: Sequence[CatalogTable]) -> list[TableProvenance]:
        return []


def load_catalog(path: Path = CATALOG) -> Catalog:
    return Catalog.from_dict(json.loads(path.read_text(encoding="utf-8")))


def write_catalog(catalog: Catalog, path: Path = CATALOG) -> None:
    """The catalog the recordings were made against, without its publish-time version."""
    document = catalog.to_dict()
    document["version"] = "warehouse-eval"
    path.write_text(json.dumps(document, indent=1, sort_keys=True, ensure_ascii=False) + "\n")


def replay_pass(
    run: Pass,
    questions: Sequence[Question],
    catalog: Catalog,
    executions: dict[str, dict[str, Any]],
) -> list[Graded]:
    """Every question through the agent with recorded model calls and executions."""
    executor = ReplayExecutor(executions, DATABASE)
    agent = Agent(
        StaticCatalog(catalog), FakeProvider(run.recordings), executor, NoProvenance(), run.limits
    )
    graded = [grade_question(agent, executor, q) for q in questions]
    check_against_report(run, graded)
    return graded


COMPARED = ("status", "correct", "attempts", "sql")


def check_against_report(run: Pass, graded: Sequence[Graded]) -> None:
    recorded = {v["id"]: v for v in json.loads(run.report.read_text())["verdicts"]}
    if len(recorded) != len(graded):
        raise ReplayMismatch(f"{run.name}: {len(graded)} replayed, {len(recorded)} recorded")
    for g in graded:
        v = recorded.get(g.verdict.id)
        if v is None:
            raise ReplayMismatch(f"{run.name}: {g.verdict.id} is not in {run.report.name}")
        for name in COMPARED:
            if getattr(g.verdict, name) != v[name]:
                raise ReplayMismatch(
                    f"{run.name}/{g.verdict.id}: {name} replayed as "
                    f"{getattr(g.verdict, name)!r}, recorded as {v[name]!r}"
                )


def agreed(first: Graded, second: Graded) -> bool:
    if first.rows is None or second.rows is None:
        return False
    if first.verdict.status != "answered" or second.verdict.status != "answered":
        return False
    return execution_match(first.rows, second.rows)


def table_of(rows: Iterable[tuple[bool, bool]]) -> Table:
    """The 2 x 2 table from (agreed, right) pairs."""
    ar = aw = dr = dw = 0
    for is_agreed, right in rows:
        if is_agreed:
            ar, aw = (ar + 1, aw) if right else (ar, aw + 1)
        else:
            dr, dw = (dr + 1, dw) if right else (dr, dw + 1)
    return Table(ar, aw, dr, dw)


def table_json(table: Table) -> dict[str, Any]:
    gap = table.gap_points
    return {
        "agreed_right": table.agreed_right,
        "agreed_wrong": table.agreed_wrong,
        "differed_right": table.differed_right,
        "differed_wrong": table.differed_wrong,
        "coverage": round(table.agreed / table.n, 4) if table.n else 0.0,
        "gap_points": None if gap is None else round(gap, 1),
        "p_one_sided": float(f"{table.p:.3g}"),
        "verdict": verdict(table),
    }


def _median(values: Sequence[float]) -> float:
    return round(statistics.median(values), 1) if values else 0.0


def warehouse(
    questions: Sequence[Question],
    plain: Sequence[Graded],
    second: Sequence[Graded],
) -> dict[str, Any]:
    by_q = list(zip(questions, plain, second, strict=True))
    agreement = [agreed(p, s) for _, p, s in by_q]
    right = [p.verdict.correct for _, p, _ in by_q]
    main = table_of(zip(agreement, right, strict=True))
    per_difficulty = {}
    for level in ("simple", "moderate", "challenging"):
        rows = [
            (a, r)
            for (q, _, _), a, r in zip(by_q, agreement, right, strict=True)
            if q.difficulty == level
        ]
        per_difficulty[level] = table_json(table_of(rows))
    no_repair = [p.verdict.status == "answered" and p.verdict.attempts == 1 for _, p, _ in by_q]
    simple = [q.difficulty == "simple" for q, _, _ in by_q]
    answered = [p for _, p, _ in by_q if p.verdict.status == "answered"]
    answered_second = [s for _, _, s in by_q if s.verdict.status == "answered"]
    plain_ms = [p.verdict.model_ms for _, p, _ in by_q]
    second_ms = [s.verdict.model_ms for _, _, s in by_q]
    agreed_count = main.agreed
    gained = sum(1 for _, p, s in by_q if s.verdict.correct and not p.verdict.correct)
    lost = sum(1 for _, p, s in by_q if p.verdict.correct and not s.verdict.correct)
    return {
        "n": len(by_q),
        "graded": "plain",
        "plain_correct": sum(right),
        "second_correct": sum(s.verdict.correct for _, _, s in by_q),
        "second_by_difficulty": {
            level: f"{sum(s.verdict.correct for q, _, s in by_q if q.difficulty == level)}"
            f" / {sum(1 for q, _, _ in by_q if q.difficulty == level)}"
            for level in ("simple", "moderate", "challenging")
        },
        "second_against_plain": {
            "gained": gained,
            "lost": lost,
            "mcnemar_p": round(mcnemar_exact(gained, lost), 4),
        },
        "differed_by_kind": {
            kind: {
                "plain_right": sum(
                    1
                    for (_, p, s), a in zip(by_q, agreement, strict=True)
                    if not a and _kind(s) == kind and p.verdict.correct
                ),
                "n": sum(
                    1
                    for (_, _, s), a in zip(by_q, agreement, strict=True)
                    if not a and _kind(s) == kind
                ),
            }
            for kind in ("other rows", "second pass did not answer")
        },
        "agreed_wrong": [
            {"id": q.id, "difficulty": q.difficulty, "plain_sql": p.verdict.sql}
            for (q, p, _), a in zip(by_q, agreement, strict=True)
            if a and not p.verdict.correct
        ],
        "second_status": dict(sorted(Counter(s.verdict.status for _, _, s in by_q).items())),
        "table": table_json(main),
        "needed_right_among_agreed": needed(agreed_count, main.right, main.n),
        "in_hypothesised_range": 14 <= agreed_count <= 24,
        "by_difficulty": per_difficulty,
        "baselines": {
            "answered_without_a_repair": table_json(table_of(zip(no_repair, right, strict=True))),
            "labelled_simple": table_json(table_of(zip(simple, right, strict=True))),
        },
        "graded_on_second": table_json(
            table_of(zip(agreement, (s.verdict.correct for _, _, s in by_q), strict=True))
        ),
        "confidence_high": {
            "plain": f"{sum(p.verdict.confidence == 'high' for p in answered)} / {len(answered)}",
            "second": f"{sum(s.verdict.confidence == 'high' for s in answered_second)}"
            f" / {len(answered_second)}",
        },
        "model_ms": {
            "plain_p50": _median(plain_ms),
            "second_p50": _median(second_ms),
            "second_mean": round(statistics.fmean(second_ms), 1) if second_ms else 0.0,
            "second_total_s": round(sum(second_ms) / 1000, 1),
            "plain_total_s": round(sum(plain_ms) / 1000, 1),
        },
        "questions": [
            {
                "id": q.id,
                "difficulty": q.difficulty,
                "agreed": a,
                "plain": p.verdict.status + (" right" if p.verdict.correct else " wrong"),
                "second": s.verdict.status + (" right" if s.verdict.correct else " wrong"),
            }
            for (q, p, s), a in zip(by_q, agreement, strict=True)
        ],
    }


def _kind(second: Graded) -> str:
    """Why a differed question differed (the plain pass answered all 40)."""
    return "other rows" if second.verdict.status == "answered" else "second pass did not answer"


# --- BIRD (exploratory) -------------------------------------------------------------------------


def bird_report(subset: str) -> dict[str, Any]:
    path = RESULTS_DIR / f"bird-{subset}-ollama-qwen2.5-coder-7b.json"
    report: dict[str, Any] = json.loads(path.read_text())
    return report


def bird_rows(
    executions: dict[str, dict[str, Any]], v: dict[str, Any]
) -> Sequence[Sequence[object]] | None:
    if v["status"] != "answered" or not v["sql"]:
        return None
    record = executions.get(execution_key(v["db_id"], v["sql"]))
    if record is None:
        raise ReplayMismatch(f"no recorded execution for BIRD question {v['id']}")
    return from_record(record).rows


def bird(executions: dict[str, dict[str, Any]]) -> dict[str, Any]:
    first, second = BIRD_PAIR
    out: dict[str, Any] = {}
    pooled: dict[str, list[tuple[bool, bool]]] = {first: [], second: []}
    for subset in BIRD_SUBSETS:
        variants = bird_report(subset)["variants"]
        a = {v["id"]: v for v in variants[first][BIRD_TRACK]["verdicts"]}
        b = {v["id"]: v for v in variants[second][BIRD_TRACK]["verdicts"]}
        if a.keys() != b.keys():
            raise ReplayMismatch(f"{subset}: the two variants graded different questions")
        split: dict[str, list[tuple[bool, bool]]] = {first: [], second: []}
        agreed_wrong = []
        for qid in sorted(a):
            ra, rb = bird_rows(executions, a[qid]), bird_rows(executions, b[qid])
            same = ra is not None and rb is not None and execution_match(ra, rb)
            split[first].append((same, bool(a[qid]["correct"])))
            split[second].append((same, bool(b[qid]["correct"])))
            if same and not a[qid]["correct"]:
                agreed_wrong.append(
                    {"id": qid, "db_id": a[qid]["db_id"], "difficulty": a[qid]["difficulty"]}
                )
        out[subset] = {
            "graded_on": {name: table_json(table_of(split[name])) for name in BIRD_PAIR},
            "agreed_wrong": agreed_wrong,
        }
        for name in BIRD_PAIR:
            pooled[name] += split[name]
    out["pooled"] = {
        "graded_on": {name: table_json(table_of(pooled[name])) for name in BIRD_PAIR},
        "agreed_wrong": [w for s in BIRD_SUBSETS for w in out[s]["agreed_wrong"]],
    }
    return out


# --- rendering ----------------------------------------------------------------------------------


def _cell(t: dict[str, Any]) -> tuple[str, str]:
    agreed = t["agreed_right"] + t["agreed_wrong"]
    differed = t["differed_right"] + t["differed_wrong"]
    return f"{t['agreed_right']} / {agreed}", f"{t['differed_right']} / {differed}"


def _p(value: float) -> str:
    return f"{value:.3f}" if value >= 0.001 else f"{value:.1e}"


def _gap(t: dict[str, Any]) -> str:
    return "n/a" if t["gap_points"] is None else f"{t['gap_points']:+.1f}"


def render_warehouse(w: dict[str, Any]) -> str:
    t = w["table"]
    lines = [
        "| Questions | Agreed: plain pass right | Differed: plain pass right | Gap (points)"
        " | One-sided Fisher p | Under the rule |",
        "|---|---:|---:|---:|---:|---|",
    ]
    a, d = _cell(t)
    lines.append(
        f"| **All 40** | **{a}** | **{d}** | {_gap(t)} | **{_p(t['p_one_sided'])}** "
        f"| **{t['verdict']}** |"
    )
    for level, row in w["by_difficulty"].items():
        a, d = _cell(row)
        lines.append(
            f"| {level} | {a} | {d} | {_gap(row)} | {_p(row['p_one_sided'])} | (not tested) |"
        )
    lines += [
        "",
        "| Signal marking an answer as probably right | Coverage | Marked: right | Not marked:"
        " right | Gap (points) | One-sided Fisher p |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    signals = [("The two passes agree", t)] + [
        (label, w["baselines"][key])
        for label, key in (
            ("Answered without a repair", "answered_without_a_repair"),
            ("Labelled `simple`", "labelled_simple"),
        )
    ]
    for label, row in signals:
        a, d = _cell(row)
        lines.append(
            f"| {label} | {100 * row['coverage']:.1f}% | {a} | {d} | {_gap(row)}"
            f" | {_p(row['p_one_sided'])} |"
        )
    return "\n".join(lines)


def render_bird(b: dict[str, Any]) -> str:
    first, second = BIRD_PAIR
    lines = [
        "| Subset (official track) | Graded on | Agreed: right | Differed: right | Gap (points)"
        " | One-sided Fisher p | Agreed but wrong |",
        "|---|---|---:|---:|---:|---:|---|",
    ]
    for subset in (*BIRD_SUBSETS, "pooled"):
        entry = b[subset]
        wrong = ", ".join(str(w["id"]) for w in entry["agreed_wrong"]) or "none"
        for name in (first, second):
            row = entry["graded_on"][name]
            a, d = _cell(row)
            label = "both, n = 100" if subset == "pooled" else subset
            lines.append(
                f"| {label} | `{name}` | {a} | {d} | {_gap(row)} | {_p(row['p_one_sided'])}"
                f" | {wrong if name == first else '(same questions)'} |"
            )
    return "\n".join(lines)


def render_into(text: str, marker: str, body: str) -> str:
    start, end = f"<!-- agree:{marker} -->\n", f"<!-- /agree:{marker} -->"
    if start not in text:
        return text
    head, rest = text.split(start, 1)
    _, tail = rest.split(end, 1)
    return f"{head}{start}{body}\n{end}{tail}"


def render(document: dict[str, Any], text: str) -> str:
    text = render_into(text, "warehouse", render_warehouse(document["warehouse"]))
    return render_into(text, "bird", render_bird(document["bird"]))


# --- the command --------------------------------------------------------------------------------


def build() -> dict[str, Any]:
    questions = load_questions()
    catalog = load_catalog()
    executions = load_executions(EXECUTIONS)
    plain = replay_pass(PLAIN, questions, catalog, executions)
    second = replay_pass(SECOND, questions, catalog, executions)
    bird_executions = load_executions(RECORDINGS / "bird-executions.jsonl.gz")
    return {
        "rule": {"alpha": 0.05, "gap_points": GAP_POINTS},
        "warehouse": warehouse(questions, plain, second),
        "bird": bird(bird_executions),
    }


def main(argv: Sequence[str] | None = None, *, emit: Callable[[str], None] = print) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--check", action="store_true", help="fail if the outputs would change")
    args = parser.parse_args(argv)
    configure_logging("console", "WARNING")  # the replay's per-call lines are noise here
    try:
        document = build()
    except ReplayMismatch as exc:
        print(f"replay differs from the recorded run: {exc}", file=sys.stderr)
        return 1
    rendered = json.dumps(document, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    text = RESULTS_MD.read_text(encoding="utf-8")
    new_text = render(document, text)
    if args.check:
        outputs = ((OUT, _read(OUT), rendered), (RESULTS_MD, text, new_text))
        stale = [p.name for p, old, new in outputs if old != new]
        if stale:
            print(f"stale: {', '.join(stale)}; run `make eval-agree`", file=sys.stderr)
            return 1
    else:
        OUT.write_text(rendered, encoding="utf-8")
        RESULTS_MD.write_text(new_text, encoding="utf-8")
    w = document["warehouse"]
    t = w["table"]
    emit(
        f"warehouse: agreed {t['agreed_right']}/{t['agreed_right'] + t['agreed_wrong']} right, "
        f"differed {t['differed_right']}/{t['differed_right'] + t['differed_wrong']} right, "
        f"p = {t['p_one_sided']}, {t['verdict']}; second pass {w['second_correct']}/{w['n']}"
    )
    for name, row in document["bird"]["pooled"]["graded_on"].items():
        a, d = _cell(row)
        emit(f"BIRD pooled, graded on {name}: agreed {a} right, differed {d} right")
    return 0


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8") if path.exists() else ""


if __name__ == "__main__":
    raise SystemExit(main())
