"""``make eval-link``: how well each schema-linking strategy finds the tables a gold query reads.

    uv run python -m eval.bird.link_eval [--track official] [--k 1 2 3 4 5]

Offline: the committed catalogs (``eval/bird/data/catalogs.json``) and
question files are all it reads, so it runs in CI. For each question the gold
tables are the relations its gold SQL reads, parsed with sqlglot (CTE names
excluded); the linker sees the question with its evidence, as the planner
does. Per strategy and ``k``:

- **recall**: gold tables found / gold tables, averaged over questions;
- **all found**: share of questions whose every gold table was linked (the
  planner cannot write the gold query otherwise);
- **precision**: gold tables found / tables linked, averaged;
- **tables shown**: mean tables in the prompt, the cost side.

Strategies that are not implemented (``core`` until it is) are reported as
such, not skipped silently. The report goes to ``eval/results/bird-link.json``.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from collections.abc import Sequence
from typing import Any

import sqlglot
from sqlglot import exp

from eval.bird import data
from eval.bird.load import load_catalogs
from tributary.agent.catalog import Catalog
from tributary.agent.linker import STRATEGIES, LinkerUnavailable, link_schema
from tributary.config import REPO_ROOT

REPORT = REPO_ROOT / "eval" / "results" / "bird-link.json"
DEFAULT_KS = (1, 2, 3, 4, 5, 6)


def gold_tables(sql: str, schema: str) -> frozenset[str]:
    """``schema.table`` for every table the PostgreSQL query reads (CTEs left out)."""
    tree = sqlglot.parse_one(sql, read="postgres")
    ctes = {c.alias_or_name.lower() for c in tree.find_all(exp.CTE)}
    found = set()
    for table in tree.find_all(exp.Table):
        name = table.name.lower()
        if name and name not in ctes:
            found.add(f"{schema}.{name}")
    return frozenset(found)


def score_strategy(
    questions: Sequence[data.BirdQuestion],
    catalogs: dict[str, Catalog],
    strategy: str,
    k: int | None,
) -> dict[str, Any]:
    recalls, precisions, shown, complete = [], [], [], 0
    for q in questions:
        catalog = catalogs[q.db_id]
        gold = gold_tables(q.sql, q.db_id) & set(catalog.relations())
        if not gold:
            continue
        link = link_schema(q.prompt, catalog, strategy=strategy, k=k)
        found = gold & set(link.tables)
        recalls.append(len(found) / len(gold))
        precisions.append(len(found) / len(link.tables) if link.tables else 0.0)
        shown.append(len(link.tables))
        complete += found == gold
    n = len(recalls)
    return {
        "strategy": strategy,
        "k": k,
        "questions": n,
        "recall": round(statistics.fmean(recalls), 4),
        "all_found": round(complete / n, 4),
        "all_found_count": complete,
        "precision": round(statistics.fmean(precisions), 4),
        "tables_shown": round(statistics.fmean(shown), 3),
    }


def evaluate(track: data.Track, ks: Sequence[int]) -> dict[str, Any]:
    questions = data.questions(track)
    catalogs = load_catalogs()
    gold_sizes = [len(gold_tables(q.sql, q.db_id)) for q in questions]
    rows: list[dict[str, Any]] = []
    unavailable: dict[str, str] = {}
    for strategy in STRATEGIES:
        for k in [None] if strategy == "full" else ks:
            try:
                rows.append(score_strategy(questions, catalogs, strategy, k))
            except LinkerUnavailable as exc:
                unavailable[strategy] = str(exc)
                break
    return {
        "track": track,
        "questions": len(questions),
        "gold_tables_per_question": {
            "mean": round(statistics.fmean(gold_sizes), 3),
            "max": max(gold_sizes),
        },
        "tables_per_database": {db: len(c.tables) for db, c in sorted(catalogs.items())},
        "results": rows,
        "unavailable": unavailable,
    }


def render(report: dict[str, Any]) -> str:
    lines = [
        f"{report['track']} track, {report['questions']} questions, "
        f"{report['gold_tables_per_question']['mean']} gold tables per question on average",
        f"{'strategy':<8} {'k':>3} {'recall':>7} {'all found':>10} {'precision':>10} {'shown':>6}",
    ]
    for r in report["results"]:
        k = "-" if r["k"] is None else str(r["k"])
        lines.append(
            f"{r['strategy']:<8} {k:>3} {r['recall']:>7.3f} {r['all_found']:>10.3f} "
            f"{r['precision']:>10.3f} {r['tables_shown']:>6.2f}"
        )
    for strategy, reason in report["unavailable"].items():
        lines.append(f"{strategy:<8} not available: {reason}")
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--track", choices=data.TRACKS, default="official")
    parser.add_argument("--k", type=int, nargs="+", default=list(DEFAULT_KS))
    parser.add_argument("--out", default=None, help="report path (default: the committed one)")
    args = parser.parse_args(argv)
    report = evaluate(args.track, args.k)
    out = REPORT if args.out is None else REPO_ROOT / args.out
    if args.track != "official" and args.out is None:
        out = REPORT.with_name(f"bird-link-{args.track}.json")
    out.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(render(report))
    return 0


if __name__ == "__main__":
    sys.exit(main())
