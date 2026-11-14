"""The 40 warehouse questions, end to end: planner → guard → agent role → compare with gold.

    uv run python -m eval.warehouse_eval [--provider fake|ollama|anthropic] [--record]

Each question goes through the same :class:`~tributary.agent.service.Agent`
path as ``/query``. The predicted query's rows are compared with the gold
query's rows as a multiset of tuples (execution accuracy as BIRD defines it,
but stricter: column order counts, row order does not, and duplicate rows
count, where BIRD compares sets), numbers rounded to six decimals; both run
as the agent's role on the same database.

``--record`` wraps the live provider so every model call (repairs included)
is stored under its request fingerprint in ``TRIBUTARY_RECORDINGS_PATH``; the
``fake`` provider then replays the run offline. ``--record-executions`` also
stores every database execution (gold queries, drafts, the errors a repair
was shown) with its value types in
``tests/recordings/warehouse-executions.jsonl.gz``, as BIRD's are, so a run
replays with neither a model nor a database (``make eval-agree``). The report
goes to ``eval/results/warehouse-<provider>-<model>[-<variant>].json``, the
variant naming the planner features that were on (``vf``: value hints and
worked examples); a replay writes ``warehouse-replay.json``, whose verdicts
must equal the recorded run's.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from collections import Counter
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import yaml

from eval.bird.recorded import Recorder, RecordingExecutor
from tributary.agent.execute import ExecutionError, Executor, jsonable
from tributary.agent.provider import Provider
from tributary.agent.service import Agent, build_agent
from tributary.config import REPO_ROOT, Settings, get_settings
from tributary.logging import bind_run_id, configure_logging

QUESTIONS = REPO_ROOT / "eval" / "warehouse_questions.yaml"
RESULTS = REPO_ROOT / "eval" / "results"
EXECUTIONS = REPO_ROOT / "tests" / "recordings" / "warehouse-executions.jsonl.gz"
# The database name recorded executions are keyed under (BIRD keys by its schema name).
DATABASE = "warehouse"


@dataclass(frozen=True, slots=True)
class Question:
    id: str
    question: str
    sql: str
    difficulty: str
    tables: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class Verdict:
    id: str
    difficulty: str
    status: str
    correct: bool
    attempts: int
    sql: str | None
    error: str | None
    input_tokens: int
    output_tokens: int
    cost_usd: float
    unpriced_calls: int
    latency_ms: float
    predicted_rows: int | None
    gold_rows: int
    # The last draft's stated confidence ("" when there was no draft) and the time spent
    # waiting on the model, repairs included.
    confidence: str = ""
    model_ms: float = 0.0


def load_questions(path: Path = QUESTIONS) -> list[Question]:
    with path.open(encoding="utf-8") as f:
        document = yaml.safe_load(f)
    return [
        Question(
            id=str(q["id"]),
            question=str(q["question"]),
            sql=str(q["sql"]),
            difficulty=str(q.get("difficulty", "unknown")),
            tables=tuple(q.get("tables", ())),
        )
        for q in document["questions"]
    ]


def _normal(value: object) -> object:
    plain = jsonable(value)
    if isinstance(plain, bool):
        return ("bool", plain)  # True == 1 in Python; a boolean must not match a count
    if isinstance(plain, float):
        return round(plain, 6)
    if isinstance(plain, list):
        return tuple(_normal(v) for v in plain)
    if isinstance(plain, dict):
        return json.dumps(plain, sort_keys=True)
    return plain


def result_key(rows: Sequence[Sequence[object]]) -> Counter[tuple[object, ...]]:
    """Rows as a multiset of normalised tuples (exact ints, floats to six decimals)."""
    return Counter(tuple(_normal(v) for v in row) for row in rows)


def same_result(predicted: Sequence[Sequence[object]], gold: Sequence[Sequence[object]]) -> bool:
    return result_key(predicted) == result_key(gold)


@dataclass(frozen=True, slots=True)
class Graded:
    """A verdict with the rows behind it: the answer's (None when nothing ran) and the gold's."""

    verdict: Verdict
    rows: tuple[tuple[object, ...], ...] | None
    gold: tuple[tuple[object, ...], ...]


def evaluate_question(agent: Agent, executor: Executor, q: Question) -> Verdict:
    return grade_question(agent, executor, q).verdict


def grade_question(agent: Agent, executor: Executor, q: Question) -> Graded:
    gold = executor.run(agent.guard().enforce(q.sql).sql)
    outcome = agent.planner().run(q.question)
    result = outcome.result
    correct = bool(result is not None and same_result(result.rows, gold.rows))
    draft = outcome.draft
    verdict = Verdict(
        id=q.id,
        difficulty=q.difficulty,
        status=outcome.status,
        correct=correct,
        attempts=len(outcome.attempts),
        sql=outcome.sql or (outcome.draft.sql if outcome.draft else None),
        error=outcome.error,
        input_tokens=outcome.usage.input_tokens,
        output_tokens=outcome.usage.output_tokens,
        cost_usd=outcome.usage.cost_usd,
        unpriced_calls=outcome.usage.unpriced_calls,
        latency_ms=outcome.latency_ms,
        predicted_rows=len(result.rows) if result else None,
        gold_rows=len(gold.rows),
        confidence=draft.confidence if draft else "",
        model_ms=round(outcome.model_ms, 1),
    )
    return Graded(
        verdict,
        tuple(tuple(r) for r in result.rows) if result is not None else None,
        tuple(tuple(r) for r in gold.rows),
    )


def _percentile(values: Sequence[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, round(q * (len(ordered) - 1))))
    return ordered[index]


def summarise(verdicts: Sequence[Verdict]) -> dict[str, Any]:
    """Totals by difficulty and status, cost and latency (Any: a JSON document)."""
    by_difficulty: dict[str, dict[str, int]] = {}
    for v in verdicts:
        entry = by_difficulty.setdefault(v.difficulty, {"correct": 0, "total": 0})
        entry["total"] += 1
        entry["correct"] += int(v.correct)
    latencies = [v.latency_ms for v in verdicts]
    return {
        "correct": sum(v.correct for v in verdicts),
        "total": len(verdicts),
        "by_difficulty": dict(sorted(by_difficulty.items())),
        "by_status": dict(sorted(Counter(v.status for v in verdicts).items())),
        "repaired": sum(1 for v in verdicts if v.correct and v.attempts > 1),
        "model_calls": sum(v.attempts for v in verdicts),
        "input_tokens": sum(v.input_tokens for v in verdicts),
        "output_tokens": sum(v.output_tokens for v in verdicts),
        "cost_usd": round(sum(v.cost_usd for v in verdicts), 6),
        # Calls with no known price: cost_usd leaves them out, and the report says so.
        "unpriced_calls": sum(v.unpriced_calls for v in verdicts),
        "latency_ms_p50": round(statistics.median(latencies), 1) if latencies else 0.0,
        "latency_ms_p95": round(_percentile(latencies, 0.95), 1),
    }


def variant(settings: Settings) -> str:
    """The planner features a run had on: "" for the plain pass, "vf" for values and examples."""
    parts = []
    if settings.agent_values:
        parts.append("v")
    if settings.agent_examples:
        parts.append("f")
    if settings.agent_votes > 1:
        parts.append(f"n{settings.agent_votes}")
    if settings.agent_linker != "full":
        parts.append(f"{settings.agent_linker}{settings.agent_link_k}")
    return "".join(parts)


def report_name(settings: Settings, provider: Provider) -> str:
    if settings.provider == "fake":
        return "warehouse-replay.json"
    safe = provider.model.replace("/", "-").replace(":", "-")
    suffix = f"-{variant(settings)}" if variant(settings) else ""
    return f"warehouse-{settings.provider}-{safe}{suffix}.json"


def limits_of(settings: Settings) -> dict[str, object]:
    """The planner settings a report was produced with (what a replay must use)."""
    return {
        "values": settings.agent_values,
        "examples": settings.agent_examples,
        "votes": settings.agent_votes,
        "linker": settings.agent_linker,
        "link_k": settings.agent_link_k,
        "max_repairs": settings.agent_max_repairs,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--provider", choices=["fake", "ollama", "anthropic"])
    parser.add_argument("--record", action="store_true", help="store every model call")
    parser.add_argument(
        "--record-executions",
        action="store_true",
        help=f"store every database execution in {EXECUTIONS.relative_to(REPO_ROOT)}",
    )
    parser.add_argument("--only", action="append", default=[], help="question id (repeatable)")
    parser.add_argument("--questions", type=Path, default=QUESTIONS)
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args(argv)

    settings = get_settings()
    if args.provider:
        settings = settings.model_copy(update={"provider": args.provider})
    configure_logging(settings.log_format, settings.log_level)
    bind_run_id("eval-warehouse")
    from tributary.agent.providers import make_provider

    provider = make_provider(settings, record=args.record)
    agent = build_agent(settings, provider=provider)
    recorder = Recorder(EXECUTIONS) if args.record_executions else None
    if recorder is not None:
        agent.executor = RecordingExecutor(agent.executor, DATABASE, recorder)
    questions = [q for q in load_questions(args.questions) if not args.only or q.id in args.only]
    verdicts = []
    for q in questions:
        try:
            verdict = evaluate_question(agent, agent.executor, q)
        except ExecutionError as exc:  # the gold failed: the question set is broken, stop
            print(f"{q.id}: gold query failed: {exc}", file=sys.stderr)
            return 2
        if recorder is not None:
            recorder.save()  # after every question, so an interrupted run keeps its work
        verdicts.append(verdict)
        mark = "ok " if verdict.correct else "BAD"
        print(f"{mark} {q.id:<45} {verdict.status:<12} attempts={verdict.attempts}", flush=True)
    summary = summarise(verdicts)
    report = {
        "provider": settings.provider,
        "model": provider.model,
        "questions": str(args.questions.relative_to(REPO_ROOT))
        if args.questions.is_relative_to(REPO_ROOT)
        else str(args.questions),
        "limits": limits_of(settings),
        "summary": summary,
        "verdicts": [asdict(v) for v in verdicts],
    }
    out = args.out or RESULTS / report_name(settings, provider)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False) + "\n")
    print(
        f"\n{summary['correct']}/{summary['total']} correct "
        f"({summary['repaired']} after a repair), {summary['model_calls']} model calls, "
        f"{summary['input_tokens']} in / {summary['output_tokens']} out tokens, "
        f"${summary['cost_usd']}, p50 {summary['latency_ms_p50']} ms, "
        f"p95 {summary['latency_ms_p95']} ms → {_display(out)}"
    )
    return 0


def _display(path: Path) -> str:
    return str(path.relative_to(REPO_ROOT)) if path.is_relative_to(REPO_ROOT) else str(path)


if __name__ == "__main__":
    raise SystemExit(main())
