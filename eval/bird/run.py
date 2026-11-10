"""``make eval``: the agent on BIRD Mini-Dev, graded by execution accuracy, by variant and track.

    uv run python -m eval.bird.run [--provider fake|ollama|anthropic] [--record]
        [--variants full,bm25-k4] [--tracks official,corrected] [--subset dev50|all]

Each question goes through the product's own path: the planner (structured
draft, at most two repairs), the guard (with the database's tables as the
allowlist) and an executor running as the read-only ``tributary_bird`` role.
The predicted rows are compared with the gold query's rows by BIRD's rule
(:mod:`eval.bird.score`).

**Variants.** A variant is a linking strategy (``full``, ``bm25-k4``, ``core``)
run with up to two repairs. Its no-repair twin (``-r0``) is read off the same
run: the first model call of a question is the same request whether or not
repairs follow, so the first attempt's outcome *is* the zero-repair result,
at the first call's cost. One run gives both rows.

**Tracks.** ``official`` grades against BIRD's PostgreSQL gold, ``corrected``
against the verified transpilation of the Arcwise corrections. A question
whose wording is the same in both tracks is answered once and graded twice.

**Recording and replay.** ``--record`` stores every model call (by request
fingerprint) and every database execution (by database and SQL), so
``--provider fake`` replays the whole run offline: no model, no database.
A replay then checks itself against the recorded report and fails on any
difference in a verdict, which is what CI runs.

**Live runs** also grade every (predicted, gold) pair with BIRD's own
``calculate_ex`` on the rows as fetched and report any disagreement with
this scorer.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import platform
import statistics
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from eval.bird import data
from eval.bird.load import bird_conninfo, load_catalogs
from eval.bird.prompt import BIRD_RULES
from eval.bird.recorded import Recorder, RecordingExecutor, ReplayExecutor, load_executions
from eval.bird.score import (
    ScorerUnavailable,
    cross_check,
    execution_match,
    lenient_match,
    official_calculate_ex,
    raw_rows,
    strict_match,
)
from tributary.agent.catalog import Catalog
from tributary.agent.execute import ExecutionError, Executor, PostgresExecutor, QueryResult
from tributary.agent.guard import Guard
from tributary.agent.linker import DEFAULT_K, Link, LinkerUnavailable
from tributary.agent.planner import Planner, PlanOutcome
from tributary.agent.provider import (
    ModelRequest,
    ModelResponse,
    Provider,
    RecordingMissing,
    Usage,
)
from tributary.agent.providers import pricing
from tributary.agent.service import linker_for
from tributary.config import REPO_ROOT, Settings, get_settings
from tributary.logging import bind_run_id, configure_logging

RESULTS = REPO_ROOT / "eval" / "results"
RECORDINGS = REPO_ROOT / "tests" / "recordings"
EXECUTIONS = RECORDINGS / "bird-executions.jsonl.gz"
TIMEOUT_MS = 30_000  # BIRD's evaluation_ex.py meta_time_out
ROW_CAP = 100_000
MAX_REPAIRS = 2
# Models a live Claude run would use, for the cost estimate in the ledger.
ESTIMATE_MODELS = ("claude-haiku-5-5", "claude-opus-5-5")


# --- variants ---------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Variant:
    linker: str
    k: int | None = None

    @property
    def name(self) -> str:
        return self.linker if self.k is None else f"{self.linker}-k{self.k}"

    @classmethod
    def parse(cls, text: str) -> Variant:
        linker, _, k = text.partition("-k")
        if linker not in ("full", "bm25", "core"):
            raise ValueError(f"unknown variant {text!r}: full, bm25-k<N> or core[-k<N>]")
        if linker == "full" and k:
            raise ValueError(f"unknown variant {text!r}: full takes no k")
        if k and (not k.isdigit() or int(k) < 1):
            raise ValueError(f"unknown variant {text!r}: k must be a positive number")
        return cls(linker, int(k) if k else (DEFAULT_K if linker == "bm25" else None))

    def linker_fn(self) -> Callable[[str, Catalog], Link] | None:
        """``None`` for ``full``: the whole catalog, one prompt prefix per database."""
        return linker_for(self.linker, self.k or DEFAULT_K)


# --- one question -----------------------------------------------------------------------------


@dataclass(slots=True)
class Prediction:
    """One planner run, shared by every track that asks the same question."""

    outcome: PlanOutcome
    rows: tuple[tuple[object, ...], ...] | None
    first_ok: bool  # the first attempt's query ran (so the no-repair variant has these rows)
    # Variants that have counted this prediction's cost: a variant pays once per prediction
    # (as if run alone), however many tracks grade it.
    paid_by: set[str] = field(default_factory=set)


@dataclass(frozen=True, slots=True)
class Verdict:
    id: int
    db_id: str
    difficulty: str
    status: str
    correct: bool
    correct_r0: bool
    lenient: bool
    strict: bool
    attempts: int
    sql: str | None
    error: str | None
    input_tokens: int
    output_tokens: int
    input_tokens_r0: int
    output_tokens_r0: int
    cost_usd: float
    cost_usd_r0: float
    model_ms: float
    model_ms_r0: float
    predicted_rows: int | None
    gold_rows: int
    linked: tuple[str, ...]
    reused: bool
    official_ex: bool | None = None


def wilson(correct: int, total: int, z: float = 1.959964) -> tuple[float, float]:
    """Wilson score interval for a proportion (95% by default)."""
    if total == 0:
        return (0.0, 0.0)
    p = correct / total
    denominator = 1 + z * z / total
    centre = (p + z * z / (2 * total)) / denominator
    half = z * math.sqrt(p * (1 - p) / total + z * z / (4 * total * total)) / denominator
    return (round(max(0.0, centre - half), 4), round(min(1.0, centre + half), 4))


@dataclass
class Harness:
    settings: Settings
    provider: Provider
    catalogs: dict[str, Catalog]
    replay: bool
    recorder: Recorder | None = None
    executions: dict[str, dict[str, Any]] = field(default_factory=dict)
    official: Callable[[Any, Any], int] | None = None
    _planners: dict[tuple[str, str], Planner] = field(default_factory=dict)
    _gold: dict[str, Executor] = field(default_factory=dict)
    _gold_results: dict[tuple[str, str], QueryResult] = field(default_factory=dict)
    _memo: dict[tuple[str, str, str], Prediction] = field(default_factory=dict)
    pairs: list[tuple[str, Any, Any]] = field(default_factory=list)

    def _executor(self, database: str, *, gold: bool) -> Executor:
        key = f"gold/{database}" if gold else database
        if self.replay:
            return ReplayExecutor(self.executions, key)
        live = PostgresExecutor(
            bird_conninfo(self.settings, as_reader=True),
            statement_timeout_ms=TIMEOUT_MS,
            row_cap=ROW_CAP,
            cost_ceiling=None,
            search_path=database if gold else "",
            # Serial plans: a parallel float4 SUM can differ in its last bits from run to
            # run, and BIRD compares values exactly.
            parallel=False,
        )
        return RecordingExecutor(live, key, self.recorder) if self.recorder else live

    def planner(self, db_id: str, variant: Variant) -> Planner:
        key = (db_id, variant.name)
        if key not in self._planners:
            catalog = self.catalogs[db_id]
            self._planners[key] = Planner(
                self.provider,
                Guard(catalog.relations(), max_rows=ROW_CAP),
                self._executor(db_id, gold=False),
                catalog,
                max_repairs=MAX_REPAIRS,
                row_cap=ROW_CAP,
                rules=BIRD_RULES,
                linker=variant.linker_fn(),
            )
        return self._planners[key]

    def gold(self, q: data.BirdQuestion) -> QueryResult:
        """The gold result, run once per run: every variant is graded on the same rows."""
        key = (q.db_id, q.sql)
        if key not in self._gold_results:
            executor = self._gold.get(q.db_id)
            if executor is None:
                executor = self._gold[q.db_id] = self._executor(q.db_id, gold=True)
            self._gold_results[key] = executor.run(q.sql)
        return self._gold_results[key]

    def predict(
        self, q: data.BirdQuestion, variant: Variant
    ) -> tuple[Prediction, bool, tuple[str, ...]]:
        """The planner's answer, the model asked once per distinct request.

        Keyed by the system prompt and the question, not by variant or track: on a
        database with at most k tables bm25 shows exactly what full shows, and a
        second, identical request could get a different reply than the first while
        the recording keeps only one. Returns the prediction, whether this variant
        has already counted its cost, and the tables the variant linked.
        """
        planner = self.planner(q.db_id, variant)
        system, link = planner.system_for(q.prompt)
        key = (q.db_id, hashlib.sha256(system.encode()).hexdigest(), q.prompt)
        prediction = self._memo.get(key)
        if prediction is None:
            outcome = planner.run(q.prompt)
            rows = outcome.result.rows if outcome.result is not None else None
            first = outcome.attempts[0] if outcome.attempts else None
            first_ok = bool(first and first.error is None and outcome.status == "answered")
            first_ok = first_ok and len(outcome.attempts) == 1
            prediction = self._memo[key] = Prediction(outcome, rows, first_ok)
        reused = variant.name in prediction.paid_by
        prediction.paid_by.add(variant.name)
        linked = link.tables if link else self.catalogs[q.db_id].relations()
        return prediction, reused, tuple(linked)

    def grade(self, q: data.BirdQuestion, variant: Variant) -> Verdict:
        gold = self.gold(q)
        prediction, reused, linked = self.predict(q, variant)
        outcome = prediction.outcome
        rows = prediction.rows
        correct = rows is not None and execution_match(rows, gold.rows)
        official_ex = None
        if rows is not None and not self.replay:
            label = f"{variant.name}/{q.track}/{q.id}"
            self.pairs.append((label, rows, gold.rows))
            if self.official is not None:
                official_ex = bool(self.official(raw_rows(rows), raw_rows(gold.rows)))
        first = outcome.attempts[0] if outcome.attempts else None
        first_usage = first.usage if first else Usage()
        return Verdict(
            id=q.id,
            db_id=q.db_id,
            difficulty=q.difficulty,
            status=outcome.status,
            correct=correct,
            correct_r0=correct and prediction.first_ok,
            lenient=rows is not None and lenient_match(rows, gold.rows),
            strict=rows is not None and strict_match(rows, gold.rows),
            attempts=len(outcome.attempts),
            sql=outcome.sql or (outcome.draft.sql if outcome.draft else None),
            error=outcome.error,
            input_tokens=outcome.usage.input_tokens,
            output_tokens=outcome.usage.output_tokens,
            input_tokens_r0=first_usage.input_tokens,
            output_tokens_r0=first_usage.output_tokens,
            cost_usd=round(outcome.usage.cost_usd, 6),
            cost_usd_r0=round(first_usage.cost_usd, 6),
            model_ms=round(sum(a.model_ms for a in outcome.attempts), 1),
            model_ms_r0=round(first.model_ms, 1) if first else 0.0,
            predicted_rows=None if rows is None else len(rows),
            gold_rows=len(gold.rows),
            linked=linked,
            reused=reused,
            official_ex=official_ex,
        )


class Resume:
    """Recorded replies where they exist, the live (recording) provider for the rest."""

    def __init__(self, recorded: Provider, live: Provider) -> None:
        self.recorded = recorded
        self.live = live
        self.name = live.name
        self.model = live.model
        self.recorded_calls = 0
        self.live_calls = 0

    def complete(self, request: ModelRequest) -> ModelResponse:
        try:
            response = self.recorded.complete(request)
        except RecordingMissing:
            self.live_calls += 1
            return self.live.complete(request)
        self.recorded_calls += 1
        return response


# --- summaries --------------------------------------------------------------------------------


def _percentile(values: Sequence[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, max(0, math.ceil(q * len(ordered)) - 1))]


def summarise(verdicts: Sequence[Verdict], *, repairs: bool) -> dict[str, Any]:
    """EX with its interval, by difficulty and database, and what it cost (Any: JSON)."""
    ok = [(v.correct if repairs else v.correct_r0) for v in verdicts]
    n, correct = len(verdicts), sum(ok)
    by_difficulty: dict[str, dict[str, int]] = {}
    by_database: dict[str, dict[str, int]] = {}
    for v, good in zip(verdicts, ok, strict=True):
        for table, key in ((by_difficulty, v.difficulty), (by_database, v.db_id)):
            entry = table.setdefault(key, {"correct": 0, "total": 0})
            entry["total"] += 1
            entry["correct"] += int(good)
    fresh = [v for v in verdicts if not v.reused]
    tokens_in = sum(v.input_tokens if repairs else v.input_tokens_r0 for v in fresh)
    tokens_out = sum(v.output_tokens if repairs else v.output_tokens_r0 for v in fresh)
    latencies = [v.model_ms if repairs else v.model_ms_r0 for v in verdicts]
    return {
        "correct": correct,
        "total": n,
        "ex": round(correct / n, 4) if n else 0.0,
        "ex_wilson95": list(wilson(correct, n)),
        "lenient": sum(v.lenient for v in verdicts) if repairs else None,
        "strict": sum(v.strict for v in verdicts) if repairs else None,
        "by_difficulty": {
            d: by_difficulty[d] for d in (*data.DIFFICULTIES, "unknown") if d in by_difficulty
        },
        "by_database": dict(sorted(by_database.items())),
        "by_status": _count(v.status for v in verdicts) if repairs else None,
        "repaired_into_correct": sum(v.correct and not v.correct_r0 for v in verdicts)
        if repairs
        else 0,
        "model_calls": sum(v.attempts if repairs else 1 for v in fresh),
        "input_tokens": tokens_in,
        "output_tokens": tokens_out,
        "cost_usd": round(sum(v.cost_usd if repairs else v.cost_usd_r0 for v in fresh), 6),
        "model_ms_p50": round(statistics.median(latencies), 1) if latencies else 0.0,
        "model_ms_p95": round(_percentile(latencies, 0.95), 1),
    }


def _count(items: Any) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        counts[item] = counts.get(item, 0) + 1
    return dict(sorted(counts.items()))


def estimate_cost(input_tokens: int, output_tokens: int, model: str) -> float | None:
    """List price of the measured token counts on a Claude model, with no cache hits."""
    cost = pricing.cost(model, input_tokens=input_tokens, output_tokens=output_tokens)
    return None if cost is None else round(cost, 4)


# --- the run ----------------------------------------------------------------------------------


def run(
    harness: Harness,
    variants: Sequence[Variant],
    tracks: Sequence[data.Track],
    subset: str,
    log: Callable[[str], None] = print,
    limit: int | None = None,
) -> dict[str, Any]:
    questions = {t: data.select(data.questions(t), subset)[:limit] for t in tracks}
    report: dict[str, Any] = {"variants": {}}
    for variant in variants:
        per_track: dict[str, Any] = {}
        for track in tracks:
            # Grouped by database so a local model reuses its cached prompt prefix.
            ordered = sorted(questions[track], key=lambda q: (q.db_id, q.id))
            verdicts = []
            for q in ordered:
                verdict = harness.grade(q, variant)
                verdicts.append(verdict)
                mark = "ok " if verdict.correct else "BAD"
                note = " (reused)" if verdict.reused else ""
                log(
                    f"{mark} {variant.name:<8} {track:<9} {q.id:>5} {q.db_id:<24} "
                    f"{verdict.status:<12} attempts={verdict.attempts}{note}"
                )
            per_track[track] = {
                "r2": summarise(verdicts, repairs=True),
                "r0": summarise(verdicts, repairs=False),
                "verdicts": [asdict(v) for v in verdicts],
            }
        report["variants"][variant.name] = per_track
    return report


def ledger(report: dict[str, Any]) -> list[dict[str, Any]]:
    """What each variant cost, measured, and what it would cost on Claude at list price."""
    rows = []
    for name, tracks in report["variants"].items():
        for repairs in ("r2", "r0"):
            calls = tokens_in = tokens_out = 0
            cost = 0.0
            for track in tracks.values():
                s = track[repairs]
                calls += s["model_calls"]
                tokens_in += s["input_tokens"]
                tokens_out += s["output_tokens"]
                cost += s["cost_usd"]
            rows.append(
                {
                    "variant": f"{name}-{repairs}",
                    "model_calls": calls,
                    "input_tokens": tokens_in,
                    "output_tokens": tokens_out,
                    "cost_usd": round(cost, 6),
                    "estimate_usd": {
                        m: estimate_cost(tokens_in, tokens_out, m) for m in ESTIMATE_MODELS
                    },
                }
            )
    return rows


def compare(replayed: dict[str, Any], recorded: dict[str, Any]) -> list[str]:
    """Differences in any verdict, summary or ledger row between a replay and the recording."""
    fields = ("status", "correct", "correct_r0", "lenient", "strict", "attempts", "sql")
    problems = []
    for name, tracks in recorded["variants"].items():
        for track, entry in tracks.items():
            want = {v["id"]: v for v in entry["verdicts"]}
            got_entry = replayed["variants"].get(name, {}).get(track)
            if got_entry is None:
                problems.append(f"{name}/{track}: not replayed")
                continue
            got = {v["id"]: v for v in got_entry["verdicts"]}
            for qid, w in want.items():
                g = got.get(qid)
                if g is None:
                    problems.append(f"{name}/{track}/{qid}: missing from the replay")
                    continue
                for f in fields:
                    if g[f] != w[f]:
                        problems.append(f"{name}/{track}/{qid}: {f} {g[f]!r} != {w[f]!r}")
            # RESULTS.md is rendered from the summaries, so they must follow from the verdicts
            # too. The official scorer needs the upstream file, which a replay does not load.
            for repairs in ("r0", "r2"):
                g_sum, w_sum = got_entry.get(repairs, {}), entry.get(repairs, {})
                for key in sorted((set(g_sum) | set(w_sum)) - {"official_ex"}):
                    if g_sum.get(key) != w_sum.get(key):
                        problems.append(
                            f"{name}/{track}/{repairs}: {key} "
                            f"{g_sum.get(key)!r} != {w_sum.get(key)!r}"
                        )
    if replayed.get("ledger") != recorded.get("ledger"):
        problems.append("ledger differs from the recorded run")
    return problems


def report_path(provider: str, model: str, subset: str) -> Path:
    safe = model.replace("/", "-").replace(":", "-")
    return RESULTS / f"bird-{subset}-{provider}-{safe}.json"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--provider", choices=["fake", "ollama", "anthropic"], default="fake")
    parser.add_argument("--model", default=None, help="model id (default: from settings)")
    parser.add_argument("--record", action="store_true")
    parser.add_argument("--variants", default="full,bm25-k4")
    parser.add_argument("--tracks", default="official,corrected")
    parser.add_argument("--subset", default="dev50")
    parser.add_argument(
        "--recorded",
        type=Path,
        default=RESULTS / "bird-dev50-ollama-qwen2.5-coder-7b.json",
        help="replay: the recorded report to replay and check against",
    )
    parser.add_argument("--recordings", type=Path, default=None, help="model recordings file")
    parser.add_argument("--executions", type=Path, default=EXECUTIONS)
    parser.add_argument("--out", type=Path, default=None)
    parser.add_argument("--limit", type=int, default=None, help="first N questions per track")
    parser.add_argument(
        "--resume",
        action="store_true",
        help="with --record: answer from the recordings where they exist, call the model "
        "only for requests never recorded (an interrupted or extended run)",
    )
    args = parser.parse_args(argv)
    variants = [Variant.parse(v) for v in args.variants.split(",") if v]
    for variant in variants:
        try:
            linker_for(variant.linker, variant.k or DEFAULT_K)
        except LinkerUnavailable as exc:
            print(f"variant {variant.name}: {exc}", file=sys.stderr)
            return 2

    settings = get_settings()
    replay = args.provider == "fake"
    recorded: dict[str, Any] | None = None
    if replay:
        recorded = json.loads(args.recorded.read_text(encoding="utf-8"))
        recordings = args.recordings or REPO_ROOT / recorded["recordings"]
    else:
        default = settings.ollama_model if args.provider == "ollama" else settings.model
        name = (args.model or default).replace("/", "-").replace(":", "-")
        recordings = args.recordings or RECORDINGS / f"bird-{name}.jsonl"
    update: dict[str, Any] = {"provider": args.provider, "recordings_path": recordings}
    if args.model:
        update["ollama_model" if args.provider == "ollama" else "model"] = args.model
    settings = settings.model_copy(update=update)
    configure_logging(settings.log_format, "WARNING")
    bind_run_id("eval-bird")

    from tributary.agent.providers import make_provider

    provider = make_provider(settings, record=args.record)
    resuming: Resume | None = None
    if args.resume:
        if replay or not args.record:
            parser.error("--resume continues a live recording: use it with --record")
        from tributary.agent.providers.fake import FakeProvider

        provider = resuming = Resume(FakeProvider(settings.recordings_path), provider)
    harness = Harness(settings, provider, load_catalogs(), replay=replay)
    if replay:
        harness.executions = load_executions(args.executions)
    else:
        if args.record:
            harness.recorder = Recorder(args.executions)
        try:
            harness.official = official_calculate_ex(settings.bird_cache_dir)
        except ScorerUnavailable as exc:
            print(f"warning: official scorer unavailable, cross-check skipped: {exc}")

    tracks = [t for t in args.tracks.split(",") if t]
    for t in tracks:
        if t not in data.TRACKS:
            parser.error(f"unknown track {t!r}")
    started = time.perf_counter()
    try:
        body = run(harness, variants, tracks, args.subset, limit=args.limit)
    except ExecutionError as exc:
        print(f"the database failed (is `make bird` loaded?): {exc.feedback()}", file=sys.stderr)
        return 2
    finally:
        if harness.recorder is not None:
            harness.recorder.save()
    wall = round(time.perf_counter() - started, 1)

    model = recorded["model"] if recorded else provider.model
    report: dict[str, Any] = {
        "provider": recorded["provider"] if recorded else args.provider,
        "model": model,
        "subset": args.subset,
        "tracks": tracks,
        "recordings": str(recordings.relative_to(REPO_ROOT))
        if recordings.is_relative_to(REPO_ROOT)
        else str(recordings),
        **body,
    }
    report["ledger"] = ledger(report)
    if not replay:
        check = cross_check(harness.pairs, harness.official) if harness.official else None
        report["cross_check"] = None if check is None else asdict(check)
        report["run"] = {
            "finished_at": datetime.now(UTC).isoformat(timespec="seconds"),
            "wall_seconds": wall,
            "machine": f"{platform.system()} {platform.machine()}, {_cpus()} CPUs",
        }
        if resuming is not None:
            report["run"]["model_calls_from_recordings"] = resuming.recorded_calls
            report["run"]["model_calls_live"] = resuming.live_calls
    out = args.out or (
        RESULTS / "bird-replay.json" if replay else report_path(args.provider, model, args.subset)
    )
    out.write_text(json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False) + "\n")
    print(render(report))
    print(f"\n{wall} s → {out.relative_to(REPO_ROOT) if out.is_relative_to(REPO_ROOT) else out}")
    if recorded is not None:
        problems = compare(report, recorded)
        if problems:
            print(f"\nreplay differs from {args.recorded.name}:", *problems[:20], sep="\n  ")
            return 1
        print(f"replay matches the recorded run ({args.recorded.name}) verdict for verdict")
    return 0


def _cpus() -> int:
    import os

    return os.cpu_count() or 0


def render(report: dict[str, Any]) -> str:
    lines = [f"{report['provider']} {report['model']} on {report['subset']}"]
    lines.append(
        f"{'variant':<12} {'track':<10} {'EX':>8} {'95% CI':>15} {'simple':>7} "
        f"{'moder.':>7} {'chall.':>7} {'calls':>6} {'tokens in/out':>16}"
    )
    for name, tracks in report["variants"].items():
        for repairs in ("r0", "r2"):
            for track, entry in tracks.items():
                s = entry[repairs]
                lo, hi = s["ex_wilson95"]
                d = s["by_difficulty"]

                def cell(key: str, d: dict[str, Any] = d) -> str:
                    e = d.get(key)
                    return f"{e['correct']}/{e['total']}" if e else "-"

                lines.append(
                    f"{name + '-' + repairs:<12} {track:<10} "
                    f"{s['correct']:>3}/{s['total']:<4} {lo:>6.3f}-{hi:<6.3f}  "
                    f"{cell('simple'):>7} {cell('moderate'):>7} {cell('challenging'):>7} "
                    f"{s['model_calls']:>6} {s['input_tokens']:>8}/{s['output_tokens']:<7}"
                )
    check = report.get("cross_check")
    if check:
        lines.append(f"official calculate_ex agrees on {check['agree']}/{check['pairs']} pairs")
    return "\n".join(lines)


if __name__ == "__main__":
    sys.exit(main())
