"""Paired comparisons between two variants, on one subset or pooled over several.

    uv run python -m eval.bird.paired full+a full+v+f+a [--subsets dev50,holdout50]
        [--tracks official,corrected] [--model qwen2.5-coder:7b]

Two variants graded on the same questions are compared question by question:
``gained`` counts the questions only the second gets right, ``lost`` the
questions only the first gets right, and the exact two-sided McNemar p-value
treats those discordant questions as a fair coin's flips. The questions both
get right or both get wrong say nothing about the difference, which is why
this test has more power at n = 50 than comparing two accuracies.

Pooling: each subset is a separate run of the same model, so its verdicts are
keyed by (subset, question id) and the discordant counts simply add. The
subsets are disjoint by construction (``eval/bird/data.py``), so no question
is counted twice.

This module also holds M10's decision rule for ``make eval``'s live default.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from tributary.config import REPO_ROOT

RESULTS = REPO_ROOT / "eval" / "results"

# The pairings M10 reports: each feature set against the baseline, and the M7 default
# against the variant it has to beat.
PAIRINGS: tuple[tuple[str, str], ...] = (
    ("full", "full+a"),
    ("full", "full+v+f+a"),
    ("full+a", "full+v+f+a"),
)
# The decision rule, fixed before the holdout50 run (RESULTS.md, M10): the candidate stays
# the live default only if it is at least level with the challenger on every track of the
# held-out subset; otherwise the challenger becomes the default.
CANDIDATE = "full+v+f+a"
CHALLENGER = "full+a"
DECIDING_SUBSET = "holdout50"
# The run the rule was stated for. A later run of the same subset on another model (a Claude
# run, say) is reported beside it but does not decide the default.
DECIDING_MODEL = "qwen2.5-coder:7b"
DECIDING_PROVIDER = "ollama"


def mcnemar_exact(gained: int, lost: int) -> float:
    """Two-sided exact McNemar p-value: the discordant pairs as a fair coin's flips."""
    n = gained + lost
    if n == 0:
        return 1.0
    tail: float = sum(math.comb(n, i) for i in range(min(gained, lost) + 1)) / 2**n
    return min(1.0, 2 * tail)


@dataclass(frozen=True, slots=True)
class Pairing:
    """``other`` against ``baseline`` on the same questions of one track."""

    baseline: str
    other: str
    track: str
    subsets: tuple[str, ...]
    n: int
    baseline_correct: int
    other_correct: int
    gained: int  # right only with ``other``
    lost: int  # right only with ``baseline``

    @property
    def p(self) -> float:
        return mcnemar_exact(self.gained, self.lost)

    @property
    def net(self) -> int:
        return self.gained - self.lost


class PairingError(ValueError):
    """The two variants were not graded on the same questions."""


def _verdicts(report: dict[str, Any], variant: str, track: str) -> dict[int, bool]:
    # Any: a JSON report written by eval.bird.run.
    try:
        entry = report["variants"][variant][track]
    except KeyError as exc:
        raise PairingError(
            f"the {report.get('subset')} run has no {variant} on the {track} track"
        ) from exc
    return {int(v["id"]): bool(v["correct"]) for v in entry["verdicts"]}


def pair(reports: Sequence[dict[str, Any]], baseline: str, other: str, track: str) -> Pairing:
    """``other`` against ``baseline`` on ``track``, pooled over ``reports`` (one per subset)."""
    if not reports:
        raise PairingError("no report to compare in")
    subsets = [str(r["subset"]) for r in reports]
    if len(set(subsets)) != len(subsets):
        raise PairingError(f"a subset appears twice: {subsets}")
    n = base_ok = other_ok = gained = lost = 0
    seen: set[int] = set()
    for report in reports:
        base = _verdicts(report, baseline, track)
        mine = _verdicts(report, other, track)
        if set(base) != set(mine):
            raise PairingError(
                f"{baseline} and {other} were graded on different {track} questions "
                f"in the {report['subset']} run"
            )
        if seen & set(base):
            raise PairingError(f"question {min(seen & set(base))} is in two subsets")
        seen |= set(base)
        for qid, b in base.items():
            o = mine[qid]
            n += 1
            base_ok += b
            other_ok += o
            gained += o and not b
            lost += b and not o
    return Pairing(baseline, other, track, tuple(subsets), n, base_ok, other_ok, gained, lost)


def accuracy(reports: Sequence[dict[str, Any]], variant: str, track: str) -> tuple[int, int]:
    """Correct answers and questions for ``variant`` on ``track``, pooled over ``reports``."""
    correct = total = 0
    for report in reports:
        verdicts = _verdicts(report, variant, track)
        correct += sum(verdicts.values())
        total += len(verdicts)
    return correct, total


@dataclass(frozen=True, slots=True)
class Decision:
    default: str  # the variant make eval runs beside full
    kept: bool  # the candidate stayed
    by_track: tuple[tuple[str, int, int], ...]  # (track, candidate correct, challenger correct)


def decide(holdout: dict[str, Any], tracks: Sequence[str] = ("official", "corrected")) -> Decision:
    """The rule fixed before the run, applied to the held-out subset alone."""
    if holdout.get("subset") != DECIDING_SUBSET:
        raise PairingError(f"the rule is decided on {DECIDING_SUBSET}, not {holdout.get('subset')}")
    by_track = []
    for track in tracks:
        candidate, _ = accuracy([holdout], CANDIDATE, track)
        challenger, _ = accuracy([holdout], CHALLENGER, track)
        by_track.append((track, candidate, challenger))
    kept = all(c >= ch for _, c, ch in by_track)
    return Decision(CANDIDATE if kept else CHALLENGER, kept, tuple(by_track))


def deciding_run(
    reports: Sequence[dict[str, Any]],
    model: str = DECIDING_MODEL,
    provider: str = DECIDING_PROVIDER,
) -> dict[str, Any] | None:
    """The held-out run the rule was stated for, or ``None`` before it is recorded."""
    found = [
        r
        for r in reports
        if r.get("subset") == DECIDING_SUBSET
        and r.get("model") == model
        and r.get("provider") == provider
    ]
    if len(found) > 1:
        raise PairingError(f"{len(found)} {DECIDING_SUBSET} runs of {provider} {model}")
    return found[0] if found else None


def _load_reports(model: str, subsets: Sequence[str], results: Path) -> list[dict[str, Any]]:
    loaded = []
    for subset in subsets:
        matches = [
            json.loads(p.read_text(encoding="utf-8"))
            for p in sorted(results.glob(f"bird-{subset}-*.json"))
        ]
        matches = [r for r in matches if r.get("model") == model and r.get("subset") == subset]
        if not matches:
            raise PairingError(f"no {subset} run of {model} in {results}")
        loaded.append(matches[0])
    return loaded


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("baseline")
    parser.add_argument("other")
    parser.add_argument("--subsets", default="dev50,holdout50", help="pooled when several")
    parser.add_argument("--tracks", default="official,corrected")
    parser.add_argument("--model", default="qwen2.5-coder:7b")
    parser.add_argument("--results", type=Path, default=RESULTS)
    args = parser.parse_args(argv)
    subsets = [s for s in args.subsets.split(",") if s]
    try:
        reports = _load_reports(args.model, subsets, args.results)
        lines = [f"{args.other} against {args.baseline}, {args.model}"]
        for subset_reports in [[r] for r in reports] + ([reports] if len(reports) > 1 else []):
            label = " + ".join(r["subset"] for r in subset_reports)
            for track in (t for t in args.tracks.split(",") if t):
                p = pair(subset_reports, args.baseline, args.other, track)
                lines.append(
                    f"  {label:<18} {track:<9} n={p.n:<3} {p.baseline_correct:>3} -> "
                    f"{p.other_correct:<3} +{p.gained} -{p.lost}  p={p.p:.3f}"
                )
    except PairingError as exc:
        print(exc, file=sys.stderr)
        return 2
    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    sys.exit(main())
