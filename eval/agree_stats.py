"""The statistics behind M13's agreement test: Fisher's exact test, the rule, the simulation.

A question is *agreed* when two passes of the agent answered it with the same
rows, *differed* otherwise. Graded against the gold answer, that gives a
2 x 2 table::

                 right   wrong
    agreed         a       b
    differed       c       d

The hypothesis is that the graded answer is right more often when the passes
agree: the one-sided Fisher exact p-value is the probability, with the
margins fixed, of an ``a`` at least as large as the one seen if agreement and
correctness were independent (the hypergeometric upper tail). Pure Python on
:func:`math.comb`, so it is exact at any n this project sees.

The rule (PLAN.md, M13, fixed before any model call) reads the table three
ways: ``transfers`` at p < 0.05; otherwise ``not seen`` when the gap between
the two accuracies is under 15 points (a small effect n = 40 cannot resolve,
not evidence that there is none); otherwise ``inconclusive``.
"""

from __future__ import annotations

import math
import random
from collections import Counter
from dataclasses import dataclass

ALPHA = 0.05
GAP_POINTS = 15.0


@dataclass(frozen=True, slots=True)
class Table:
    """Agreement against correctness: right/wrong among agreed and among differed."""

    agreed_right: int
    agreed_wrong: int
    differed_right: int
    differed_wrong: int

    @property
    def agreed(self) -> int:
        return self.agreed_right + self.agreed_wrong

    @property
    def differed(self) -> int:
        return self.differed_right + self.differed_wrong

    @property
    def right(self) -> int:
        return self.agreed_right + self.differed_right

    @property
    def n(self) -> int:
        return self.agreed + self.differed

    @property
    def gap_points(self) -> float | None:
        """Accuracy when agreed minus accuracy when differed, in points (None if a row is empty)."""
        if not self.agreed or not self.differed:
            return None
        return 100.0 * (self.agreed_right / self.agreed - self.differed_right / self.differed)

    @property
    def p(self) -> float:
        return fisher_greater(self.agreed_right, self.agreed, self.right, self.n)


def fisher_greater(a: int, row: int, column: int, n: int) -> float:
    """P(X >= a) for X hypergeometric: ``row`` drawn from ``n`` of which ``column`` are right."""
    if not 0 <= a <= row <= n or not 0 <= column <= n:
        raise ValueError(f"not a 2x2 table: a={a} row={row} column={column} n={n}")
    total = math.comb(n, row)
    high = min(row, column)
    tail = sum(math.comb(column, k) * math.comb(n - column, row - k) for k in range(a, high + 1))
    return tail / total


def verdict(table: Table) -> str:
    """The pre-registered reading of a table: transfers, not seen or inconclusive."""
    if table.p < ALPHA:
        return "transfers"
    gap = table.gap_points
    if gap is None or gap < GAP_POINTS:
        return "not seen"
    return "inconclusive"


def needed(agreed: int, right: int, n: int) -> int | None:
    """The fewest right answers among ``agreed`` that reach p < 0.05 (None if none does)."""
    for a in range(max(0, agreed - (n - right)), min(agreed, right) + 1):
        if fisher_greater(a, agreed, right, n) < ALPHA:
            return a
    return None


def simulate(
    labels: list[bool],
    p_agree_right: float,
    p_agree_wrong: float,
    *,
    runs: int = 100_000,
    seed: int = 13,
) -> dict[str, float]:
    """Share of runs per verdict, with correctness fixed and agreement drawn per question.

    ``labels`` is the graded pass's right/wrong per question (known before the
    second pass runs); a right answer is agreed with probability
    ``p_agree_right``, a wrong one with ``p_agree_wrong``.
    """
    rng = random.Random(seed)  # noqa: S311 (a seeded simulation, not a secret)
    counts: Counter[str] = Counter()
    for _ in range(runs):
        ar = aw = dr = dw = 0
        for right in labels:
            agreed = rng.random() < (p_agree_right if right else p_agree_wrong)
            if right:
                ar, dr = (ar + 1, dr) if agreed else (ar, dr + 1)
            else:
                aw, dw = (aw + 1, dw) if agreed else (aw, dw + 1)
        counts[verdict(Table(ar, aw, dr, dw))] += 1
    return {k: counts[k] / runs for k in ("transfers", "inconclusive", "not seen")}


def rates_from(table: Table) -> tuple[float, float]:
    """P(agreed | right) and P(agreed | wrong) in a table (the simulation's inputs)."""
    right = table.agreed_right + table.differed_right
    wrong = table.agreed_wrong + table.differed_wrong
    return table.agreed_right / right, table.agreed_wrong / wrong


__all__ = [
    "ALPHA",
    "GAP_POINTS",
    "Table",
    "fisher_greater",
    "needed",
    "rates_from",
    "simulate",
    "verdict",
]
