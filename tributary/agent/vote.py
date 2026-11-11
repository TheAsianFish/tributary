"""Self-consistency: ask several times, run every query, return the answer most runs agree on.

A text-to-SQL model that is unsure tends to be unsure in different ways on
different draws: one draw joins the wrong table, another forgets a filter.
Two queries that are both right usually return the same rows even when the
SQL differs, so the vote is on what each query *returned*, not on its text.
Each draw is a full planner run (draft, guard, execute, repairs); a draw that
ends without rows (refused, blocked, failed) casts no vote.

Draw 0 is the ordinary deterministic call, so a single-draw planner and a
voting planner share it (and its recording); draws 1..n-1 are sampled at
``temperature``. Voting stops early once the leading answer can no longer be
overtaken by the draws that remain: with three draws, two that agree settle
it and the third is never asked for.

The returned outcome is the earliest draw in the winning group, carrying the
total usage and latency of every draw made, and every draw in ``samples``.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import time
from collections.abc import Sequence
from datetime import date, datetime, timedelta
from datetime import time as dtime
from decimal import Decimal

import structlog

from tributary.agent.catalog import Catalog
from tributary.agent.execute import QueryResult
from tributary.agent.guard import Guard
from tributary.agent.linker import Link
from tributary.agent.planner import Planner, PlanOutcome
from tributary.agent.provider import Usage

log = structlog.get_logger(__name__)

DEFAULT_TEMPERATURE = 0.7


def _canonical(value: object) -> object:
    """A JSON-able stand-in for one cell, equal for values a reader would call equal.

    Numbers of any type compare by value to 9 significant digits (two correct
    queries may sum floats in a different order, or return 2 where the other
    returns 2.0); everything else by its text.
    """
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, int | float | Decimal):
        number = float(value)
        return float(f"{number:.9g}") if number == number else "NaN"
    if isinstance(value, datetime | date | dtime | timedelta):
        return value.isoformat() if not isinstance(value, timedelta) else str(value)
    if isinstance(value, bytes | bytearray | memoryview):
        return bytes(value).hex()
    return str(value)


def result_key(result: QueryResult) -> str:
    """Which answer a result is: its distinct rows, in any order, and its width."""
    rows = sorted(
        {
            json.dumps([_canonical(v) for v in row], sort_keys=True, default=str)
            for row in result.rows
        }
    )
    document = json.dumps([len(result.columns), rows], separators=(",", ":"))
    return hashlib.sha256(document.encode("utf-8")).hexdigest()


@dataclasses.dataclass(frozen=True, slots=True)
class Tally:
    """The vote: which draw won, how many draws agreed with it, and the group sizes."""

    winner: int | None  # index of the earliest draw in the largest group; None: no votes
    agreeing: int
    groups: tuple[int, ...]  # group sizes in order of first appearance


def tally(results: Sequence[QueryResult | None]) -> Tally:
    """Group the results by :func:`result_key`; the largest group wins, ties to the earliest."""
    first: dict[str, int] = {}
    size: dict[str, int] = {}
    for i, result in enumerate(results):
        if result is None:
            continue
        key = result_key(result)
        first.setdefault(key, i)
        size[key] = size.get(key, 0) + 1
    if not size:
        return Tally(None, 0, ())
    order = sorted(size, key=lambda k: first[k])
    best = max(order, key=lambda k: (size[k], -first[k]))
    return Tally(first[best], size[best], tuple(size[k] for k in order))


def settled(results: Sequence[QueryResult | None], remaining: int) -> bool:
    """Whether ``remaining`` more draws could still change the winner."""
    groups = sorted(tally(results).groups, reverse=True)
    if not groups:
        return False
    runner_up = groups[1] if len(groups) > 1 else 0
    # A tie goes to the earlier group, so the leader keeps the win at equal size.
    return groups[0] >= runner_up + remaining


def _rows(outcome: PlanOutcome) -> QueryResult | None:
    return outcome.result if outcome.status == "answered" else None


class VotingPlanner:
    """A :class:`Planner` asked up to ``n`` times per question, answering by majority result."""

    def __init__(
        self,
        planner: Planner,
        n: int = 3,
        *,
        temperature: float = DEFAULT_TEMPERATURE,
        early_stop: bool = True,
    ) -> None:
        if n < 1:
            raise ValueError(f"n must be at least 1, got {n}")
        self.planner = planner
        self.n = n
        self.temperature = temperature
        self.early_stop = early_stop

    @property
    def catalog(self) -> Catalog:
        return self.planner.catalog

    @property
    def guard(self) -> Guard:
        return self.planner.guard

    def system_for(self, question: str) -> tuple[str, Link | None]:
        return self.planner.system_for(question)

    def user_message(self, question: str) -> str:
        return self.planner.user_message(question)

    def run(self, question: str) -> PlanOutcome:
        started = time.perf_counter()
        draws: list[PlanOutcome] = []
        for i in range(self.n):
            draws.append(
                self.planner.run(
                    question, sample=i, temperature=None if i == 0 else self.temperature
                )
            )
            results = [_rows(d) for d in draws]
            if self.early_stop and settled(results, self.n - len(draws)):
                break
        vote = tally([_rows(d) for d in draws])
        chosen = draws[vote.winner if vote.winner is not None else 0]
        usage = Usage()
        for draw in draws:
            usage = usage + draw.usage
        latency = (time.perf_counter() - started) * 1000
        log.info(
            "planner.vote",
            draws=len(draws),
            agreeing=vote.agreeing,
            groups=list(vote.groups),
            cost_usd=round(usage.cost_usd, 6),
        )
        return dataclasses.replace(
            chosen,
            usage=usage,
            latency_ms=latency,
            calls=sum(d.calls for d in draws),
            samples=tuple(draws),
            extra={**chosen.extra, "draws": len(draws), "agreeing": vote.agreeing},
        )


__all__ = ["DEFAULT_TEMPERATURE", "Tally", "VotingPlanner", "result_key", "settled", "tally"]
