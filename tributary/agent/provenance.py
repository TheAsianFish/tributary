"""Where an answer's numbers come from: each table's last rebuild and its quality checks.

An answer is only as good as the tables it read, so it carries, for each one,
when dbt last rebuilt it (``ops.refresh_runs``) and the latest result of every
check on it *and on everything it is built from* (``ops.latest_quality``,
through the catalog's lineage). A mart whose own tests pass but whose source
failed its freshness check is not trustworthy, and the answer says so.

A table is

- ``fail`` when any of those checks last failed or errored,
- ``warn`` when one warned (and none failed),
- ``pass`` when there are results and all passed (skipped ones are ignored),
- ``unknown`` when there are none, which is reported, not treated as a pass;

and ``stale`` when its last successful rebuild is older than the limit, or it
has never been rebuilt.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Literal, Protocol

import psycopg

from tributary.agent.catalog import Table

Quality = Literal["pass", "warn", "fail", "unknown"]
_RANK: dict[str, int] = {"pass": 0, "warn": 1, "fail": 2, "error": 2}


@dataclass(frozen=True, slots=True)
class CheckResult:
    check_name: str
    check_type: str
    target: str
    status: str
    finished_at: datetime

    @property
    def short_name(self) -> str:
        """``freshness_fct_edits_ingested_at`` from dbt's ``test.project.name.hash``."""
        parts = self.check_name.split(".")
        return parts[2] if len(parts) >= 3 and parts[0] == "test" else self.check_name


@dataclass(frozen=True, slots=True)
class Refresh:
    relation: str
    status: str
    finished_at: datetime


@dataclass(frozen=True, slots=True)
class TableProvenance:
    relation: str
    last_refreshed_at: datetime | None
    stale: bool
    quality: Quality
    checks: int
    failing: tuple[str, ...] = ()
    warning: tuple[str, ...] = ()
    # Relations whose checks were counted: the table and its upstream.
    covers: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, object]:
        return {
            "relation": self.relation,
            "last_refreshed_at": self.last_refreshed_at.isoformat()
            if self.last_refreshed_at
            else None,
            "stale": self.stale,
            "quality": self.quality,
            "checks": self.checks,
            "failing": list(self.failing),
            "warning": list(self.warning),
            "covers": list(self.covers),
        }

    def warnings(self, stale_after: timedelta, now: datetime) -> list[str]:
        """Plain-language flags for the answer (empty when the table is fine)."""
        out = []
        if self.last_refreshed_at is None:
            out.append(f"{self.relation} has no recorded rebuild")
        elif self.stale:
            age = _human(now - self.last_refreshed_at)
            out.append(f"{self.relation} was last rebuilt {age} ago (limit {_human(stale_after)})")
        if self.failing:
            out.append(f"{self.relation}: failing checks: {', '.join(self.failing)}")
        if self.warning:
            out.append(f"{self.relation}: checks warning: {', '.join(self.warning)}")
        if self.quality == "unknown":
            out.append(f"{self.relation}: no quality results recorded")
        return out


def _human(delta: timedelta) -> str:
    minutes = int(delta.total_seconds() // 60)
    if minutes < 120:
        return f"{minutes} min"
    hours = minutes // 60
    return f"{hours} h" if hours < 48 else f"{hours // 24} d"


def summarise(
    table: Table,
    refresh: Refresh | None,
    checks: Iterable[CheckResult],
    *,
    stale_after: timedelta,
    now: datetime,
) -> TableProvenance:
    """One table's provenance from its latest successful rebuild and the checks (pure)."""
    covers = (table.relation, *table.upstream)
    relevant = sorted(
        (c for c in checks if c.target in covers and c.status != "skipped"),
        key=lambda c: c.check_name,
    )
    failing = tuple(c.short_name for c in relevant if _RANK.get(c.status, 2) == 2)
    warning = tuple(c.short_name for c in relevant if c.status == "warn")
    quality: Quality
    if not relevant:
        quality = "unknown"
    elif failing:
        quality = "fail"
    elif warning:
        quality = "warn"
    else:
        quality = "pass"
    refreshed = refresh.finished_at if refresh and refresh.status == "success" else None
    stale = refreshed is None or now - refreshed > stale_after
    return TableProvenance(
        relation=table.relation,
        last_refreshed_at=refreshed,
        stale=stale,
        quality=quality,
        checks=len(relevant),
        failing=failing,
        warning=warning,
        covers=covers,
    )


class ProvenanceSource(Protocol):
    def lookup(self, tables: Sequence[Table]) -> list[TableProvenance]: ...


class PostgresProvenance:
    """Reads ``ops.refresh_runs`` and ``ops.latest_quality`` (the agent's role may)."""

    def __init__(
        self,
        conninfo: str,
        *,
        stale_after: timedelta,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._conninfo = conninfo
        self.stale_after = stale_after
        self._clock = clock

    def lookup(self, tables: Sequence[Table]) -> list[TableProvenance]:
        if not tables:
            return []
        relations = sorted({t.relation for t in tables})
        targets = sorted({r for t in tables for r in (t.relation, *t.upstream)})
        try:
            refreshes, checks = self._read(relations, targets)
        except psycopg.errors.UndefinedTable:
            # No warehouse refresh has run here yet (ops.* missing): nothing is known,
            # and every table reports exactly that.
            refreshes, checks = {}, []
        now = self._clock()
        return [
            summarise(t, refreshes.get(t.relation), checks, stale_after=self.stale_after, now=now)
            for t in tables
        ]

    def _read(
        self, relations: list[str], targets: list[str]
    ) -> tuple[dict[str, Refresh], list[CheckResult]]:
        with psycopg.connect(self._conninfo, connect_timeout=5) as conn:
            conn.read_only = True
            refreshes = {
                r[0]: Refresh(r[0], r[1], r[2])
                for r in conn.execute(
                    "SELECT DISTINCT ON (relation) relation, status, finished_at"
                    " FROM ops.refresh_runs WHERE relation = ANY(%s) AND status = 'success'"
                    " ORDER BY relation, finished_at DESC",
                    (relations,),
                )
            }
            checks = [
                CheckResult(*row)
                for row in conn.execute(
                    "SELECT check_name, check_type, target, status, finished_at"
                    " FROM ops.latest_quality WHERE target = ANY(%s)",
                    (targets,),
                )
            ]
        return refreshes, checks
