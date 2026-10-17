"""Data-quality and refresh results, persisted to ``ops.quality_runs`` / ``ops.refresh_runs``.

Every check that the platform runs ends up as one row per check per
orchestrator run: dbt's tests (generic, the custom freshness and volume-anomaly
tests, singular tests) and the lake's per-hour reconciliation. Persisting them
is what lets an answer report, for each table it read, whether that table's
latest checks passed, which a red task in a scheduler UI cannot do.

Parsing is pure (artifacts in, dataclasses out) so it is tested against
recorded dbt artifacts; only :func:`record_checks` and :func:`record_refreshes`
touch the database. Both upsert on the run's key, so a retried task replaces its
earlier rows instead of adding a second set.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal

import psycopg
import structlog
from psycopg.types.json import Jsonb

from tributary.stream.sink import ensure_schema
from tributary.warehouse.dbt import NodeResult, RunResults

log = structlog.get_logger(__name__)

CheckStatus = Literal["pass", "warn", "fail", "error", "skipped"]

_TEST_STATUS: dict[str, CheckStatus] = {
    "pass": "pass",
    "success": "pass",
    "warn": "warn",
    "fail": "fail",
    "error": "error",
    "runtime error": "error",
    "skipped": "skipped",
    "no-op": "skipped",
}

REFRESHED_TYPES = frozenset({"model", "seed", "snapshot"})
UNKNOWN_TARGET = "unknown"
_VALID_STATUSES = frozenset(_TEST_STATUS.values())


@dataclass(frozen=True, slots=True)
class CheckResult:
    """One check's outcome, whatever ran it (dbt, the lake reconciliation, ...).

    ``target`` is the checked relation as ``schema.table``; ``failures`` is the
    number of offending rows (or mismatching units) when the check counts them.
    """

    check_name: str
    check_type: str
    target: str
    status: CheckStatus
    failures: int | None
    details: dict[str, Any] = field(default_factory=dict)
    # Any: details is free-form JSON stored as jsonb.
    started_at: datetime | None = None
    finished_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.status not in _VALID_STATUSES:
            raise ValueError(f"invalid check status {self.status!r}")


@dataclass(frozen=True, slots=True)
class RefreshRow:
    """One model or seed build."""

    node: str
    relation: str
    status: str
    rows_affected: int | None
    execution_seconds: float
    finished_at: datetime


def map_test_status(status: str) -> CheckStatus:
    """dbt's test status → the ``ops.quality_runs`` vocabulary (unknown → ``error``)."""
    return _TEST_STATUS.get(status.lower(), "error")


def split_relation(relation_name: str) -> tuple[str, ...]:
    """Split ``"db"."schema"."table"`` into its unquoted parts (dots inside quotes kept)."""
    parts: list[str] = []
    current: list[str] = []
    quoted = False
    i = 0
    while i < len(relation_name):
        ch = relation_name[i]
        if ch == '"':
            if quoted and relation_name[i + 1 : i + 2] == '"':  # escaped quote
                current.append('"')
                i += 1
            else:
                quoted = not quoted
        elif ch == "." and not quoted:
            parts.append("".join(current))
            current = []
        else:
            current.append(ch)
        i += 1
    parts.append("".join(current))
    return tuple(parts)


def _schema_table(node: Mapping[str, Any]) -> str:
    """``schema.table`` of a manifest node or source."""
    schema = node.get("schema")
    name = node.get("alias") or node.get("identifier") or node.get("name")
    if not schema or not name:
        return UNKNOWN_TARGET
    return f"{schema}.{name}"


def _lookup(manifest: Mapping[str, Any], unique_id: str) -> Mapping[str, Any] | None:
    for section in ("nodes", "sources", "snapshots", "seeds"):
        node = manifest.get(section, {}).get(unique_id)
        if node is not None:
            return node  # type: ignore[no-any-return]  # JSON object from the manifest
    return None


def resolve_target(manifest: Mapping[str, Any], test: Mapping[str, Any]) -> str:
    """The relation a test checks, as ``schema.table``.

    ``attached_node`` is the model (or source) the test is declared on, which
    for a ``relationships`` test is the child, not the parent it also depends
    on. Tests without it (singular tests) fall back to their first dependency
    that is a relation.
    """
    candidates: list[str] = []
    attached = test.get("attached_node")
    if attached:
        candidates.append(attached)
    candidates.extend(test.get("depends_on", {}).get("nodes", []))
    for unique_id in candidates:
        node = _lookup(manifest, unique_id)
        if node is not None and node.get("resource_type", "") != "test":
            return _schema_table(node)
    return UNKNOWN_TARGET


def _timing_json(result: NodeResult) -> list[dict[str, str | None]]:
    return [
        {
            "name": t.name,
            "started_at": None if t.started_at is None else t.started_at.isoformat(),
            "completed_at": None if t.completed_at is None else t.completed_at.isoformat(),
        }
        for t in result.timing
    ]


def check_from_test(
    result: NodeResult, manifest: Mapping[str, Any], run_results: RunResults
) -> CheckResult:
    """One dbt test result as a :class:`CheckResult`."""
    node = _lookup(manifest, result.unique_id) or {}
    metadata = node.get("test_metadata") or {}
    # ``model`` is the Jinja expression that selects the tested relation: noise here.
    kwargs = {k: v for k, v in (metadata.get("kwargs") or {}).items() if k != "model"}
    details: dict[str, Any] = {
        "message": result.message,
        "kwargs": kwargs,
        "column_name": node.get("column_name") or kwargs.get("column_name"),
        "severity": (node.get("config") or {}).get("severity"),
        "namespace": metadata.get("namespace"),
        "dbt_status": result.status,
        "execution_time": round(result.execution_time, 4),
        "timing": _timing_json(result),
        "invocation_id": run_results.invocation_id,
    }
    return CheckResult(
        check_name=result.unique_id,
        # Generic tests are named after their macro; singular tests have no metadata.
        check_type=str(metadata.get("name") or "singular"),
        target=resolve_target(manifest, node),
        status=map_test_status(result.status),
        failures=result.failures,
        details=details,
        started_at=result.started_at,
        finished_at=result.completed_at or run_results.generated_at,
    )


def quality_checks(run_results: RunResults, manifest: Mapping[str, Any]) -> list[CheckResult]:
    """Every test in a dbt run as a :class:`CheckResult`; other node types are ignored."""
    return [
        check_from_test(r, manifest, run_results)
        for r in run_results.results
        if r.resource_type == "test"
    ]


def refresh_rows(
    run_results: RunResults, manifest: Mapping[str, Any] | None = None
) -> list[RefreshRow]:
    """Every model, seed and snapshot build in a dbt run.

    The relation comes from the result's ``relation_name`` when dbt reports it
    and from the manifest otherwise (dbt 1.12 leaves it empty for seeds).
    """
    rows = []
    for r in run_results.results:
        if r.resource_type not in REFRESHED_TYPES:
            continue
        parts = split_relation(r.relation_name) if r.relation_name else ()
        if len(parts) >= 2:
            relation = ".".join(parts[-2:])
        else:
            node = _lookup(manifest or {}, r.unique_id)
            relation = UNKNOWN_TARGET if node is None else _schema_table(node)
        rows.append(
            RefreshRow(
                node=r.unique_id,
                relation=relation,
                status=r.status,
                # PostgreSQL reports -1 for statements without a row count (CREATE VIEW).
                rows_affected=None
                if r.rows_affected is None or r.rows_affected < 0
                else r.rows_affected,
                execution_seconds=r.execution_time,
                finished_at=r.completed_at or run_results.generated_at,
            )
        )
    return rows


def failed_checks(checks: Iterable[CheckResult]) -> list[CheckResult]:
    """Checks that should turn the run red (``warn`` and ``skipped`` do not)."""
    return [c for c in checks if c.status in ("fail", "error")]


UPSERT_CHECK = """
INSERT INTO ops.quality_runs
    (run_id, source, check_name, check_type, target, status, failures, details,
     started_at, finished_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, coalesce(%s, clock_timestamp()))
ON CONFLICT (run_id, check_name) DO UPDATE SET
    source = EXCLUDED.source,
    check_type = EXCLUDED.check_type,
    target = EXCLUDED.target,
    status = EXCLUDED.status,
    failures = EXCLUDED.failures,
    details = EXCLUDED.details,
    started_at = EXCLUDED.started_at,
    finished_at = EXCLUDED.finished_at,
    recorded_at = clock_timestamp()
"""

UPSERT_REFRESH = """
INSERT INTO ops.refresh_runs
    (run_id, node, relation, status, rows_affected, execution_seconds, finished_at)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (run_id, node) DO UPDATE SET
    relation = EXCLUDED.relation,
    status = EXCLUDED.status,
    rows_affected = EXCLUDED.rows_affected,
    execution_seconds = EXCLUDED.execution_seconds,
    finished_at = EXCLUDED.finished_at,
    recorded_at = clock_timestamp()
"""


def record_checks(conninfo: str, run_id: str, checks: Sequence[CheckResult], *, source: str) -> int:
    """Upsert checks for ``run_id`` in one transaction; returns how many were written."""
    if not checks:
        return 0
    ensure_schema(conninfo, "ops.sql")
    with psycopg.connect(conninfo) as conn, conn.cursor() as cur:
        cur.executemany(
            UPSERT_CHECK,
            [
                (
                    run_id,
                    source,
                    c.check_name,
                    c.check_type,
                    c.target,
                    c.status,
                    c.failures,
                    Jsonb(c.details),
                    c.started_at,
                    c.finished_at,
                )
                for c in checks
            ],
        )
    by_status: dict[str, int] = {}
    for c in checks:
        by_status[c.status] = by_status.get(c.status, 0) + 1
    log.info("quality.recorded", source=source, checks=len(checks), **by_status)
    return len(checks)


def record_check(conninfo: str, run_id: str, check: CheckResult, *, source: str) -> None:
    """Upsert a single check (e.g. one hour of the lake reconciliation)."""
    record_checks(conninfo, run_id, [check], source=source)


def record_refreshes(conninfo: str, run_id: str, rows: Sequence[RefreshRow]) -> int:
    """Upsert model and seed builds for ``run_id``; returns how many were written."""
    if not rows:
        return 0
    ensure_schema(conninfo, "ops.sql")
    with psycopg.connect(conninfo) as conn, conn.cursor() as cur:
        cur.executemany(
            UPSERT_REFRESH,
            [
                (
                    run_id,
                    r.node,
                    r.relation,
                    r.status,
                    r.rows_affected,
                    r.execution_seconds,
                    r.finished_at,
                )
                for r in rows
            ],
        )
    log.info("refresh.recorded", nodes=len(rows))
    return len(rows)
