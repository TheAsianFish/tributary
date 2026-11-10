"""The warehouse refresh, step by step: build, test, publish.

These are the functions the ``warehouse_refresh`` DAG's tasks call and that
``tributary warehouse refresh`` calls in sequence, so the scheduler and the
command line run the same code. Each takes the settings and the orchestrator's
run id (the Airflow ``run_id``, or the CLI's), which becomes the ``run_id`` of
every row in ``ops.quality_runs`` and ``ops.refresh_runs``.

:func:`check_freshness` is the exception to "only on new rows": the
``raw_recentchange`` poll runs it when nothing arrived, because a stalled sink
is exactly when freshness has to fail, and no refresh would otherwise run it.

Failing tests are recorded first and raised second: the task turns red, so the
run is visibly not green, while ``ops.quality_runs`` holds which checks failed
and why. Recording inside the test step (rather than in a downstream task with
an ``all_done`` trigger rule) keeps the results out of XCom and makes a retry
of the step re-record exactly what it re-ran.
"""

from __future__ import annotations

import tempfile
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import psycopg
import structlog

from tributary.config import Settings
from tributary.warehouse.catalog import (
    MART_SCHEMAS,
    MAX_DISTINCT,
    postgres_query,
    publish_from_target,
    read_json,
)
from tributary.warehouse.dbt import DbtCrashError, DbtResult, run_dbt
from tributary.warehouse.quality import (
    CheckResult,
    RefreshRow,
    failed_checks,
    measure_freshness,
    quality_checks,
    record_checks,
    record_refreshes,
    refresh_rows,
)

log = structlog.get_logger(__name__)

DBT_SOURCE = "dbt"


class WarehouseError(RuntimeError):
    """A refresh step did not succeed (its results, if any, are already recorded)."""


class QualityCheckError(WarehouseError):
    """One or more checks failed or errored; they are in ``ops.quality_runs``."""

    def __init__(self, failed: Sequence[CheckResult]) -> None:
        self.failed = list(failed)
        names = ", ".join(f"{c.check_name} ({c.status})" for c in self.failed[:5])
        more = f" and {len(self.failed) - 5} more" if len(self.failed) > 5 else ""
        super().__init__(f"{len(self.failed)} check(s) did not pass: {names}{more}")


@dataclass(frozen=True, slots=True)
class RefreshSummary:
    """What ``tributary warehouse refresh`` did."""

    run_id: str
    refreshed: list[RefreshRow]
    checks: list[CheckResult]
    catalog: Path | None


def _record_build(settings: Settings, run_id: str, result: DbtResult) -> list[RefreshRow]:
    run_results = result.require_results()
    manifest_path = result.target_path / "manifest.json"
    manifest = read_json(manifest_path) if manifest_path.exists() else None
    rows = refresh_rows(run_results, manifest)
    record_refreshes(settings.pg_conninfo, run_id, rows)
    if not result.ok:
        failed = [f"{r.node} ({r.status})" for r in rows if r.status != "success"]
        raise WarehouseError(f"dbt {' '.join(result.args)} failed: {', '.join(failed) or '?'}")
    return rows


def build_models(settings: Settings, run_id: str) -> list[RefreshRow]:
    """``dbt seed`` then ``dbt run``; every build lands in ``ops.refresh_runs``.

    Raises :class:`WarehouseError` after recording when a seed or model fails,
    and :class:`~tributary.warehouse.dbt.DbtCrashError` when dbt itself fails.
    """
    with tempfile.TemporaryDirectory(prefix="tributary-dbt-") as work:
        rows = _record_build(settings, run_id, run_dbt(settings, ["seed"], Path(work)))
        rows += _record_build(settings, run_id, run_dbt(settings, ["run"], Path(work)))
    log.info("warehouse.built", run_id=run_id, nodes=len(rows))
    return rows


def _run_tests(settings: Settings, run_id: str, args: Sequence[str]) -> list[CheckResult]:
    with tempfile.TemporaryDirectory(prefix="tributary-dbt-") as work:
        result = run_dbt(settings, args, Path(work))
        run_results = result.require_results()
        manifest = read_json(result.target_path / "manifest.json")
    checks = measure_freshness(settings.pg_conninfo, quality_checks(run_results, manifest))
    record_checks(settings.pg_conninfo, run_id, checks, source=DBT_SOURCE)
    failed = failed_checks(checks)
    if failed:
        raise QualityCheckError(failed)
    if result.returncode != 0:
        # Exit code 1 with every test passing: some other node failed; do not hide it.
        raise WarehouseError(
            f"dbt {' '.join(args)} exited {result.returncode} with all tests passing"
        )
    return checks


def test_models(settings: Settings, run_id: str) -> list[CheckResult]:
    """``dbt test``; records every result, then raises if any check failed or errored.

    ``warn`` does not fail the step: a warning is a signal to look, not a reason
    to hold back the catalog.
    """
    return _run_tests(settings, run_id, ["test"])


FRESHNESS_SELECTOR = "test_name:freshness"


def check_freshness(settings: Settings, run_id: str) -> list[CheckResult]:
    """Only the ``freshness`` tests, recorded and raised like :func:`test_models`.

    For the poll that found no new rows: a refresh runs only when rows arrive,
    so without this a stalled sink would leave the last ``pass`` standing in
    ``ops.latest_quality`` forever. No build is needed; freshness reads the
    landing table and the incremental mart as they are.
    """
    return _run_tests(settings, run_id, ["test", "--select", FRESHNESS_SELECTOR])


def publish_catalog(
    settings: Settings,
    run_id: str,
    *,
    schemas: Sequence[str] = MART_SCHEMAS,
    max_distinct: int = MAX_DISTINCT,
) -> Path:
    """``dbt docs generate``, the value index, and an atomic publish to ``catalog_dir``."""
    with tempfile.TemporaryDirectory(prefix="tributary-dbt-") as work:
        result = run_dbt(settings, ["docs", "generate"], Path(work))
        if not result.ok:
            raise DbtCrashError(f"dbt docs generate exited {result.returncode}")
        query, close = postgres_query(settings.pg_conninfo)
        try:
            path = publish_from_target(
                settings.catalog_dir,
                result.target_path,
                run_id=run_id,
                query=query,
                schemas=schemas,
                max_distinct=max_distinct,
            )
        finally:
            close()
    return path


def refresh(settings: Settings, run_id: str) -> RefreshSummary:
    """Build, test (recording results), and publish the catalog if every check passed."""
    built = build_models(settings, run_id)
    checks = test_models(settings, run_id)
    catalog = publish_catalog(settings, run_id)
    return RefreshSummary(run_id, built, checks, catalog)


NEW_ROWS = "SELECT max(ingested_at) FROM raw.recentchange"


def new_rows_since(conninfo: str, after: datetime | None) -> datetime | None:
    """The newest ``ingested_at`` if any row was ingested after ``after``, else None.

    ``after`` is the high-water mark returned by the previous call that found
    rows (None the first time). A missing landing table counts as no rows.
    """
    query = NEW_ROWS if after is None else NEW_ROWS + " WHERE ingested_at > %s"
    params: tuple[object, ...] = () if after is None else (after,)
    try:
        with psycopg.connect(conninfo) as conn:
            row = conn.execute(query, params).fetchone()
    except psycopg.errors.UndefinedTable:
        return None
    newest = None if row is None else row[0]
    if newest is not None and not isinstance(newest, datetime):
        raise TypeError(f"unexpected max(ingested_at): {newest!r}")
    return newest


def next_high_water_mark(conninfo: str, previous: str | None) -> str | None:
    """:func:`new_rows_since` with the mark as an ISO-8601 string (as stored in JSON).

    Returns the new mark when rows arrived after ``previous``, else None. Rows
    of a sink transaction that commits after a later-stamped one can fall
    behind the mark; they are not lost (the next rebuild reads them, and
    ``fct_edits`` re-reads a look-back window), only their trigger is folded
    into the next arrival's.
    """
    after = None if previous is None else datetime.fromisoformat(previous)
    newest = new_rows_since(conninfo, after)
    log.info("raw.checked", previous=previous, newest=None if newest is None else str(newest))
    return None if newest is None else newest.isoformat()
