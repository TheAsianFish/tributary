"""``raw_recentchange`` and ``warehouse_refresh`` under ``dag.test()``, end to end.

Needs PostgreSQL from TRIBUTARY_PG_* with the sample loaded
(``tributary sample load``) and the dbt project in ``warehouse/``. Airflow's
own metadata lives in a temporary SQLite database (see conftest.py).
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import psycopg
import pytest

from tributary.config import Settings, get_settings
from tributary.warehouse import assets
from tributary.warehouse.catalog import load_current

from .conftest import DAGS_FOLDER, TIMINGS

pytestmark = pytest.mark.warehouse

MARTS = {name for name in assets.MARTS}


def get_dag(dag_id: str) -> Any:
    # Any: Airflow's DAG type; dag.test() is untyped.
    from airflow.dag_processing.dagbag import DagBag

    bag = DagBag(str(DAGS_FOLDER))
    assert bag.import_errors == {}
    return bag.dags[dag_id]


def run(dag_id: str, label: str) -> Any:
    start = time.perf_counter()
    dagrun = get_dag(dag_id).test()
    TIMINGS[f"dag.test() {label}"] = time.perf_counter() - start
    return dagrun


def task_states(dagrun: Any) -> dict[str, str]:
    return {ti.task_id: str(ti.state) for ti in dagrun.get_task_instances()}


@pytest.fixture(scope="module")
def raw_runs(airflow_db: Path, conninfo: str) -> tuple[Any, Any]:
    first = run("raw_recentchange", "raw_recentchange (new rows)")
    second = run("raw_recentchange", "raw_recentchange (nothing new)")
    return first, second


def test_raw_asset_emits_once_then_skips(raw_runs: tuple[Any, Any], conninfo: str) -> None:
    first, second = raw_runs
    assert str(first.state) == "success"
    # check_stream runs beside it every time; with no failing stream it passes.
    # With no dropped row on record there is nothing to reconcile.
    assert task_states(first) == {
        "detect_new_rows": "success",
        "reconcile_drops": "skipped",
        "check_stream": "success",
    }
    # Nothing ingested since: skipped, so no asset event and no rebuild.
    assert str(second.state) == "success"
    assert task_states(second) == {
        "detect_new_rows": "skipped",
        "reconcile_drops": "skipped",
        "check_stream": "success",
    }

    from airflow.models.asset import AssetEvent, AssetModel
    from airflow.utils.session import create_session

    with create_session() as session:
        events = (
            session.query(AssetEvent)
            .join(AssetModel, AssetEvent.asset_id == AssetModel.id)
            .filter(AssetModel.name == assets.RAW_RECENTCHANGE)
            .all()
        )
        extras = [e.extra for e in events]
    assert len(extras) == 1
    with psycopg.connect(conninfo) as conn:
        row = conn.execute("SELECT max(ingested_at) FROM raw.recentchange").fetchone()
    assert row is not None
    assert extras[0][assets.HIGH_WATER_KEY] == row[0].isoformat()


@pytest.fixture
def stale_limit(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """A freshness limit of 0 minutes: every arrival is too old, whenever it was loaded."""
    monkeypatch.setenv("TRIBUTARY_DBT_VARS", json.dumps({"freshness_max_lag_minutes": 0}))
    get_settings.cache_clear()
    yield
    monkeypatch.undo()  # before clearing, so the next get_settings() sees the widened limit
    get_settings.cache_clear()


def test_nothing_new_still_checks_freshness_and_fails_when_stale(
    raw_runs: tuple[Any, Any], stale_limit: None, conninfo: str
) -> None:
    # The sink has stalled: no new rows, so no refresh, but freshness must still fail.
    dagrun = run("raw_recentchange", "raw_recentchange (nothing new, stale)")
    assert str(dagrun.state) == "failed"
    assert task_states(dagrun) == {
        "detect_new_rows": "failed",
        "reconcile_drops": "skipped",
        "check_stream": "success",
    }
    with psycopg.connect(conninfo) as conn:
        rows = conn.execute(
            "SELECT check_type, status, source FROM ops.quality_runs"
            " WHERE run_id = %s AND check_type <> 'stream_loss'",
            (dagrun.run_id,),
        ).fetchall()
    assert rows, "the freshness results were not recorded"
    assert {r[0] for r in rows} == {"freshness"}
    assert {r[1] for r in rows} == {"fail"}
    assert {r[2] for r in rows} == {"dbt"}


def test_nothing_new_records_passing_freshness(raw_runs: tuple[Any, Any], conninfo: str) -> None:
    _, second = raw_runs
    with psycopg.connect(conninfo) as conn:
        rows = conn.execute(
            "SELECT check_type, status FROM ops.quality_runs"
            " WHERE run_id = %s AND check_type <> 'stream_loss'",
            (second.run_id,),
        ).fetchall()
    assert {r[0] for r in rows} == {"freshness"}
    assert {r[1] for r in rows} == {"pass"}


@pytest.fixture(scope="module")
def refresh_run(raw_runs: tuple[Any, Any]) -> Any:
    return run("warehouse_refresh", "warehouse_refresh")


def test_refresh_run_is_green(refresh_run: Any) -> None:
    assert str(refresh_run.state) == "success"
    assert task_states(refresh_run) == {
        "build_models": "success",
        "test_models": "success",
        "publish_catalog": "success",
    }


def test_one_quality_row_per_dbt_test(refresh_run: Any, conninfo: str, settings: Settings) -> None:
    published = load_current(settings.catalog_dir)
    tests = {k for k, v in published.manifest["nodes"].items() if v["resource_type"] == "test"}
    with psycopg.connect(conninfo) as conn:
        rows = conn.execute(
            "SELECT check_name, check_type, target, status, source FROM ops.quality_runs"
            " WHERE run_id = %s",
            (refresh_run.run_id,),
        ).fetchall()
    assert {r[0] for r in rows} == tests
    assert len(rows) == len(tests)
    assert {r[3] for r in rows} == {"pass"}  # the sample passes every check
    assert {r[4] for r in rows} == {"dbt"}
    types = {r[1] for r in rows}
    assert {"not_null", "unique", "relationships", "freshness", "volume_anomaly"} <= types
    assert "unknown" not in {r[2] for r in rows}


def test_freshness_checks_record_the_measured_lag(refresh_run: Any, conninfo: str) -> None:
    with psycopg.connect(conninfo) as conn:
        rows = conn.execute(
            "SELECT target, (details->>'lag_seconds')::float, details->>'newest',"
            " extract(epoch FROM finished_at - (details->>'newest')::timestamptz),"
            " extract(epoch FROM recorded_at - (details->>'newest')::timestamptz)"
            " FROM ops.quality_runs WHERE run_id = %s AND check_type = 'freshness'",
            (refresh_run.run_id,),
        ).fetchall()
        newest = conn.execute("SELECT max(ingested_at) FROM marts.fct_edits").fetchone()
    by_target = {r[0]: r for r in rows}
    assert set(by_target) == {"raw.recentchange", "marts.fct_edits"}
    assert newest is not None
    _target, lag, stamp, after_test, at_record = by_target["marts.fct_edits"]
    assert stamp == newest[0].isoformat()
    # The lag is read between the dbt test finishing and the row being recorded,
    # so it must lie between those two ages of the newest row (1 s of slack).
    assert lag is not None
    assert float(after_test) - 1 <= lag <= float(at_record) + 1, (lag, after_test, at_record)
    assert lag > 0


def test_every_mart_refreshed(refresh_run: Any, conninfo: str) -> None:
    with psycopg.connect(conninfo) as conn:
        rows = conn.execute(
            "SELECT relation, status FROM ops.refresh_runs WHERE run_id = %s",
            (refresh_run.run_id,),
        ).fetchall()
    refreshed = {relation: status for relation, status in rows}
    assert set(refreshed) >= MARTS
    assert set(refreshed.values()) == {"success"}


def test_catalog_published(refresh_run: Any, settings: Settings) -> None:
    current = settings.catalog_dir / "current"
    assert current.is_symlink()
    for name in ("manifest.json", "catalog.json", "values.json", "export.json"):
        assert (current / name).is_file(), name
    published = load_current(settings.catalog_dir)
    assert published.export["run_id"] == refresh_run.run_id
    family = published.values["columns"]["marts.dim_wiki.project_family"]
    assert family["indexed"] is True
    assert "wikipedia" in {v["value"] for v in family["values"]}
