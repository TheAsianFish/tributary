"""``raw_recentchange`` and ``warehouse_refresh`` under ``dag.test()``, end to end.

Needs PostgreSQL from TRIBUTARY_PG_* with the sample loaded
(``tributary sample load``) and the dbt project in ``warehouse/``. Airflow's
own metadata lives in a temporary SQLite database (see conftest.py).
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import psycopg
import pytest

from tributary.config import Settings
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
    assert task_states(first) == {"detect_new_rows": "success"}
    # Nothing ingested since: skipped, so no asset event and no rebuild.
    assert str(second.state) == "success"
    assert task_states(second) == {"detect_new_rows": "skipped"}

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
