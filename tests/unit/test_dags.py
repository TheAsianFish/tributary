"""The DAGs parse with zero import errors and have the intended shape.

Airflow parses them in a child process (``_dag_dump.py``) with a throwaway
AIRFLOW_HOME: importing Airflow reconfigures logging and structlog process-wide,
which would leak into every later test. DagBag needs no metadata database.
"""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from tributary.config import REPO_ROOT
from tributary.warehouse import assets

DAGS = REPO_ROOT / "orchestration" / "dags"
DUMP = Path(__file__).resolve().parent / "_dag_dump.py"

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("airflow") is None, reason="the airflow extra is not installed"
)


@pytest.fixture(scope="module")
def bag(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    home = tmp_path_factory.mktemp("airflow-home")
    out = home / "dags.json"
    env = {
        **os.environ,
        "AIRFLOW_HOME": str(home),
        "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
        "AIRFLOW__CORE__DAGS_FOLDER": str(DAGS),
    }
    proc = subprocess.run(
        [sys.executable, str(DUMP), str(DAGS), str(out)],
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    document: dict[str, Any] = json.loads(out.read_text())
    return document


def test_no_import_errors(bag: dict[str, Any]) -> None:
    assert bag["import_errors"] == {}
    assert sorted(bag["dags"]) == ["lake_maintenance", "raw_recentchange", "warehouse_refresh"]


def test_every_dag_runs_alone_without_catchup(bag: dict[str, Any]) -> None:
    for dag_id, dag in bag["dags"].items():
        assert dag["max_active_runs"] == 1, dag_id
        assert dag["catchup"] is False, dag_id
        assert dag["dagrun_timeout"], dag_id
        assert dag["doc_md"], dag_id
        assert dag["tags"], dag_id


def test_raw_recentchange_polls_and_reads_its_own_events(bag: dict[str, Any]) -> None:
    dag = bag["dags"]["raw_recentchange"]
    assert dag["cron"] == "*/5 * * * *"
    # Shorter than the interval, so a hung check never overlaps the next one.
    assert dag["dagrun_timeout"] < 5 * 60
    task = dag["tasks"]["detect_new_rows"]
    assert task["inlets"] == task["outlets"] == [assets.RAW_RECENTCHANGE]
    # The stream_loss checks run every five minutes beside it, whether rows arrived or not,
    # after a reconciliation pass has had the chance to cover the drops (passed, skipped
    # or failed: all_done).
    check = dag["tasks"]["check_stream"]
    assert check["upstream"] == ["reconcile_drops"]
    assert check["trigger_rule"] == "all_done"
    assert check["outlets"] == []
    assert dag["tasks"]["reconcile_drops"]["upstream"] == []
    # A long pass is cut off with time left for check_stream inside the run's timeout.
    timeout = dag["tasks"]["reconcile_drops"]["execution_timeout"]
    assert timeout is not None
    assert timeout <= dag["dagrun_timeout"] - 60


def test_warehouse_refresh_is_scheduled_on_the_raw_asset(bag: dict[str, Any]) -> None:
    dag = bag["dags"]["warehouse_refresh"]
    assert dag["timetable"] == "AssetTriggeredTimetable"
    assert dag["asset_schedule"] == [assets.RAW_RECENTCHANGE]
    assert dag["cron"] is None


def test_warehouse_refresh_chain_and_outlets(bag: dict[str, Any]) -> None:
    tasks = bag["dags"]["warehouse_refresh"]["tasks"]
    assert {k: v["upstream"] for k, v in tasks.items()} == {
        "build_models": [],
        "test_models": ["build_models"],
        "publish_catalog": ["test_models"],
    }
    # Quality is recorded inside test_models, so nothing downstream needs all_done;
    # the catalog is published only after every check passed.
    assert {v["trigger_rule"] for v in tasks.values()} == {"all_success"}
    assert tasks["test_models"]["outlets"] == list(assets.MARTS)
    assert tasks["publish_catalog"]["outlets"] == [assets.CATALOG]
    assert all(v["retries"] >= 1 for v in tasks.values())


def test_lake_maintenance_hourly_with_the_maintenance_chain(bag: dict[str, Any]) -> None:
    dag = bag["dags"]["lake_maintenance"]
    assert dag["cron"] == "7 * * * *"
    assert dag["dagrun_timeout"] < 60 * 60
    assert {k: v["upstream"] for k, v in dag["tasks"].items()} == {
        "reconcile_closed_hours": [],
        "optimize_closed_hours": [],
        "expire_snapshots": ["optimize_closed_hours"],
        "remove_orphan_files": ["expire_snapshots"],
    }
