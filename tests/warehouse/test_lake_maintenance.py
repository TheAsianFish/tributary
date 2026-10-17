"""``lake_maintenance`` under ``dag.test()`` against the lakehouse profile (Trino, MinIO).

Skipped when Trino is not reachable at TRIBUTARY_TRINO_HOST:TRINO_PORT.
"""

from __future__ import annotations

import socket
import time
from pathlib import Path
from typing import Any

import psycopg
import pytest

from tributary.config import Settings

from .conftest import DAGS_FOLDER, TIMINGS

pytestmark = pytest.mark.lakehouse


@pytest.fixture(scope="module")
def trino(settings: Settings) -> None:
    try:
        socket.create_connection((settings.trino_host, settings.trino_port), timeout=3).close()
    except OSError as exc:
        pytest.skip(
            f"Trino not reachable at {settings.trino_host}:{settings.trino_port} ({exc});"
            " start the lakehouse profile"
        )


def test_lake_maintenance_is_green_and_reconciles(
    trino: None, airflow_db: Path, conninfo: str
) -> None:
    from airflow.dag_processing.dagbag import DagBag

    bag = DagBag(str(DAGS_FOLDER))
    assert bag.import_errors == {}
    start = time.perf_counter()
    dagrun: Any = bag.dags["lake_maintenance"].test()  # Any: dag.test() is untyped
    TIMINGS["dag.test() lake_maintenance"] = time.perf_counter() - start

    assert str(dagrun.state) == "success"
    assert {ti.task_id: str(ti.state) for ti in dagrun.get_task_instances()} == {
        "reconcile_closed_hours": "success",
        "optimize_closed_hours": "success",
        "expire_snapshots": "success",
        "remove_orphan_files": "success",
    }
    with psycopg.connect(conninfo) as conn:
        rows = conn.execute(
            "SELECT check_type, status FROM ops.quality_runs WHERE run_id = %s",
            (dagrun.run_id,),
        ).fetchall()
    assert rows, "no reconciliation rows recorded for this run"
    assert {status for _, status in rows} == {"pass"}
