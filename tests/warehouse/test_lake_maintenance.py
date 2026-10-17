"""``lake_maintenance`` under ``dag.test()`` against the lakehouse profile (Trino, MinIO).

Skipped when Trino is not reachable at TRIBUTARY_TRINO_HOST:TRINO_PORT. The
reconciliation's notion of the warehouse's newest event needs PostgreSQL
alone, so that test is in the ``warehouse`` tier.
"""

from __future__ import annotations

import socket
import time
from datetime import timedelta
from pathlib import Path
from typing import Any

import psycopg
import pytest

from tributary.config import Settings
from tributary.lake.reconcile import committed_newest
from tributary.stream.sink import ensure_schema

from .conftest import DAGS_FOLDER, TIMINGS


@pytest.fixture(scope="module")
def trino(settings: Settings) -> None:
    try:
        socket.create_connection((settings.trino_host, settings.trino_port), timeout=3).close()
    except OSError as exc:
        pytest.skip(
            f"Trino not reachable at {settings.trino_host}:{settings.trino_port} ({exc});"
            " start the lakehouse profile"
        )


@pytest.mark.lakehouse
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
            "SELECT check_name, status FROM ops.quality_runs WHERE run_id = %s",
            (dagrun.run_id,),
        ).fetchall()
    assert rows, "no reconciliation rows recorded for this run"
    assert {status for _, status in rows} == {"pass"}
    # lake_lag alone passes with no closed hour compared; at least one hour must be.
    assert any(name.startswith("reconcile_hour:") for name, _ in rows), rows


@pytest.mark.warehouse
def test_committed_progress_wins_over_the_tables_newest_event(conninfo: str) -> None:
    """A progress mark behind ``max(event_ts)`` is the newest event; without one, the table's is.

    Runs in one transaction that is always rolled back: the stream's own
    progress rows are hidden from this connection only, and the sentinel
    batch never becomes visible to anything else.
    """
    ensure_schema(conninfo, "ops.sql")
    with psycopg.connect(conninfo) as conn, conn.transaction(force_rollback=True):
        row = conn.execute("SELECT max(event_ts) FROM raw.recentchange").fetchone()
        assert row is not None
        assert row[0] is not None
        table_newest = row[0]
        conn.execute("DELETE FROM ops.stream_progress WHERE query = 'warehouse'")
        assert committed_newest(conn) == table_newest
        conn.execute(
            "INSERT INTO ops.stream_progress (query, batch_id, max_event_ts, rows, inserted)"
            " VALUES ('warehouse', -424242, %s, 0, 0)",
            (table_newest - timedelta(hours=1),),
        )
        assert committed_newest(conn) == table_newest - timedelta(hours=1)
