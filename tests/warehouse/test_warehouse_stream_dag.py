"""``raw_recentchange`` under ``dag.test()`` when the stream has dropped rows.

A batch committed a minute ago with dropped rows and Kafka offsets is put in
``ops.stream_progress``. The pass itself (a Spark batch over Kafka) is replaced
by a stand-in that records what a pass over those offsets would record, or
fails; everything else (the coverage query, the task chain, the check and its
row in ``ops.quality_runs``) runs for real against PostgreSQL.

Named to run after ``test_warehouse_refresh.py``: these runs emit the raw asset's
event too, and that module's first run expects to be the one that sees new rows.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg
import pytest

from tributary.stream.reconcile import PassResult, record_pass
from tributary.stream.sink import BatchHealth, ensure_schema, record_health

from .test_warehouse_refresh import run, task_states

pytestmark = pytest.mark.warehouse

TOPIC = f"test.dag.{uuid.uuid4().hex[:8]}"
BATCH_ID = 900_000_000 + uuid.uuid4().int % 1_000_000


@pytest.fixture
def dropped_batch(conninfo: str) -> Iterator[int]:
    ensure_schema(conninfo, "ops.sql")
    record_health(
        conninfo,
        BatchHealth(
            query="warehouse",
            batch_id=BATCH_ID,
            input_rows=500,
            dropped_by_watermark=35,
            malformed_rows=0,
            future_rows=0,
            canary_rows=0,
            watermark=datetime.now(UTC),
            start_offsets={TOPIC: {"0": 1_000}},
            end_offsets={TOPIC: {"0": 1_500}},
        ),
    )
    yield BATCH_ID
    with psycopg.connect(conninfo, autocommit=True) as conn:
        conn.execute(
            "DELETE FROM ops.stream_progress WHERE query = 'warehouse' AND batch_id = %s",
            (BATCH_ID,),
        )
        conn.execute("DELETE FROM ops.reconcile_runs WHERE topic = %s", (TOPIC,))


class FakeSpark:
    def stop(self) -> None:
        pass


@pytest.fixture
def passes(monkeypatch: pytest.MonkeyPatch, conninfo: str) -> list[dict[str, Any]]:
    """Every call the task makes to ``run_pass``; the stand-in records a covering pass."""
    from tributary.stream import reconcile

    calls: list[dict[str, Any]] = []

    def fake_pass(_spark: object, settings: Any, run_id: str, **kwargs: Any) -> PassResult:
        calls.append({"run_id": run_id, **kwargs})
        result = PassResult(
            topic=TOPIC,
            start_offsets={TOPIC: {"0": 1_000}},
            end_offsets={TOPIC: {"0": 1_500}},
            records=500,
            landed=500,  # every dropped row was a copy of a landed event
            malformed=0,
            future=0,
            canary=0,
            missing_records=0,
            missing=(),
            recovered=0,
            left_missing=frozenset(),
            dry_run=False,
            seconds=0.1,
            started_at=datetime.now(UTC),
        )
        return replace(result, pass_id=record_pass(settings.pg_conninfo, run_id, result))

    monkeypatch.setattr(reconcile, "build_session", lambda _settings: FakeSpark())
    monkeypatch.setattr(reconcile, "run_pass", fake_pass)
    return calls


def stream_check(conninfo: str, run_id: str) -> tuple[str, dict[str, Any]]:
    with psycopg.connect(conninfo) as conn:
        row = conn.execute(
            "SELECT status, details FROM ops.quality_runs"
            " WHERE run_id = %s AND check_name = 'stream_loss_warehouse'",
            (run_id,),
        ).fetchone()
    assert row is not None
    return str(row[0]), dict(row[1])


@pytest.mark.usefixtures("dropped_batch")
def test_uncovered_drops_get_a_pass_and_the_check_then_passes(
    airflow_db: Path, conninfo: str, passes: list[dict[str, Any]]
) -> None:
    dagrun = run("raw_recentchange", "raw_recentchange (uncovered drops)")
    states = task_states(dagrun)
    assert (states["reconcile_drops"], states["check_stream"]) == ("success", "success")
    assert [(c["run_id"], c["start"]) for c in passes] == [(dagrun.run_id, "uncovered")]
    status, details = stream_check(conninfo, dagrun.run_id)
    assert status == "pass"
    assert details["covered_dropped"] >= 35

    # Covered now: the next run has nothing to reconcile and starts no Spark.
    again = run("raw_recentchange", "raw_recentchange (drops covered)")
    assert task_states(again)["reconcile_drops"] == "skipped"
    assert len(passes) == 1


@pytest.mark.usefixtures("dropped_batch")
def test_a_failed_pass_still_records_the_check_red(
    airflow_db: Path, conninfo: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tributary.stream import reconcile

    def broken(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("broker unreachable")

    monkeypatch.setattr(reconcile, "build_session", lambda _settings: FakeSpark())
    monkeypatch.setattr(reconcile, "run_pass", broken)
    dagrun = run("raw_recentchange", "raw_recentchange (pass fails)")
    states = task_states(dagrun)
    assert (states["reconcile_drops"], states["check_stream"]) == ("failed", "failed")
    status, details = stream_check(conninfo, dagrun.run_id)
    assert status == "fail"
    assert details["uncovered_dropped"] >= 35
