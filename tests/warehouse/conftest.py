"""Run the DAGs for real with ``dag.test()``: Airflow metadata in SQLite, data in PostgreSQL.

Airflow reads its configuration once, when it is first imported, so the
environment is set here at import time, before any test module imports it:
a throwaway AIRFLOW_HOME with a SQLite metadata database, no examples, the
repository's DAG folder, and a temporary catalog directory. This directory is
run on its own (``make test-warehouse``; ``-m lakehouse`` for the lake DAG),
never in the unit suite.
"""

from __future__ import annotations

import json
import math
import os
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path

import psycopg
import pytest

from tributary.config import REPO_ROOT, Settings, get_settings

DAGS_FOLDER = REPO_ROOT / "orchestration" / "dags"
_HOME = Path(tempfile.mkdtemp(prefix="tributary-airflow-"))
os.environ.update(
    AIRFLOW_HOME=str(_HOME),
    AIRFLOW__CORE__LOAD_EXAMPLES="False",
    AIRFLOW__CORE__DAGS_FOLDER=str(DAGS_FOLDER),
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=f"sqlite:///{_HOME / 'airflow.db'}",
    TRIBUTARY_CATALOG_DIR=str(_HOME / "catalog"),
)

TIMINGS: dict[str, float] = {}


def pytest_terminal_summary(terminalreporter: pytest.TerminalReporter) -> None:
    if TIMINGS:
        terminalreporter.section("dag timings")
        for name, seconds in TIMINGS.items():
            terminalreporter.write_line(f"{name}: {seconds:.1f} s")


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    shutil.rmtree(_HOME, ignore_errors=True)


@pytest.fixture(scope="session")
def settings() -> Settings:
    return Settings()


@pytest.fixture(scope="session")
def conninfo(settings: Settings) -> str:
    try:
        with psycopg.connect(settings.pg_conninfo, connect_timeout=3) as conn:
            row = conn.execute("SELECT count(*), max(ingested_at) FROM raw.recentchange").fetchone()
    except psycopg.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    except psycopg.errors.UndefinedTable:
        pytest.skip("raw.recentchange does not exist; run `tributary sample load` first")
    if not row or not row[0]:
        pytest.skip("raw.recentchange is empty; run `tributary sample load` first")
    _tolerate_sample_age(row[1])
    return settings.pg_conninfo


def _tolerate_sample_age(newest: object) -> None:
    """Widen the freshness limit by the age of a loaded sample, through TRIBUTARY_DBT_VARS.

    A sample loaded once stops arriving, so an hour later the freshness check
    fails on every run, correctly for a pipeline and uselessly for a test of
    the DAG. In CI the sample is loaded minutes before and the limit stays at
    its default; that the check does fire on stale data is the dbt fixture
    test's job.
    """
    if not isinstance(newest, datetime):
        return
    age_minutes = (datetime.now(UTC) - newest).total_seconds() / 60
    limit = max(60, math.ceil(age_minutes) + 60)
    os.environ["TRIBUTARY_DBT_VARS"] = json.dumps({"freshness_max_lag_minutes": limit})
    get_settings.cache_clear()


@pytest.fixture(scope="session")
def airflow_db() -> Path:
    """Migrate the SQLite metadata database once per session (timed)."""
    start = time.perf_counter()
    subprocess.run(
        [sys.executable, "-m", "airflow", "db", "migrate"],
        check=True,
        capture_output=True,
        timeout=600,
    )
    TIMINGS["airflow db migrate (sqlite)"] = time.perf_counter() - start
    return _HOME
