"""dbt against a real PostgreSQL (connection from the TRIBUTARY_PG_* variables).

Two checks:

- the custom quality tests discriminate: the fixture project in
  ``tests/dbt/quality_fixtures`` is built into a throwaway schema and every
  fixture test must end with exactly the expected status;
- the warehouse builds: ``dbt build`` on ``warehouse/`` over the data in
  ``raw.recentchange`` (the committed sample is loaded first if the table is
  empty) finishes with every model, seed and test successful.
"""

from __future__ import annotations

import json
import math
import os
import subprocess
import sys
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import psycopg
import pytest
from psycopg import sql

from tributary.config import REPO_ROOT, Settings
from tributary.ingest.sample import read_sample
from tributary.stream.sink import ensure_schema
from tributary.warehouse.load import load_events

pytestmark = pytest.mark.warehouse

WAREHOUSE = REPO_ROOT / "warehouse"
FIXTURES = REPO_ROOT / "tests/dbt/quality_fixtures"
DBT = Path(sys.executable).with_name("dbt")

# The suffix of each fixture test's name states the outcome it must have.
EXPECTED = {
    "volume_flat_passes": "pass",
    "volume_spike_fails": "fail",
    "volume_outage_fails": "fail",
    "volume_short_history_passes": "pass",
    "volume_short_history_low_minimum_fails": "fail",
    "volume_constant_history_passes": "pass",
    "volume_future_event_passes": "pass",
    "freshness_fresh_passes": "pass",
    "freshness_stale_fails": "fail",
    "freshness_stale_within_limit_passes": "pass",
    "freshness_empty_fails": "fail",
}


@pytest.fixture(scope="module")
def settings() -> Settings:
    if not DBT.exists():
        pytest.skip(f"dbt is not installed next to {sys.executable} (uv sync --all-extras)")
    settings = Settings()
    try:
        psycopg.connect(settings.pg_conninfo, connect_timeout=3).close()
    except psycopg.OperationalError as exc:
        pytest.skip(f"PostgreSQL from TRIBUTARY_PG_* not reachable: {exc}")
    return settings


def dbt_env(settings: Settings, **extra: str) -> dict[str, str]:
    """The environment for dbt: the resolved settings, so .env and defaults apply too."""
    env = dict(os.environ)
    env.update(
        TRIBUTARY_PG_HOST=settings.pg_host,
        TRIBUTARY_PG_PORT=str(settings.pg_port),
        TRIBUTARY_PG_DATABASE=settings.pg_database,
        TRIBUTARY_PG_USER=settings.pg_user,
        TRIBUTARY_PG_PASSWORD=settings.pg_password.get_secret_value(),
        **extra,
    )
    return env


def dbt(
    project: Path, out: Path, env: dict[str, str], *args: str
) -> subprocess.CompletedProcess[str]:
    """Run one dbt command; target and logs go to ``out`` so a developer's are untouched."""
    command = [str(DBT), *args, "--project-dir", str(project), "--profiles-dir", str(project)]
    paths = {"DBT_TARGET_PATH": str(out / "target"), "DBT_LOG_PATH": str(out / "logs")}
    return subprocess.run(command, env=env | paths, capture_output=True, text=True, timeout=600)


def statuses(out: Path) -> dict[str, str]:
    """Node name -> status from run_results.json (test ids end in a hash; it is dropped)."""
    results = json.loads((out / "target/run_results.json").read_text())["results"]
    named: dict[str, str] = {}
    for result in results:
        parts = str(result["unique_id"]).split(".")
        named[parts[2]] = str(result["status"])
    return named


@pytest.fixture
def fixture_schema(settings: Settings) -> Iterator[str]:
    schema = f"dbt_quality_{uuid.uuid4().hex[:8]}"
    yield schema
    with psycopg.connect(settings.pg_conninfo, autocommit=True) as conn:
        conn.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))


def test_custom_quality_tests_discriminate(
    settings: Settings, fixture_schema: str, tmp_path: Path
) -> None:
    env = dbt_env(settings, TRIBUTARY_DBT_SCHEMA=fixture_schema)
    deps = dbt(FIXTURES, tmp_path, env, "deps")
    assert deps.returncode == 0, deps.stdout + deps.stderr

    # Exit code 1 is expected: half of the fixture tests exist to fail.
    build = dbt(FIXTURES, tmp_path, env, "build")
    assert build.returncode in (0, 1), build.stdout + build.stderr

    got = statuses(tmp_path)
    models = {name: status for name, status in got.items() if name not in EXPECTED}
    assert models
    assert all(status == "success" for status in models.values()), models
    assert {name: got.get(name) for name in EXPECTED} == EXPECTED


def ensure_sample(settings: Settings) -> datetime:
    """Load the committed sample if raw.recentchange is empty; returns the newest arrival."""
    query = "SELECT max(ingested_at) FROM raw.recentchange"
    with psycopg.connect(settings.pg_conninfo, autocommit=True) as conn:
        try:
            row = conn.execute(query).fetchone()
        except psycopg.errors.UndefinedTable:
            row = None
    newest = row[0] if row is not None else None
    if isinstance(newest, datetime):
        return newest
    load_events(settings.pg_conninfo, read_sample(settings.sample_path))
    with psycopg.connect(settings.pg_conninfo) as conn:
        row = conn.execute(query).fetchone()
    assert row is not None
    assert isinstance(row[0], datetime)
    return row[0]


def test_warehouse_builds_on_the_sample(settings: Settings, tmp_path: Path) -> None:
    newest = ensure_sample(settings)
    # A loaded sample is static, so its arrival age is the time since the load, not
    # a pipeline symptom. The limit is widened to cover that age; that the freshness
    # test fires on stale data is the fixture test's job.
    age_minutes = (datetime.now(UTC) - newest).total_seconds() / 60
    limit = max(60, math.ceil(age_minutes) + 60)
    env = dbt_env(settings)

    build = dbt(
        WAREHOUSE,
        tmp_path,
        env,
        "build",
        "--vars",
        json.dumps({"freshness_max_lag_minutes": limit}),
    )
    got = statuses(tmp_path)
    failed = {name: status for name, status in got.items() if status not in ("success", "pass")}
    assert not failed, (failed, build.stdout[-4000:])
    assert build.returncode == 0, build.stdout[-4000:]
    marts = {"fct_edits", "dim_wiki", "dim_page", "agg_edits_hourly", "agg_editor_activity"}
    assert marts <= set(got)


@pytest.fixture
def empty_database(settings: Settings) -> Iterator[Settings]:
    """A throwaway database on the same server, with an empty raw.recentchange."""
    name = f"tributary_empty_{uuid.uuid4().hex[:8]}"
    with psycopg.connect(settings.pg_conninfo, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    scratch = settings.model_copy(update={"pg_database": name})
    ensure_schema(scratch.pg_conninfo)
    yield scratch
    with psycopg.connect(settings.pg_conninfo, autocommit=True) as conn:
        conn.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))


def test_fct_edits_loads_after_a_first_build_on_an_empty_table(
    empty_database: Settings, tmp_path: Path
) -> None:
    """A warehouse refreshed before any data arrived must still pick the data up later."""
    env = dbt_env(empty_database)
    models = ("run", "--select", "stg_recentchange", "fct_edits")
    first = dbt(WAREHOUSE, tmp_path / "first", env, "seed")
    assert first.returncode == 0, first.stdout[-4000:]
    first = dbt(WAREHOUSE, tmp_path / "first", env, *models)
    assert first.returncode == 0, first.stdout[-4000:]

    load_events(empty_database.pg_conninfo, read_sample(empty_database.sample_path, limit=500))
    second = dbt(WAREHOUSE, tmp_path / "second", env, *models)
    assert second.returncode == 0, second.stdout[-4000:]
    with psycopg.connect(empty_database.pg_conninfo) as conn:
        edits = conn.execute(
            "SELECT count(*) FROM raw.recentchange WHERE event_type IN ('edit', 'new')"
        ).fetchone()
        loaded = conn.execute("SELECT count(*) FROM marts.fct_edits").fetchone()
    assert edits is not None
    assert loaded is not None
    assert edits[0] > 0
    assert loaded[0] == edits[0]
