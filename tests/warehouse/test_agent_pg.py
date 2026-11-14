"""The agent against the built warehouse: its role, the executor's limits, the 40 questions.

Runs after ``tributary sample load`` and ``make dbt`` (``make test-warehouse``).
The session fixture creates the agent's role, runs one warehouse refresh (so
``ops`` holds rebuilds and check results) and publishes the catalog into a
temporary directory, the same steps a deployment takes.
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterator
from pathlib import Path

import psycopg
import pytest
from fastapi.testclient import TestClient

from eval.warehouse_eval import RESULTS, evaluate_question, load_questions, same_result
from tributary.agent.catalog import load_dbt
from tributary.agent.execute import ExecutionError, PostgresExecutor
from tributary.agent.providers.fake import FakeProvider
from tributary.agent.roles import ensure_agent_role
from tributary.agent.service import Agent, build_agent
from tributary.api.app import create_app
from tributary.config import REPO_ROOT, Settings, get_settings
from tributary.warehouse.refresh import refresh

pytestmark = pytest.mark.warehouse

RECORDINGS = REPO_ROOT / "tests" / "recordings" / "warehouse.jsonl"
ALL_RECORDINGS = sorted(RECORDINGS.parent.glob("warehouse*.jsonl"))


# Recordings made with planner features on, and the settings their replay needs (M13's second
# pass: value hints and three worked examples).
FEATURES: dict[str, dict[str, object]] = {
    "warehouse-vf": {"agent_values": True, "agent_examples": 3},
}


def recorded_run(recordings: Path = RECORDINGS) -> Path:
    """The live run's report for a recordings file (named by provider, model and variant)."""
    first = json.loads(recordings.read_text(encoding="utf-8").splitlines()[0])
    model = str(first["model"]).replace("/", "-").replace(":", "-")
    suffix = "-vf" if recordings.stem in FEATURES else ""
    return RESULTS / f"warehouse-{first['provider']}-{model}{suffix}.json"


@pytest.fixture(scope="module")
def agent_settings(conninfo: str, tmp_path_factory: pytest.TempPathFactory) -> Settings:
    # get_settings() after `conninfo`: that fixture widens the freshness limit by the
    # sample's age through the environment, which a Settings built earlier never saw.
    settings = get_settings()
    ensure_agent_role(conninfo, settings.agent_user, settings.agent_password.get_secret_value())
    agent = settings.model_copy(
        update={
            "catalog_dir": tmp_path_factory.mktemp("catalog"),
            "provider": "fake",
            "recordings_path": RECORDINGS,
        }
    )
    refresh(agent, "test-agent-pg")
    return agent


@pytest.fixture(scope="module")
def agent(agent_settings: Settings) -> Agent:
    return build_agent(agent_settings, provider=FakeProvider(RECORDINGS))


@pytest.fixture
def executor(agent_settings: Settings) -> PostgresExecutor:
    return PostgresExecutor(
        agent_settings.agent_conninfo, statement_timeout_ms=500, row_cap=5, cost_ceiling=1e6
    )


# -- the role and the executor's limits, with the guard deliberately bypassed ---------------


def test_reads_marts_and_caps_rows(executor: PostgresExecutor) -> None:
    result = executor.run("SELECT wiki FROM marts.dim_wiki ORDER BY wiki")
    assert result.columns == ("wiki",)
    assert len(result.rows) == 5
    assert result.truncated
    assert result.cost is not None
    assert result.cost > 0


@pytest.mark.parametrize(
    ("query", "kind"),
    [
        ("SELECT count(*) FROM raw.recentchange", "permission"),
        ("SELECT count(*) FROM staging.stg_recentchange", "permission"),
        # TEMPORARY is revoked from the role (and PUBLIC): refused before the read-only check
        ("CREATE TEMP TABLE t AS SELECT 1 AS x", "permission"),
        # lo_create is open to every role but PostgreSQL forbids it in a read-only transaction
        ("SELECT lo_create(0)", "read_only"),
        ("SELECT pg_sleep(5)", "timeout"),
        ("SELECT count(*) FROM marts.fct_edits a CROSS JOIN marts.fct_edits b", "too_expensive"),
        # search_path is empty: an unqualified name resolves to nothing.
        ("SELECT count(*) FROM fct_edits", "database"),
    ],
)
def test_database_refuses(executor: PostgresExecutor, query: str, kind: str) -> None:
    with pytest.raises(ExecutionError) as caught:
        executor.run(query)
    assert caught.value.kind == kind, caught.value.message


def test_stacked_statements_never_run(executor: PostgresExecutor, conninfo: str) -> None:
    """A prepared statement holds one statement: the COMMIT that would end the read-only
    transaction, and the DELETE after it, are refused before anything runs."""
    with psycopg.connect(conninfo) as conn:
        before = conn.execute("SELECT count(*) FROM marts.dim_wiki").fetchone()
    with pytest.raises(ExecutionError) as caught:
        executor.run("SELECT 1; COMMIT; DELETE FROM marts.dim_wiki")
    assert caught.value.kind == "database"
    assert "multiple commands" in caught.value.message
    with psycopg.connect(conninfo) as conn:
        assert conn.execute("SELECT count(*) FROM marts.dim_wiki").fetchone() == before


def test_role_default_holds_after_a_commit(conninfo: str, agent_settings: Settings) -> None:
    """Without the executor (simple protocol, stacked string), a COMMIT ends the read-only
    transaction, and the role's default_transaction_read_only must still refuse the write."""
    with (
        psycopg.connect(agent_settings.agent_conninfo) as conn,
        pytest.raises(psycopg.errors.ReadOnlySqlTransaction),
    ):
        conn.execute("BEGIN READ ONLY; COMMIT; CREATE TABLE marts.x (i int)")


def test_session_settings_die_with_the_connection(executor: PostgresExecutor) -> None:
    executor.run("SELECT set_config('statement_timeout', '0', false)")
    with pytest.raises(ExecutionError) as caught:
        executor.run("SELECT pg_sleep(5)")
    assert caught.value.kind == "timeout"


# -- the 40 warehouse questions -------------------------------------------------------------


def test_every_gold_query_passes_the_guard_and_runs(agent: Agent) -> None:
    guard = agent.guard()
    for q in load_questions():
        checked = guard.check(q.sql)
        assert checked.allowed, (q.id, checked.reasons)
        assert set(checked.tables) == set(q.tables), q.id
        result = agent.executor.run(checked.sql)
        assert result.rows, q.id
        assert same_result(result.rows, result.rows)


@pytest.mark.parametrize("recordings", ALL_RECORDINGS, ids=lambda p: p.stem)
def test_replay_reproduces_the_recorded_run(agent_settings: Settings, recordings: Path) -> None:
    """The fake provider replays every recorded call, and each verdict is the live run's."""
    settings = agent_settings.model_copy(update=FEATURES.get(recordings.stem, {}))
    agent = build_agent(settings, provider=FakeProvider(recordings))
    recorded = json.loads(recorded_run(recordings).read_text())
    expected = {v["id"]: v for v in recorded["verdicts"]}
    questions = load_questions()
    assert set(expected) == {q.id for q in questions}
    for q in questions:
        verdict = evaluate_question(agent, agent.executor, q)
        live = expected[q.id]
        assert (verdict.status, verdict.correct, verdict.attempts) == (
            live["status"],
            live["correct"],
            live["attempts"],
        ), q.id
        assert verdict.sql == live["sql"], q.id
    assert sum(v["correct"] for v in recorded["verdicts"]) == recorded["summary"]["correct"]


# -- provenance and the API on the real database ----------------------------------------------


def test_freshness_reports_the_refresh(agent: Agent) -> None:
    tables = {t.relation: t for t in agent.freshness()}
    fct = tables["marts.fct_edits"]
    assert fct.last_refreshed_at is not None
    assert not fct.stale
    assert fct.checks > 0
    assert "raw.recentchange" in fct.covers


@pytest.fixture
def client(agent: Agent) -> Iterator[TestClient]:
    with TestClient(create_app(agent, ["test-key"])) as test_client:
        test_client.headers["X-API-Key"] = "test-key"
        yield test_client


def test_api_sql_and_freshness(client: TestClient) -> None:
    response = client.post(
        "/sql", json={"sql": "SELECT count(*) AS edits FROM fct_edits WHERE wiki = 'enwiki'"}
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["sql"].startswith("SELECT COUNT(*) AS edits FROM marts.fct_edits")
    assert body["tables"] == ["marts.fct_edits"]
    assert body["provenance"][0]["relation"] == "marts.fct_edits"
    assert body["rows"][0][0] > 0

    blocked = client.post("/sql", json={"sql": "DELETE FROM marts.fct_edits"})
    assert blocked.status_code == 422
    assert blocked.json()["status"] == "blocked"

    freshness = client.get("/freshness").json()
    assert {t["relation"] for t in freshness["tables"]} >= {"marts.fct_edits", "marts.dim_wiki"}


def test_api_query_replays_a_recorded_question(client: TestClient) -> None:
    question = next(q for q in load_questions() if q.id == "total-edits")
    started = time.perf_counter()
    response = client.post("/query", json={"question": question.question})
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["status"] == "answered"
    assert body["provenance"]
    assert body["provenance"][0]["quality"] in ("pass", "warn")
    assert body["request_id"] == response.headers["X-Request-ID"]
    assert time.perf_counter() - started < 5


def test_catalog_is_published(agent_settings: Settings) -> None:
    assert (Path(agent_settings.catalog_dir) / "current" / "values.json").exists()


def test_the_committed_evaluation_catalog_is_the_published_one(agent_settings: Settings) -> None:
    # `make eval-agree` replays both warehouse passes offline over eval/warehouse_catalog.json;
    # the prompts (and so every recording's fingerprint) depend on it matching dbt's.
    from eval.agree import load_catalog

    published = load_dbt(agent_settings.catalog_dir).to_dict()
    committed = load_catalog().to_dict()
    for document in (published, committed):  # when it was published, not what it says
        document.pop("version")
        document.pop("generated_at")
    assert committed == published
