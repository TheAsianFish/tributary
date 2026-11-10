"""The HTTP API: keys, request ids in headers, bodies and logs, and status codes."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import structlog
from fastapi.testclient import TestClient
from structlog.testing import LogCapture

from tests.unit.agent_doubles import (
    FakeExecutor,
    ScriptedProvider,
    catalog,
    draft,
    make_agent,
    result,
)
from tributary import __version__
from tributary.agent.execute import ExecutionError
from tributary.agent.provider import ProviderError, ProviderRefusal
from tributary.agent.service import Agent, CatalogSource
from tributary.api.app import create_app, parse_keys, request_id_for

KEY = "s3cret-key"
GOOD = "SELECT count(*) AS edits FROM marts.fct_edits"
GOOD_RAN = "SELECT COUNT(*) AS edits FROM marts.fct_edits LIMIT 1000"


def client(agent: Agent | None = None, keys: tuple[str, ...] = (KEY,)) -> TestClient:
    return TestClient(create_app(agent or make_agent(), keys))


def auth(key: str = KEY, **extra: str) -> dict[str, str]:
    return {"X-API-Key": key, **extra}


@pytest.fixture
def logs() -> LogCapture:
    """Captured log entries with the context merged in (conftest resets structlog)."""
    capture = LogCapture()
    structlog.configure(processors=[structlog.contextvars.merge_contextvars, capture])
    return capture


def test_health_is_open() -> None:
    response = client().get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "version": __version__}


@pytest.mark.parametrize("headers", [{}, {"X-API-Key": "wrong"}, {"X-API-Key": ""}])
@pytest.mark.parametrize(
    ("method", "path", "body"),
    [
        ("POST", "/query", {"question": "q"}),
        ("POST", "/sql", {"sql": GOOD}),
        ("GET", "/catalog", None),
        ("GET", "/freshness", None),
    ],
)
def test_unauthorised(
    headers: dict[str, str], method: str, path: str, body: dict[str, str] | None
) -> None:
    executor = FakeExecutor()
    response = client(make_agent(executor=executor)).request(
        method, path, json=body, headers=headers
    )
    assert response.status_code == 401
    assert response.headers["WWW-Authenticate"] == "ApiKey"
    assert "X-Request-ID" in response.headers
    assert executor.ran == []


@pytest.mark.parametrize("key", ["first", "second"])
def test_any_configured_key_is_accepted(key: str) -> None:
    response = client(keys=("first", "second")).get("/catalog", headers=auth(key))
    assert response.status_code == 200


def test_no_keys_refuses_to_start() -> None:
    with pytest.raises(ValueError, match="no API key configured"):
        create_app(make_agent(), [])
    with pytest.raises(ValueError, match="no API key configured"):
        create_app(make_agent(), [""])


def test_parse_keys() -> None:
    assert parse_keys(" a, b ,,c ") == ["a", "b", "c"]
    assert parse_keys("") == []


@pytest.mark.parametrize(
    ("header", "kept"),
    [
        ("abc-123_x.y", True),
        ("a" * 64, True),
        ("a" * 65, False),
        ("has space", False),
        ("new\nline", False),
        ("semi;colon", False),
        ("", False),
        (None, False),
    ],
)
def test_request_id_for(header: str | None, kept: bool) -> None:
    rid = request_id_for(header)
    if kept:
        assert rid == header
    else:
        assert rid != header
        assert len(rid) == 32


def test_query_returns_the_answer_document(logs: LogCapture) -> None:
    provider = ScriptedProvider([draft(GOOD)])
    executor = FakeExecutor({GOOD_RAN: result(rows=[(42,)])})
    response = client(make_agent(provider, executor)).post(
        "/query", json={"question": "How many edits?"}, headers=auth(**{"X-Request-ID": "req-1"})
    )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "answered"
    assert body["sql"] == GOOD_RAN
    assert body["rows"] == [[42]]
    assert body["answer"] == "The number of edits. edits: 42."
    assert body["request_id"] == "req-1"
    assert response.headers["X-Request-ID"] == "req-1"
    assert provider.requests[0].messages[0].content == "Question: How many edits?"

    http = [e for e in logs.entries if e["event"] == "http.request"]
    assert http == [
        {
            "event": "http.request",
            "log_level": "info",
            "request_id": "req-1",
            "method": "POST",
            "path": "/query",
            "status": 200,
            "latency_ms": http[0]["latency_ms"],
        }
    ]
    inside = [e for e in logs.entries if e["event"] == "planner.done"]
    assert len(inside) == 1
    assert inside[0]["request_id"] == "req-1"


def test_generated_request_id(logs: LogCapture) -> None:
    response = client().get("/catalog", headers=auth())
    rid = response.headers["X-Request-ID"]
    assert len(rid) == 32
    assert [e["request_id"] for e in logs.entries if e["event"] == "http.request"] == [rid]


@pytest.mark.parametrize("unsafe", ["has space", "x" * 65, "bad;id"])
def test_unsafe_request_id_is_replaced(unsafe: str, logs: LogCapture) -> None:
    provider = ScriptedProvider([draft(GOOD)])
    response = client(make_agent(provider)).post(
        "/query", json={"question": "q"}, headers=auth(**{"X-Request-ID": unsafe})
    )
    rid = response.headers["X-Request-ID"]
    assert rid != unsafe
    assert len(rid) == 32
    assert response.json()["request_id"] == rid
    assert all(e.get("request_id") == rid for e in logs.entries)
    assert {"http.request", "planner.done"} <= {e["event"] for e in logs.entries}


def test_request_ids_do_not_leak_between_requests(logs: LogCapture) -> None:
    c = client()
    c.get("/catalog", headers=auth(**{"X-Request-ID": "one"}))
    c.get("/catalog", headers=auth(**{"X-Request-ID": "two"}))
    ids = [e["request_id"] for e in logs.entries if e["event"] == "http.request"]
    assert ids == ["one", "two"]


def test_sql_allowed() -> None:
    executor = FakeExecutor()
    response = client(make_agent(executor=executor)).post(
        "/sql", json={"sql": GOOD}, headers=auth()
    )
    assert response.status_code == 200
    assert response.json()["sql"] == GOOD_RAN
    assert executor.ran == [GOOD_RAN]


def test_sql_blocked_is_422() -> None:
    executor = FakeExecutor()
    response = client(make_agent(executor=executor)).post(
        "/sql", json={"sql": "DELETE FROM marts.fct_edits"}, headers=auth()
    )
    assert response.status_code == 422
    body = response.json()
    assert body["status"] == "blocked"
    assert body["request_id"] == response.headers["X-Request-ID"]
    assert executor.ran == []


@pytest.mark.parametrize(
    ("path", "body"), [("/query", {"question": ""}), ("/sql", {"sql": ""}), ("/query", {})]
)
def test_invalid_bodies_are_422(path: str, body: dict[str, Any]) -> None:
    assert client().post(path, json=body, headers=auth()).status_code == 422


def test_catalog() -> None:
    response = client().get("/catalog", headers=auth())
    assert response.status_code == 200
    assert response.json() == catalog().to_dict()


def test_freshness() -> None:
    response = client().get("/freshness", headers=auth(**{"X-Request-ID": "f1"}))
    assert response.status_code == 200
    body = response.json()
    assert body["request_id"] == "f1"
    assert body["catalog_version"] == catalog().version
    assert body["stale_after_minutes"] == 120
    assert [t["relation"] for t in body["tables"]] == list(catalog().relations())


@pytest.mark.parametrize(
    ("error", "status", "detail"),
    [
        (ProviderRefusal("declined"), 422, "the model declined the question: declined"),
        (ProviderError("overloaded"), 502, "model provider failed: overloaded"),
    ],
)
def test_provider_errors(error: Exception, status: int, detail: str) -> None:
    response = client(make_agent(ScriptedProvider([error]))).post(
        "/query", json={"question": "q"}, headers=auth(**{"X-Request-ID": "p1"})
    )
    assert response.status_code == status
    assert response.json() == {"detail": detail, "request_id": "p1"}
    assert response.headers["X-Request-ID"] == "p1"


def test_no_catalog_is_503(tmp_path: Path) -> None:
    agent = make_agent()
    agent.catalogs = CatalogSource(tmp_path, ("marts",))
    response = client(agent).get("/catalog", headers=auth())
    assert response.status_code == 503
    assert response.json()["detail"].startswith("no published catalog under")


def test_agent_factory_is_called_once() -> None:
    built: list[Agent] = []

    def factory() -> Agent:
        built.append(make_agent())
        return built[-1]

    c = TestClient(create_app(factory, [KEY]))
    assert c.get("/health").status_code == 200
    assert built == []
    c.get("/catalog", headers=auth())
    c.get("/freshness", headers=auth())
    assert len(built) == 1


@pytest.mark.parametrize(
    ("kind", "status", "start"),
    [
        ("connection", 503, "warehouse unavailable: "),
        ("permission", 500, "the agent's role is misconfigured (permission): "),
    ],
)
def test_deployment_failures_are_the_servers_fault(kind: str, status: int, start: str) -> None:
    error = ExecutionError(kind, "boom")  # type: ignore[arg-type]
    provider = ScriptedProvider([draft(GOOD)] * 3)
    agent = make_agent(provider, executor=FakeExecutor(errors={GOOD_RAN: error}))
    c = client(agent)

    for path, body in (("/query", {"question": "q"}), ("/sql", {"sql": GOOD})):
        response = c.post(path, json=body, headers=auth(**{"X-Request-ID": "d1"}))
        assert response.status_code == status, response.text
        assert response.json()["detail"].startswith(start)
        assert response.json()["request_id"] == "d1"
        assert response.headers["X-Request-ID"] == "d1"
    assert len(provider.requests) == 1, "the outage was not repaired with more model calls"


def test_concurrent_first_requests_build_one_agent() -> None:
    import threading
    import time

    built: list[Agent] = []
    lock = threading.Lock()

    def factory() -> Agent:
        time.sleep(0.05)  # long enough for every thread to arrive before the first returns
        with lock:
            built.append(make_agent())
        return built[-1]

    c = TestClient(create_app(factory, [KEY]))
    statuses: list[int] = []

    def hit() -> None:
        statuses.append(c.get("/catalog", headers=auth()).status_code)

    threads = [threading.Thread(target=hit) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert statuses == [200] * 8
    assert len(built) == 1
