"""The agent commands: ask, sql and catalog, locally and against a running API."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
from typer.testing import CliRunner

from tests.unit.agent_doubles import (
    CATALOG_DIR,
    FakeExecutor,
    ScriptedProvider,
    catalog,
    draft,
    make_agent,
    result,
)
from tributary import cli
from tributary.agent.provider import ProviderError
from tributary.agent.service import Agent, CatalogSource
from tributary.cli import app
from tributary.config import get_settings

GOOD = "SELECT count(*) AS edits FROM marts.fct_edits"
GOOD_RAN = "SELECT COUNT(*) AS edits FROM marts.fct_edits LIMIT 1000"


@pytest.fixture(autouse=True)
def _fresh_settings() -> Iterator[None]:
    """``get_settings`` is cached; these tests change the environment it reads."""
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def use_agent(monkeypatch: pytest.MonkeyPatch, agent: Agent) -> list[str | None]:
    providers: list[str | None] = []

    def local(provider: str | None, *, second_pass: bool = False) -> Agent:
        providers.append(provider)
        return agent

    monkeypatch.setattr(cli, "_local_agent", local)
    return providers


def test_ask_local(monkeypatch: pytest.MonkeyPatch) -> None:
    agent = make_agent(ScriptedProvider([draft(GOOD)]), FakeExecutor({GOOD_RAN: result()}))
    providers = use_agent(monkeypatch, agent)

    out = CliRunner().invoke(app, ["ask", "How many edits?", "--provider", "fake"])

    assert out.exit_code == 0, out.output
    assert providers == ["fake"]
    lines = out.stdout.splitlines()
    assert lines[0] == "The number of edits. edits: 42."
    assert f"  {GOOD_RAN}" in lines
    assert "Provenance:" in lines


def test_ask_local_json(monkeypatch: pytest.MonkeyPatch) -> None:
    use_agent(monkeypatch, make_agent(ScriptedProvider([draft(GOOD)])))
    out = CliRunner().invoke(app, ["ask", "How many edits?", "--json"])
    assert out.exit_code == 0, out.output
    document = json.loads(out.stdout)
    assert document["status"] == "answered"
    assert document["rows"] == [[42]]
    assert len(document["request_id"]) == 12  # the run id


def test_ask_unanswerable_exits_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    reply = draft("", answerable=False, assumptions=["no revenue table"])
    use_agent(monkeypatch, make_agent(ScriptedProvider([reply])))
    out = CliRunner().invoke(app, ["ask", "Revenue?"])
    assert out.exit_code == 0, out.output
    assert "cannot answer this: no revenue table" in out.stdout


def test_ask_blocked_exits_one(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = ScriptedProvider([draft("SELECT * FROM raw.recentchange")] * 3)
    use_agent(monkeypatch, make_agent(provider))
    out = CliRunner().invoke(app, ["ask", "Everything"])
    assert out.exit_code == 1
    assert "after 3 attempts" in out.stdout


def test_ask_provider_error_exits_two(monkeypatch: pytest.MonkeyPatch) -> None:
    use_agent(monkeypatch, make_agent(ScriptedProvider([ProviderError("overloaded")])))
    out = CliRunner().invoke(app, ["ask", "q"])
    assert out.exit_code == 2
    assert "error: overloaded" in out.stderr


def test_ask_without_catalog_exits_two(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    agent = make_agent()
    agent.catalogs = CatalogSource(tmp_path, ("marts",))
    use_agent(monkeypatch, agent)
    out = CliRunner().invoke(app, ["ask", "q"])
    assert out.exit_code == 2
    assert "no published catalog" in out.stderr


class Posted:
    def __init__(self, status: int, document: dict[str, Any]) -> None:
        self.status = status
        self.document = document
        self.calls: list[tuple[str, dict[str, Any], dict[str, str]]] = []

    def __call__(
        self, url: str, *, json: dict[str, Any], headers: dict[str, str], timeout: float
    ) -> httpx.Response:
        self.calls.append((url, json, headers))
        return httpx.Response(self.status, json=self.document)


def test_ask_api(monkeypatch: pytest.MonkeyPatch) -> None:
    document = make_agent(ScriptedProvider([draft(GOOD)])).ask("q", request_id="r").to_dict()
    posted = Posted(200, document)
    monkeypatch.setattr(httpx, "post", posted)
    monkeypatch.setattr(cli, "_local_agent", lambda provider: pytest.fail("ran locally"))

    out = CliRunner().invoke(
        app, ["ask", "How many edits?", "--api", "http://api:8000/", "--api-key", "k"]
    )

    assert out.exit_code == 0, out.output
    assert posted.calls == [
        ("http://api:8000/query", {"question": "How many edits?"}, {"X-API-Key": "k"})
    ]
    assert out.stdout.splitlines()[0] == document["answer"]


def test_ask_api_key_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    posted = Posted(200, {"status": "answered", "answer": "ok"})
    monkeypatch.setattr(httpx, "post", posted)
    monkeypatch.setenv("TRIBUTARY_API_KEY", "from-env")
    out = CliRunner().invoke(app, ["ask", "q", "--api", "http://api"])
    assert out.exit_code == 0, out.output
    assert posted.calls[0][2] == {"X-API-Key": "from-env"}


def test_ask_api_error_exits_two(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(httpx, "post", Posted(401, {"detail": "missing or invalid API key"}))
    out = CliRunner().invoke(app, ["ask", "q", "--api", "http://api"])
    assert out.exit_code == 2
    assert "401: missing or invalid API key" in out.stderr


def test_ask_api_unreachable_exits_two(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(url: str, **kwargs: object) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    monkeypatch.setattr(httpx, "post", refuse)
    out = CliRunner().invoke(app, ["ask", "q", "--api", "http://api"])
    assert out.exit_code == 2
    assert "cannot reach http://api: connection refused" in out.stderr


def test_sql_api_blocked_prints_the_answer(monkeypatch: pytest.MonkeyPatch) -> None:
    blocked = make_agent().sql("DELETE FROM marts.fct_edits").to_dict()
    posted = Posted(422, blocked)
    monkeypatch.setattr(httpx, "post", posted)
    out = CliRunner().invoke(app, ["sql", "DELETE FROM marts.fct_edits", "--api", "http://api"])
    assert out.exit_code == 1
    assert posted.calls[0][:2] == ("http://api/sql", {"sql": "DELETE FROM marts.fct_edits"})
    assert out.stdout.startswith("blocked by the safety policy")


def test_sql_local(monkeypatch: pytest.MonkeyPatch) -> None:
    executor = FakeExecutor()
    providers = use_agent(monkeypatch, make_agent(executor=executor))
    out = CliRunner().invoke(app, ["sql", GOOD, "--json"])
    assert out.exit_code == 0, out.output
    assert providers == [None]
    assert executor.ran == [GOOD_RAN]
    assert json.loads(out.stdout)["sql"] == GOOD_RAN


def test_sql_local_blocked_exits_one(monkeypatch: pytest.MonkeyPatch) -> None:
    executor = FakeExecutor()
    use_agent(monkeypatch, make_agent(executor=executor))
    out = CliRunner().invoke(app, ["sql", "DELETE FROM marts.fct_edits"])
    assert out.exit_code == 1
    assert executor.ran == []


def test_catalog_renders(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRIBUTARY_CATALOG_DIR", str(CATALOG_DIR))
    out = CliRunner().invoke(app, ["catalog"])
    assert out.exit_code == 0, out.output
    assert out.stdout.rstrip("\n") == catalog().render().rstrip("\n")


def test_catalog_one_relation(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRIBUTARY_CATALOG_DIR", str(CATALOG_DIR))
    out = CliRunner().invoke(app, ["catalog", "--relation", "marts.dim_wiki"])
    assert out.exit_code == 0, out.output
    assert "CREATE TABLE marts.dim_wiki" in out.stdout
    assert "CREATE TABLE marts.fct_edits" not in out.stdout


def test_catalog_json(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRIBUTARY_CATALOG_DIR", str(CATALOG_DIR))
    out = CliRunner().invoke(app, ["catalog", "--json"])
    assert out.exit_code == 0, out.output
    assert json.loads(out.stdout) == catalog().to_dict()


def test_catalog_missing_exits_two(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("TRIBUTARY_CATALOG_DIR", str(tmp_path))
    out = CliRunner().invoke(app, ["catalog"])
    assert out.exit_code == 2
    assert "run `tributary warehouse refresh`" in out.stderr


def test_ask_unknown_provider_exits_two() -> None:
    out = CliRunner().invoke(app, ["ask", "How many edits?", "--provider", "bogus"])

    assert out.exit_code == 2
    assert "must be one of anthropic, ollama, fake" in out.output


def test_ask_api_non_json_error_exits_two(monkeypatch: pytest.MonkeyPatch) -> None:
    def posted(url: str, **kwargs: Any) -> httpx.Response:
        return httpx.Response(502, text="Bad Gateway", request=httpx.Request("POST", url))

    monkeypatch.setattr(httpx, "post", posted)
    out = CliRunner().invoke(app, ["ask", "q", "--api", "http://api.test"])

    assert out.exit_code == 2
    assert "502 from http://api.test: Bad Gateway" in out.output


def test_catalog_unknown_relation_exits_two(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRIBUTARY_CATALOG_DIR", str(CATALOG_DIR))
    out = CliRunner().invoke(app, ["catalog", "--relation", "marts.nope"])
    assert out.exit_code == 2, out.output
    assert "unknown relation 'marts.nope'" in out.stderr
    assert "Traceback" not in out.stderr


def test_ask_and_sql_report_an_unreachable_warehouse(monkeypatch: pytest.MonkeyPatch) -> None:
    from tributary.agent.execute import ExecutionError

    down = ExecutionError("connection", "cannot connect to the warehouse: connection refused")
    agent = make_agent(
        ScriptedProvider([draft(GOOD)]), executor=FakeExecutor(errors={GOOD_RAN: down})
    )
    monkeypatch.setattr(cli, "_local_agent", lambda provider, **_: agent)
    for args in (["ask", "How many edits?"], ["sql", GOOD]):
        out = CliRunner().invoke(app, args)
        assert out.exit_code == 2, out.output
        assert "error: The warehouse is unreachable: cannot connect" in out.stderr
