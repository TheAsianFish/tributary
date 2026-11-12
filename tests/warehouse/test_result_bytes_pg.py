"""The executor's result byte cap against a real PostgreSQL: refused on the server, else invisible.

Needs the marts built (``make dbt``) and the agent's role; no warehouse refresh.
"""

from __future__ import annotations

import psycopg
import pytest

from tributary.agent.execute import ExecutionError, PostgresExecutor
from tributary.agent.roles import ensure_agent_role
from tributary.config import Settings

pytestmark = pytest.mark.warehouse

CAP = 8_388_608

# Shapes the wrapper must leave alone: row order, duplicate and unnamed columns,
# unknown-typed literals, set operations, CTEs, VALUES, a trailing comment, no columns.
QUERIES = [
    "SELECT event_id, event_ts, wiki, namespace FROM marts.fct_edits"
    " ORDER BY event_ts DESC, event_id LIMIT 50",
    "SELECT wiki, edits FROM marts.dim_wiki ORDER BY edits DESC, wiki",
    "SELECT project_family, count(*) AS wikis, avg(edits) AS mean_edits"
    " FROM marts.dim_wiki GROUP BY project_family ORDER BY wikis DESC, project_family",
    "SELECT * FROM marts.agg_edits_hourly ORDER BY 1, 2",
    "SELECT * FROM marts.fct_edits ORDER BY event_id",  # cut at the row cap
    "SELECT 1 AS a, 2 AS a",
    "SELECT 'x'",
    "SELECT 1 AS x UNION SELECT 2 ORDER BY 1 DESC LIMIT 1",
    "WITH t AS (SELECT 1 AS x UNION ALL SELECT 2) SELECT * FROM t ORDER BY x DESC",
    "VALUES (1, 'a'), (2, NULL)",
    "SELECT ARRAY[1, 2], 1.5::numeric, 'NaN'::float8, '\\x00ff'::bytea, ROW(1, 'a'), true",
    "SELECT 1 -- a trailing comment",
    "SELECT FROM marts.dim_wiki LIMIT 3",
]


@pytest.fixture(scope="module")
def agent_conninfo(conninfo: str, settings: Settings) -> str:
    ensure_agent_role(conninfo, settings.agent_user, settings.agent_password.get_secret_value())
    with psycopg.connect(conninfo) as conn:
        if conn.execute("SELECT to_regclass('marts.fct_edits')").fetchone() == (None,):
            pytest.skip("the marts are not built; run `make dbt` first")
    return settings.agent_conninfo


def executor(conninfo: str, cap: int | None) -> PostgresExecutor:
    return PostgresExecutor(conninfo, row_cap=1000, max_result_bytes=cap)


def test_a_huge_row_is_refused_as_too_large(agent_conninfo: str) -> None:
    with pytest.raises(ExecutionError) as caught:
        executor(agent_conninfo, CAP).run("SELECT repeat('x', 200000000)")
    error = caught.value
    assert error.kind == "too_large"
    assert error.sqlstate == "22P02"
    assert error.repairable
    assert "200,000,000 bytes, over the cap of 8,388,608" in error.message
    assert executor(agent_conninfo, CAP).run("SELECT 1").rows == ((1,),)


def test_many_small_rows_over_the_cap_are_refused(agent_conninfo: str) -> None:
    with pytest.raises(ExecutionError) as caught:
        executor(agent_conninfo, 1000).run("SELECT wiki FROM marts.dim_wiki ORDER BY wiki")
    assert caught.value.kind == "too_large"
    assert "over the cap of 1,000" in caught.value.message


@pytest.mark.parametrize("query", QUERIES)
def test_results_under_the_cap_are_unchanged(agent_conninfo: str, query: str) -> None:
    bare = executor(agent_conninfo, None).run(query)
    capped = executor(agent_conninfo, CAP).run(query)
    assert capped.columns == bare.columns
    assert capped.truncated == bare.truncated
    # repr compares NaN equal to NaN and tells 1 from 1.0 and Decimal("1") apart
    assert repr(capped.rows) == repr(bare.rows)
    assert [type(v) for r in capped.rows for v in r] == [type(v) for r in bare.rows for v in r]
