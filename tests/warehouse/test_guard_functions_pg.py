"""The guard's list of callable names against the server it guards."""

from __future__ import annotations

import time

import pytest

from eval.guard.differential import VIEW, Oracle
from tributary.agent.pg_functions import ONE_ARGUMENT_FUNCTIONS

pytestmark = pytest.mark.warehouse


def test_every_one_argument_function_on_the_server_is_in_the_guards_list(conninfo: str) -> None:
    # A name missing from the list is one the guard would let through as alias.name, where
    # PostgreSQL calls it on the whole row; regenerate with
    # `python -m eval.guard.differential functions`.
    oracle = Oracle(conninfo, ())
    try:
        names = {p.name for p in oracle.procs() if p.schema == "pg_catalog"}
    finally:
        oracle.close()
    assert names, "pg_proc returned no pg_catalog function"
    assert sorted(names - ONE_ARGUMENT_FUNCTIONS) == []


def _view_left(oracle: Oracle) -> object:
    row = oracle._conn.execute(f"SELECT to_regclass('pg_temp.{VIEW}')").fetchone()
    oracle._conn.rollback()
    return row[0] if row else None


def test_the_oracle_resolves_without_running_and_leaves_nothing_behind(conninfo: str) -> None:
    oracle = Oracle(conninfo, ())
    try:
        started = time.perf_counter()
        resolved = oracle.resolve("SELECT pg_catalog.pg_sleep(30)")
        assert time.perf_counter() - started < 5  # parse analysis only: nothing slept
        assert resolved.resolved
        assert "pg_catalog.pg_sleep" in {f.name for f in resolved.findings}
        assert _view_left(oracle) is None
        refused = oracle.resolve("SELECT no_such_column FROM pg_catalog.pg_class")
        assert not refused.resolved
        assert refused.sqlstate == "42703"
        assert _view_left(oracle) is None
        # the connection is usable after both: the next view name is free again
        assert oracle.resolve("SELECT 1").resolved
    finally:
        oracle.close()
