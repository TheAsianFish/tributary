"""The guard's list of callable names against the server it guards."""

from __future__ import annotations

import pytest

from eval.guard.differential import Oracle
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
