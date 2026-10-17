"""The agent's catalog read from PostgreSQL's own catalog, on the built warehouse."""

from __future__ import annotations

import pytest

from tributary.agent.catalog import from_information_schema
from tributary.warehouse.catalog import postgres_query

pytestmark = pytest.mark.warehouse

MARTS = {
    "marts.agg_edits_hourly": ("wiki", "event_hour", "edits"),
    "marts.agg_editor_activity": ("wiki", "user_name", "is_bot"),
    "marts.dim_page": ("page_key", "wiki", "namespace"),
    "marts.dim_wiki": ("wiki", "domain", "project_family"),
    "marts.fct_edits": ("event_id", "event_ts", "event_hour", "wiki", "page_key"),
}


def test_marts_from_information_schema(conninfo: str) -> None:
    query, close = postgres_query(conninfo)
    try:
        catalog = from_information_schema(query, ["marts"])
    finally:
        close()
    assert set(MARTS) <= set(catalog.relations())
    for relation, leading in MARTS.items():
        table = catalog.table(relation)
        assert tuple(c.name for c in table.columns[: len(leading)]) == leading  # database order
        assert table.upstream == ()
        assert all(c.values is None for c in table.columns)
        # dbt creates no constraints; whatever keys there are must name real columns.
        names = {c.name for c in table.columns}
        assert set(table.primary_key) <= names
        assert {f.column for f in table.foreign_keys} <= names
    fct = catalog.table("marts.fct_edits")
    assert fct.column("event_id").data_type == "uuid"
    assert fct.column("event_ts").data_type == "timestamp with time zone"
    assert fct.column("is_bot").data_type == "boolean"
    assert "CREATE TABLE marts.fct_edits (" in catalog.render(["marts.fct_edits"])
