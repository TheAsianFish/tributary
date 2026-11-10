"""The agent's catalog: keys, lineage and values from a published dbt catalog, and the DDL."""

from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest
from psycopg import sql

from tributary.agent.catalog import (
    Catalog,
    Column,
    ForeignKey,
    from_information_schema,
    load_dbt,
    quote_ident,
    quote_literal,
)
from tributary.warehouse.catalog import QueryFn

FIXTURE = Path(__file__).resolve().parent.parent / "fixtures" / "catalog"
MARTS = (
    "marts.agg_edits_hourly",
    "marts.agg_editor_activity",
    "marts.dim_page",
    "marts.dim_wiki",
    "marts.fct_edits",
)


@pytest.fixture(scope="module")
def catalog() -> Catalog:
    return load_dbt(FIXTURE)


def test_the_five_marts_sorted(catalog: Catalog) -> None:
    assert catalog.relations() == tuple(sorted(MARTS))
    assert catalog.source == "dbt"
    assert catalog.version == (FIXTURE / "current").readlink().name
    assert catalog.generated_at is not None


def test_other_schemas_on_request() -> None:
    staging = load_dbt(FIXTURE, ["staging"])
    assert "staging.stg_recentchange" in staging.relations()
    assert "staging.namespaces" in staging.relations()  # a seed
    assert staging.table("staging.namespaces").primary_key == (
        "namespace_id",
        "project_family",
        "wiki",
    )


@pytest.mark.parametrize(
    ("relation", "key"),
    [
        ("marts.fct_edits", ("event_id",)),
        ("marts.agg_edits_hourly", ("wiki", "event_hour")),
        ("marts.agg_editor_activity", ("wiki", "user_name")),
        ("marts.dim_wiki", ("wiki",)),
        ("marts.dim_page", ("page_key",)),
    ],
)
def test_primary_keys_from_tests(catalog: Catalog, relation: str, key: tuple[str, ...]) -> None:
    assert catalog.table(relation).primary_key == key


def test_foreign_keys_from_relationships_tests(catalog: Catalog) -> None:
    assert catalog.table("marts.fct_edits").foreign_keys == (
        ForeignKey("page_key", "marts.dim_page", "page_key"),
        ForeignKey("wiki", "marts.dim_wiki", "wiki"),
    )
    assert catalog.table("marts.dim_wiki").foreign_keys == ()


def test_upstream_is_transitive_through_models_seeds_and_sources(catalog: Catalog) -> None:
    assert catalog.table("marts.fct_edits").upstream == (
        "raw.recentchange",
        "staging.namespaces",
        "staging.stg_recentchange",
    )
    hourly = catalog.table("marts.agg_edits_hourly").upstream
    assert "marts.fct_edits" in hourly
    assert "raw.recentchange" in hourly
    assert "marts.agg_edits_hourly" not in hourly


def test_columns_types_order_and_descriptions(catalog: Catalog) -> None:
    fct = catalog.table("marts.fct_edits")
    assert [c.name for c in fct.columns[:5]] == [
        "event_id",
        "event_ts",
        "event_hour",
        "wiki",
        "page_key",
    ]
    assert fct.column("event_id").data_type == "uuid"
    assert fct.column("event_ts").data_type == "timestamp with time zone"
    assert fct.column("event_id").description.startswith("Event UUID")
    assert fct.column("EVENT_ID") is fct.column("event_id")
    with pytest.raises(KeyError, match="no column 'nope'"):
        fct.column("nope")


def test_values_from_the_index(catalog: Catalog) -> None:
    wiki = catalog.table("marts.fct_edits").column("wiki")
    assert wiki.distinct == 153
    assert wiki.values is not None
    assert len(wiki.values) == 153
    assert "enwiki" in wiki.values[:3]
    # page_key is counted but has too many values to index.
    page_key = catalog.table("marts.fct_edits").column("page_key")
    assert page_key.values is None
    assert page_key.distinct is not None
    # Opted out with meta.index_values: false: neither counted nor indexed.
    for relation, name in [
        ("marts.fct_edits", "user_name"),
        ("marts.fct_edits", "title"),
        ("marts.agg_editor_activity", "user_name"),
        ("marts.dim_page", "title"),
    ]:
        column = catalog.table(relation).column(name)
        assert column.values is None
        assert column.distinct is None


def test_render_ddl(catalog: Catalog) -> None:
    text = catalog.render()
    assert text == catalog.render() == load_dbt(FIXTURE).render()
    assert catalog.version is not None
    assert catalog.version not in text
    for fragment in [
        "CREATE TABLE marts.fct_edits (\n  event_id uuid PRIMARY KEY,  -- Event UUID",
        "  wiki text REFERENCES marts.dim_wiki(wiki),  -- Wiki database name",
        "  page_key text REFERENCES marts.dim_page(page_key),  -- Stable page identifier",
        "  PRIMARY KEY (wiki, event_hour)\n);",
        "  PRIMARY KEY (wiki, user_name)\n);",
        "-- Built from: raw.recentchange, staging.namespaces, staging.stg_recentchange.",
        "Values (153 distinct, most frequent first), e.g. 'wikidatawiki', 'enwiki',",
        "'Category talk'",
        "Values: 'wikipedia', 'wiktionary',",
    ]:
        assert fragment in text, fragment
    assert text.index("marts.agg_edits_hourly") < text.index("CREATE TABLE marts.fct_edits")
    assert "  user_name text,  -- Who made the edit" in text
    user_line = next(line for line in text.splitlines() if line.startswith("  user_name text"))
    assert "Values" not in user_line


def test_render_value_thresholds(catalog: Catalog) -> None:
    text = catalog.render(["marts.dim_wiki"], max_values=5, sample_values=2)
    family = next(line for line in text.splitlines() if line.startswith("  project_family"))
    assert family.endswith(
        "Values (14 distinct, most frequent first), e.g. 'wikipedia', 'wiktionary'."
    )
    everything = catalog.render(["marts.dim_wiki"], max_values=1000)
    assert "'zuwiki'" in everything


def test_render_subset(catalog: Catalog) -> None:
    text = catalog.render(["marts.fct_edits", "marts.dim_wiki"])
    assert text.count("CREATE TABLE") == 2
    assert text.index("marts.dim_wiki (") < text.index("marts.fct_edits (")
    assert "agg_edits_hourly (" not in text
    with pytest.raises(KeyError, match=r"did you mean marts\.fct_edits"):
        catalog.render(["staging.fct_edits"])


def test_unknown_relation(catalog: Catalog) -> None:
    with pytest.raises(KeyError, match=r"unknown relation 'marts\.nope'"):
        catalog.table("marts.nope")


def test_to_dict_is_json(catalog: Catalog) -> None:
    document = json.loads(json.dumps(catalog.to_dict()))
    assert document["source"] == "dbt"
    assert document["version"] == catalog.version
    fct = next(t for t in document["tables"] if t["relation"] == "marts.fct_edits")
    assert fct["primary_key"] == ["event_id"]
    assert {"column": "wiki", "ref_relation": "marts.dim_wiki", "ref_column": "wiki"} in fct[
        "foreign_keys"
    ]
    wiki = next(c for c in fct["columns"] if c["name"] == "wiki")
    assert wiki["type"] == "text"
    assert wiki["distinct"] == 153
    assert len(wiki["values"]) == 153


def test_quoting() -> None:
    assert quote_ident("wiki") == "wiki"
    assert quote_ident("order") == '"order"'
    assert quote_ident("Free Meal") == '"Free Meal"'
    assert quote_ident('a"b') == '"a""b"'
    assert quote_literal("Category talk") == "'Category talk'"
    assert quote_literal("O'Brien, J.") == "'O''Brien, J.'"
    assert quote_literal("a\nb\\") == "E'a\\x0ab\\\\'"


def test_render_quotes_identifiers() -> None:
    catalog = Catalog(
        tables=(from_information_schema(_fake([_COLUMN_ROWS[0]], []), ["s"]).tables[0],),
        source="information_schema",
    )
    assert 'CREATE TABLE "School Data".frpm (' in catalog.render()


# --- information_schema -------------------------------------------------------------------

_COLUMN_ROWS: list[tuple[Any, ...]] = [
    ("School Data", "frpm", "CDSCode", "text", None, None),
    ("app", "orders", "id", "integer", "Customer orders.", "Order id."),
    ("app", "orders", "customer_id", "integer", "Customer orders.", None),
    ("app", "orders", "tags", "_text", "Customer orders.", None),
    ("app", "order_lines", "order_id", "integer", None, None),
    ("app", "order_lines", "line_no", "integer", None, None),
    ("app", "customers", "id", "integer", None, "Customer id."),
]
_KEY_ROWS: list[tuple[Any, ...]] = [
    ("app", "order_lines", "f", "order_id", "app", "orders", "id"),
    ("app", "order_lines", "p", "order_id", None, None, None),
    ("app", "order_lines", "p", "line_no", None, None, None),
    ("app", "orders", "f", "customer_id", "app", "customers", "id"),
    ("app", "orders", "p", "id", None, None, None),
]


def _fake(
    columns: list[tuple[Any, ...]],
    keys: list[tuple[Any, ...]],
    calls: list[tuple[str, Sequence[object]]] | None = None,
) -> QueryFn:
    def query(statement: sql.Composed, params: Sequence[object]) -> list[tuple[Any, ...]]:
        text = statement.as_string(None)
        if calls is not None:
            calls.append((text, params))
        return columns if "information_schema.columns" in text else keys

    return query


def test_from_information_schema() -> None:
    calls: list[tuple[str, Sequence[object]]] = []
    catalog = from_information_schema(
        _fake(_COLUMN_ROWS[1:], _KEY_ROWS, calls),
        ["app", "other"],
        descriptions={"app.customers": {"": "People who order.", "ID": "The customer."}},
    )
    assert len(calls) == 2
    for text, params in calls:
        assert "= ANY(%s)" in text
        assert "app" not in text
        assert params == (["app", "other"],)
    assert catalog.source == "information_schema"
    assert catalog.version is None
    assert catalog.relations() == ("app.customers", "app.order_lines", "app.orders")
    orders = catalog.table("app.orders")
    assert orders.description == "Customer orders."
    assert orders.columns == (
        Column("id", "integer", "Order id."),
        Column("customer_id", "integer", ""),
        Column("tags", "_text", ""),
    )
    assert orders.primary_key == ("id",)
    assert orders.foreign_keys == (ForeignKey("customer_id", "app.customers", "id"),)
    assert orders.upstream == ()
    lines = catalog.table("app.order_lines")
    assert lines.primary_key == ("order_id", "line_no")
    customers = catalog.table("app.customers")
    assert customers.description == "People who order."
    assert customers.column("id").description == "The customer."
    text = catalog.render()
    assert "  PRIMARY KEY (order_id, line_no)\n);" in text
    assert "  customer_id integer REFERENCES app.customers(id)," in text
    assert "-- People who order.\nCREATE TABLE app.customers (" in text
