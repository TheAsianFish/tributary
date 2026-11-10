"""The ``core`` linking strategy's contract.

``core`` is not implemented yet, so every test here is a strict expected
failure on :class:`LinkerUnavailable`: once ``link_core`` exists, a test that
passes turns into an XPASS failure (remove the marker then), and a test that
fails for any other reason fails outright.
"""

from __future__ import annotations

import pytest

from eval.bird import data
from eval.bird.link_eval import gold_tables
from eval.bird.load import load_catalogs
from tributary.agent.catalog import Catalog, Column, ForeignKey, Table
from tributary.agent.linker import (
    LinkerUnavailable,
    foreign_key_graph,
    is_connected,
    link_schema,
)

pytestmark = pytest.mark.xfail(
    raises=LinkerUnavailable, strict=True, reason="the core linker is not written yet"
)


def _table(
    name: str,
    columns: list[tuple[str, str]],
    fks: tuple[ForeignKey, ...] = (),
    values: dict[str, tuple[str, ...]] | None = None,
    description: str = "",
) -> Table:
    values = values or {}
    return Table(
        schema="shop",
        name=name,
        description=description,
        columns=tuple(
            Column(n, t, "", len(values[n]) if n in values else None, values.get(n))
            for n, t in columns
        ),
        primary_key=(columns[0][0],),
        foreign_keys=fks,
        upstream=(),
    )


SHOP = Catalog(
    tables=(
        _table(
            "customers",
            [("customer_id", "bigint"), ("name", "text"), ("country", "text")],
            values={"name": ("Zalando", "Otto"), "country": ("Germany", "France")},
        ),
        _table(
            "orders",
            [("order_id", "bigint"), ("customer_id", "bigint"), ("product_id", "bigint")],
            fks=(
                ForeignKey("customer_id", "shop.customers", "customer_id"),
                ForeignKey("product_id", "shop.products", "product_id"),
            ),
            description="One row per order.",
        ),
        _table(
            "products",
            [("product_id", "bigint"), ("title", "text"), ("supplier_id", "bigint")],
            fks=(ForeignKey("supplier_id", "shop.suppliers", "supplier_id"),),
        ),
        _table("suppliers", [("supplier_id", "bigint"), ("company", "text")]),
        _table("audit_log", [("entry_id", "bigint"), ("message", "text")]),
    ),
    source="information_schema",
)


def test_returns_catalog_relations_once_each() -> None:
    link = link_schema("How many orders per product?", SHOP, strategy="core", k=3)
    assert link.strategy == "core"
    assert link.tables
    assert set(link.tables) <= set(SHOP.relations())
    assert len(set(link.tables)) == len(link.tables)


def test_includes_the_table_holding_a_quoted_value() -> None:
    # "orders" wins lexically with k=1; Zalando is only a value of customers.name.
    link = link_schema("How many orders did Zalando place?", SHOP, strategy="core", k=1)
    assert "shop.orders" in link.tables
    assert "shop.customers" in link.tables


def test_adds_the_tables_on_the_join_path() -> None:
    # Orders and suppliers are only connected through products.
    link = link_schema("Which suppliers had orders?", SHOP, strategy="core", k=2)
    assert {"shop.orders", "shop.suppliers"} <= set(link.tables)
    assert "shop.products" in link.tables
    assert is_connected(link.tables, foreign_key_graph(SHOP))


def test_best_table_first() -> None:
    link = link_schema("Show every order.", SHOP, strategy="core", k=2)
    assert link.tables[0] == "shop.orders"


def test_no_evidence_falls_back_to_bm25() -> None:
    question = "xyzzy plugh?"
    core = link_schema(question, SHOP, strategy="core", k=2)
    bm25 = link_schema(question, SHOP, strategy="bm25", k=2)
    assert core.tables == bm25.tables


def test_deterministic() -> None:
    question = "Which German customers ordered products from suppliers?"
    first = link_schema(question, SHOP, strategy="core", k=2)
    assert all(link_schema(question, SHOP, strategy="core", k=2) == first for _ in range(3))


def test_finds_all_gold_tables_at_least_as_often_as_bm25_on_bird() -> None:
    """On the 500 official BIRD questions, at the same k (offline, committed catalogs)."""
    catalogs = load_catalogs()
    complete = {"core": 0, "bm25": 0}
    for q in data.official_questions():
        gold = gold_tables(q.sql, q.db_id)
        for strategy in complete:
            link = link_schema(q.prompt, catalogs[q.db_id], strategy=strategy, k=4)
            complete[strategy] += gold <= set(link.tables)
    assert complete["core"] >= complete["bm25"]
