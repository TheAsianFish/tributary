"""Catalog.to_dict / from_dict, including the committed BIRD catalogs."""

from __future__ import annotations

import json

import pytest

from eval.bird.load import CATALOGS, load_catalogs
from tributary.agent.catalog import Catalog, Column, ForeignKey, Table


def sample() -> Catalog:
    orders = Table(
        "marts",
        "orders",
        "One row per order.",
        (
            Column("id", "bigint", "Order id.", 100, None),
            Column("status", "text", "State.", 3, ("open", "closed", "it's")),
            Column("customer_id", "bigint", ""),
        ),
        ("id",),
        (ForeignKey("customer_id", "marts.customers", "id"),),
        ("seeds.raw_orders",),
    )
    customers = Table("marts", "customers", "", (Column("id", "bigint", "Id."),), ("id",), (), ())
    return Catalog((orders, customers), "dbt", "v1", "2026-01-01T00:00:00Z")


def test_roundtrip_preserves_everything() -> None:
    catalog = sample()
    assert Catalog.from_dict(catalog.to_dict()) == catalog


def test_roundtrip_survives_json_serialisation() -> None:
    catalog = sample()
    assert Catalog.from_dict(json.loads(json.dumps(catalog.to_dict()))) == catalog


def test_values_none_stays_none_and_empty_tuple_stays_empty() -> None:
    catalog = Catalog(
        (
            Table(
                "s",
                "t",
                "",
                (Column("a", "text", "", None, None), Column("b", "text", "", 0, ())),
                (),
                (),
                (),
            ),
        ),
        "information_schema",
    )

    back = Catalog.from_dict(catalog.to_dict())

    assert back.tables[0].columns[0].values is None
    assert back.tables[0].columns[1].values == ()


def test_from_dict_rejects_an_unknown_source() -> None:
    document = sample().to_dict()
    document["source"] = "csv"

    with pytest.raises(ValueError, match="unknown catalog source 'csv'"):
        Catalog.from_dict(document)


def test_committed_bird_catalogs_roundtrip() -> None:
    catalogs = load_catalogs()
    committed = json.loads(CATALOGS.read_text(encoding="utf-8"))

    assert len(catalogs) == 11
    assert set(catalogs) == set(committed)
    for db_id, catalog in catalogs.items():
        assert catalog.to_dict() == committed[db_id], db_id
