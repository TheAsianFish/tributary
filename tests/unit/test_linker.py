"""Schema linking: tokens, the full and bm25 strategies, and the foreign-key graph helpers."""

from __future__ import annotations

import os
import subprocess
import sys

import pytest

from tributary.agent.catalog import Catalog, Column, ForeignKey, Table
from tributary.agent.linker import (
    LinkerUnavailable,
    foreign_key_graph,
    is_connected,
    link_schema,
    tokens,
)


def col(name: str, description: str = "", values: tuple[str, ...] | None = None) -> Column:
    return Column(name, "text", description, None, values)


def table(
    name: str,
    columns: tuple[Column, ...],
    description: str = "",
    fks: tuple[ForeignKey, ...] = (),
    schema: str = "s",
) -> Table:
    return Table(schema, name, description, columns, (), fks, ())


def school_catalog() -> Catalog:
    return Catalog(
        (
            table("zebra", (col("stripes"), col("habitat"))),
            table(
                "schools",
                (col("school_name", "Name of the school"), col("county")),
                "Public schools of California",
                (ForeignKey("county", "s.districts", "county"),),
            ),
            table("districts", (col("county"), col("budget"))),
            table("orders", (col("total"), col("customer"))),
            table("islands", (col("area"),)),
        ),
        "information_schema",
    )


def test_tokens_split_camel_and_snake_case() -> None:
    assert tokens("CDSCode") == ["cds", "code"]
    assert tokens("freeMealCount") == ["free", "meal", "count"]
    assert tokens("free_meal_count") == ["free", "meal", "count"]


def test_tokens_drop_stopwords_and_lowercase() -> None:
    assert tokens("What is the Highest budget?") == ["highest", "budget"]
    assert tokens("List the names of all schools") == ["school"]


def test_tokens_stem_plurals() -> None:
    assert tokens("schools") == ["school"]
    assert tokens("countries") == ["country"]
    assert tokens("class") == ["class"]
    assert tokens("status") == ["status"]
    assert tokens("bus") == ["bus"]


def test_full_returns_every_relation_in_catalog_order() -> None:
    catalog = school_catalog()

    link = link_schema("anything", catalog, strategy="full")

    assert link.strategy == "full"
    assert link.tables == catalog.relations()
    assert link.tables[0] == "s.zebra"
    assert set(link.scores.values()) == {1.0}


def test_full_is_the_default_strategy() -> None:
    catalog = school_catalog()
    assert link_schema("q", catalog).tables == catalog.relations()


def test_bm25_ranks_the_table_the_question_names_first() -> None:
    link = link_schema("How many schools are in each county?", school_catalog(), strategy="bm25")

    assert link.strategy == "bm25"
    assert link.tables[0] == "s.schools"


def test_bm25_matches_column_names_and_descriptions() -> None:
    catalog = school_catalog()

    assert link_schema("average budget", catalog, strategy="bm25", k=1).tables == ("s.districts",)
    assert link_schema("which habitat has stripes", catalog, strategy="bm25", k=1).tables == (
        "s.zebra",
    )


def test_bm25_matches_indexed_values() -> None:
    catalog = Catalog(
        (
            table("a", (col("x"),)),
            table("b", (col("language", values=("dewiki", "frwiki")),)),
        ),
        "dbt",
    )

    assert link_schema("edits on dewiki", catalog, strategy="bm25", k=1).tables == ("s.b",)


def test_bm25_returns_exactly_k_tables() -> None:
    catalog = school_catalog()

    for k in (1, 2, 3, 5):
        assert len(link_schema("schools", catalog, strategy="bm25", k=k).tables) == k


def test_bm25_default_k_is_four_and_k_is_capped_by_the_catalog() -> None:
    catalog = school_catalog()
    assert len(link_schema("schools", catalog, strategy="bm25").tables) == 4
    assert len(link_schema("schools", catalog, strategy="bm25", k=99).tables) == 5


def test_bm25_is_deterministic() -> None:
    catalog = school_catalog()
    first = link_schema("schools in each county", catalog, strategy="bm25", k=3)
    second = link_schema("schools in each county", catalog, strategy="bm25", k=3)
    assert first == second


def test_bm25_breaks_ties_by_relation_name() -> None:
    catalog = Catalog(
        (table("b", (col("x"),)), table("a", (col("x"),)), table("c", (col("x"),))),
        "information_schema",
    )

    link = link_schema("unrelated words", catalog, strategy="bm25", k=3)

    assert set(link.scores.values()) == {0.0}
    assert link.tables == ("s.a", "s.b", "s.c")


def test_bm25_scores_cover_every_table_even_when_k_is_smaller() -> None:
    catalog = school_catalog()

    link = link_schema("schools", catalog, strategy="bm25", k=2)

    assert set(link.scores) == set(catalog.relations())
    assert link.scores["s.schools"] > link.scores["s.orders"] == 0.0
    assert list(link.scores)[:2] == list(link.tables)


def test_bm25_on_an_empty_catalog_returns_nothing() -> None:
    link = link_schema("q", Catalog((), "information_schema"), strategy="bm25")
    assert link.tables == ()
    assert link.scores == {}


def test_link_supports_membership() -> None:
    link = link_schema("schools", school_catalog(), strategy="bm25", k=1)
    assert "s.schools" in link
    assert "s.zebra" not in link


def test_unknown_strategy_raises_value_error() -> None:
    with pytest.raises(ValueError, match="unknown linking strategy 'magic'"):
        link_schema("q", school_catalog(), strategy="magic")


def test_core_is_unavailable_rather_than_falling_back() -> None:
    with pytest.raises(LinkerUnavailable):
        link_schema("q", school_catalog(), strategy="core")


def test_foreign_key_graph_is_undirected_and_covers_isolated_tables() -> None:
    graph = foreign_key_graph(school_catalog())

    assert graph["s.schools"] == {"s.districts"}
    assert graph["s.districts"] == {"s.schools"}
    assert graph["s.zebra"] == set()
    assert set(graph) == set(school_catalog().relations())


def test_foreign_key_graph_ignores_self_references_and_unknown_targets() -> None:
    catalog = Catalog(
        (
            table(
                "emp",
                (col("id"), col("boss")),
                fks=(ForeignKey("boss", "s.emp", "id"), ForeignKey("id", "s.ghost", "id")),
            ),
        ),
        "information_schema",
    )

    assert foreign_key_graph(catalog) == {"s.emp": set()}


def test_is_connected() -> None:
    graph = {"a": {"b"}, "b": {"a", "c"}, "c": {"b"}, "d": set()}

    assert is_connected(["a", "b", "c"], graph)
    assert not is_connected(["a", "c"], graph)  # only joined through b, which is not selected
    assert not is_connected(["a", "d"], graph)
    assert is_connected(["d"], graph)
    assert is_connected([], graph)


@pytest.mark.parametrize("k", [0, -1])
def test_k_below_one_is_refused(k: int) -> None:
    with pytest.raises(ValueError, match="k must be at least 1"):
        link_schema("schools", school_catalog(), strategy="bm25", k=k)


def test_bm25_scores_do_not_depend_on_the_hash_seed() -> None:
    """Set order follows PYTHONHASHSEED; the float sum must not, or ties flip between runs."""
    script = (
        "from tributary.agent.catalog import Catalog, Column, Table\n"
        "from tributary.agent.linker import BM25\n"
        "words = [f'w{i}' for i in range(40)]\n"
        "tables = tuple(Table('s', f't{j}', ' '.join(words[j::j + 2]),"
        " (Column('c', 'text', ' '.join(words[: 10 + j]), None, None),), (), (), ())"
        " for j in range(8))\n"
        "scores = BM25().scores(' '.join(words), Catalog(tables, 'information_schema'))\n"
        "print(sorted((r, s.hex()) for r, s in scores.items()))\n"
    )
    outputs = {
        subprocess.run(
            [sys.executable, "-c", script],
            env={**os.environ, "PYTHONHASHSEED": str(seed)},
            capture_output=True,
            text=True,
            check=True,
        ).stdout
        for seed in range(6)
    }
    assert len(outputs) == 1
