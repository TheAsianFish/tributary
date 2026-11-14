"""Few-shot examples: BM25 selection, rendering, the catalog path and both example sources."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
import sqlglot
import yaml

from eval.bird.data import BirdQuestion, official_questions, questions, select
from eval.bird.examples import bird_example_pools
from tributary.agent.catalog import Catalog, Column, Table, from_published
from tributary.agent.examples import Example, ExamplePool, render_examples
from tributary.agent.guard import Guard
from tributary.agent.linker import tokens
from tributary.warehouse.catalog import PublishedCatalog

ROOT = Path(__file__).resolve().parents[2]
MARTS_YAML = ROOT / "warehouse" / "models" / "marts" / "_marts.yml"
EVAL_QUESTIONS = ROOT / "eval" / "warehouse_questions.yaml"

POOL = ExamplePool(
    (
        Example("How many bot edits were made on Wikidata?", "SELECT 1", "a"),
        Example("Which pages on Wikidata have the most editors?", "SELECT 2", "b"),
        Example("What is the average page size on German Wikipedia?", "SELECT 3", "c"),
        Example("How many bot edits per hour?", "SELECT 4", "d"),
    )
)


# --- selection ----------------------------------------------------------------------------------


def test_most_similar_first() -> None:
    chosen = POOL.select("How many bot edits did Wikidata get?", k=3)
    assert [e.source for e in chosen] == ["a", "d", "b"]


def test_zero_score_examples_are_not_selected() -> None:
    chosen = POOL.select("average size of a German article", k=4)
    assert [e.source for e in chosen] == ["c"]
    assert POOL.select("completely unrelated xylophone", k=3) == ()


def test_k_bounds_the_selection() -> None:
    assert len(POOL.select("bot edits Wikidata hour", k=2)) == 2
    assert len(POOL.select("bot edits Wikidata hour", k=10)) == 3
    assert POOL.select("bot edits Wikidata", k=0) == ()


def test_the_asked_question_is_never_its_own_example() -> None:
    asked = "  how many BOT edits were made on wikidata?  "
    chosen = POOL.select(asked, k=4)
    assert "a" not in [e.source for e in chosen]
    assert chosen  # the others still compete
    assert chosen[0].source == "d"


def test_ties_keep_pool_order() -> None:
    pool = ExamplePool(
        tuple(Example(f"edits on wiki number {i}", f"SELECT {i}", str(i)) for i in range(5))
    )
    assert [e.source for e in pool.select("edits", k=5)] == ["0", "1", "2", "3", "4"]
    reversed_pool = ExamplePool(tuple(reversed(pool.examples)))
    assert [e.source for e in reversed_pool.select("edits", k=2)] == ["4", "3"]


def test_an_empty_pool_selects_nothing() -> None:
    assert ExamplePool(()).select("anything at all") == ()


def test_selection_is_the_same_in_every_process() -> None:
    script = (
        "from tributary.agent.examples import Example, ExamplePool\n"
        "pool = ExamplePool(tuple(Example(q, '') for q in [\n"
        "  'bot edits wikidata hour', 'wikidata hour pages', 'bot pages size',\n"
        "  'hour size bytes', 'edits bytes bot wikidata']))\n"
        "print([e.question for e in pool.select('bot wikidata hour bytes size pages', k=5)])\n"
        "print(pool.scores('bot wikidata hour bytes size pages'))\n"
    )
    outputs = set()
    for seed in ("0", "1", "12345"):
        env = {**os.environ, "PYTHONHASHSEED": seed}
        run = subprocess.run(
            [sys.executable, "-c", script], env=env, cwd=ROOT, capture_output=True, text=True
        )
        assert run.returncode == 0, run.stderr
        outputs.add(run.stdout)
    assert len(outputs) == 1


# --- rendering ----------------------------------------------------------------------------------


def test_render_examples() -> None:
    text = render_examples(
        [Example("Q one?", "SELECT 1"), Example("Q two?", "SELECT a\nFROM t\n", "x")]
    )
    assert text == (
        "Examples of questions on this database and the SQL that answers them:\n"
        "\n"
        "Question: Q one?\n"
        "SQL: SELECT 1\n"
        "\n"
        "Question: Q two?\n"
        "SQL: SELECT a\n"
        "FROM t\n"
    )


def test_render_no_examples_is_empty() -> None:
    assert render_examples([]) == ""
    assert render_examples(()) == ""


# --- the catalog carries them -------------------------------------------------------------------


def small_catalog(examples: tuple[Example, ...] = ()) -> Catalog:
    table = Table("marts", "t", "A table.", (Column("a", "bigint", "A."),), ("a",), (), ())
    return Catalog((table,), "dbt", "v1", "2026-01-01T00:00:00Z", examples)


def test_catalog_roundtrip_with_examples() -> None:
    catalog = small_catalog(
        (Example("Q?", "SELECT a FROM marts.t", "marts.t"), Example("R?", "SELECT 1"))
    )
    document = json.loads(json.dumps(catalog.to_dict()))
    assert document["examples"] == [
        {"question": "Q?", "sql": "SELECT a FROM marts.t", "source": "marts.t"},
        {"question": "R?", "sql": "SELECT 1", "source": ""},
    ]
    assert Catalog.from_dict(document) == catalog
    assert ExamplePool.from_catalog(catalog).examples == catalog.examples


def test_catalog_without_examples_serialises_as_before() -> None:
    document = small_catalog().to_dict()
    assert "examples" not in document
    assert list(document) == ["source", "version", "generated_at", "tables"]
    assert Catalog.from_dict(document).examples == ()


def test_examples_do_not_change_the_rendered_schema() -> None:
    with_examples = small_catalog((Example("Q?", "SELECT a FROM marts.t"),))
    assert with_examples.render() == small_catalog().render()


def published(nodes: dict[str, dict[str, Any]]) -> PublishedCatalog:
    """A published catalog with one-column marts ``nodes`` (manifest entries)."""
    catalog_nodes = {
        unique_id: {
            "metadata": {"schema": node["schema"], "name": node["name"]},
            "columns": {"a": {"name": "a", "type": "bigint", "index": 1}},
        }
        for unique_id, node in nodes.items()
    }
    return PublishedCatalog(
        path=Path("unused"),
        version="v1",
        manifest={"nodes": nodes, "sources": {}},
        catalog={"nodes": catalog_nodes},
        values={},
        export={},
    )


def model(name: str, **extra: Any) -> dict[str, Any]:
    # Any: a dbt manifest node.
    return {"resource_type": "model", "schema": "marts", "name": name, **extra}


def test_from_published_reads_meta_examples_from_config_and_top_level() -> None:
    nodes = {
        "model.p.zeta": model(
            "zeta",
            config={
                "meta": {"examples": [{"question": " Z? ", "sql": "SELECT a FROM marts.zeta"}]}
            },
            meta={"examples": [{"question": " Z? ", "sql": "SELECT a FROM marts.zeta"}]},
        ),
        "model.p.alpha": model(
            "alpha",
            meta={
                "examples": [
                    {"question": "A1?", "sql": "SELECT 1"},
                    {"question": "A2?", "sql": "SELECT 2\n"},
                ]
            },
        ),
        "model.p.plain": model("plain"),
        "model.p.staged": {**model("staged"), "schema": "staging", "meta": {"examples": []}},
    }
    catalog = from_published(published(nodes))
    # Relation order, then documented order; config.meta and its top-level mirror not doubled.
    assert catalog.examples == (
        Example("A1?", "SELECT 1", "marts.alpha"),
        Example("A2?", "SELECT 2", "marts.alpha"),
        Example("Z?", "SELECT a FROM marts.zeta", "marts.zeta"),
    )


def test_from_published_without_examples_has_none() -> None:
    assert from_published(published({"model.p.m": model("m")})).examples == ()


def test_from_published_rejects_malformed_examples() -> None:
    bad = model("m", meta={"examples": [{"question": "Q?"}]})
    with pytest.raises(ValueError, match=r"marts\.m"):
        from_published(published({"model.p.m": bad}))


# --- the warehouse's documented examples --------------------------------------------------------


def marts_models() -> list[dict[str, Any]]:
    # Any: the dbt properties YAML.
    models: list[dict[str, Any]] = yaml.safe_load(MARTS_YAML.read_text())["models"]
    return models


def documented() -> list[tuple[str, dict[str, str]]]:
    return [
        (f"marts.{m['name']}", e)
        for m in marts_models()
        for e in ((m.get("config") or {}).get("meta") or {}).get("examples", [])
    ]


def test_every_mart_documents_two_or_three_examples() -> None:
    for m in marts_models():
        examples = m["config"]["meta"]["examples"]
        assert 2 <= len(examples) <= 3, m["name"]


@pytest.mark.parametrize(("relation", "example"), documented())
def test_documented_example_sql_parses_and_passes_the_guard(
    relation: str, example: dict[str, str]
) -> None:
    sqlglot.parse_one(example["sql"], read="postgres")
    guard = Guard(f"marts.{m['name']}" for m in marts_models())
    result = guard.check(example["sql"])
    assert result.allowed, result.reasons
    assert relation in result.tables


def jaccard(a: str, b: str) -> float:
    x, y = set(tokens(a)), set(tokens(b))
    return len(x & y) / len(x | y) if x | y else 1.0


def test_documented_examples_do_not_leak_the_evaluation_questions() -> None:
    evaluation = [q["question"] for q in yaml.safe_load(EVAL_QUESTIONS.read_text())["questions"]]
    assert len(evaluation) == 40
    asked = {" ".join(q.split()).casefold() for q in evaluation}
    for _, example in documented():
        assert " ".join(example["question"].split()).casefold() not in asked
        for question in evaluation:
            assert jaccard(example["question"], question) < 0.6, (example["question"], question)


def test_the_jaccard_check_would_catch_a_paraphrase() -> None:
    assert (
        jaccard(
            "How many edits were made by bots? Return one number.",
            "How many edits were made by bots (editors with the bot flag)? Return one number.",
        )
        >= 0.6
    )


def test_documented_examples_reach_the_catalog_through_the_manifest() -> None:
    nodes = {
        f"model.tributary.{m['name']}": model(m["name"], config={"meta": m["config"]["meta"]})
        for m in marts_models()
    }
    catalog = from_published(published(nodes))
    assert len(catalog.examples) == len(documented())
    assert {e.source for e in catalog.examples} == {f"marts.{m['name']}" for m in marts_models()}
    chosen = ExamplePool.from_catalog(catalog).select("busiest bot accounts on Wikidata", k=1)
    assert chosen[0].source == "marts.agg_editor_activity"


# --- the BIRD held-out pool ---------------------------------------------------------------------


def test_bird_pools_exclude_dev50_and_stay_within_a_database() -> None:
    official = official_questions()
    dev50 = {q.id for q in select(questions("official"), "dev50")}
    assert len(dev50) == 50
    pools = bird_example_pools(dev50)
    by_id = {q.id: q for q in official}
    ids = [int(e.source.removeprefix("bird:")) for p in pools.values() for e in p.examples]
    assert len(ids) == len(set(ids)) == 450
    assert not set(ids) & dev50
    assert list(pools) == sorted({q.db_id for q in official})
    for db, pool in pools.items():
        for example in pool.examples:
            q = by_id[int(example.source.removeprefix("bird:"))]
            assert q.db_id == db
            assert example.question == q.prompt
            assert example.sql == q.sql


def test_bird_pool_never_offers_a_graded_question() -> None:
    def q(qid: int, db: str, text: str) -> BirdQuestion:
        return BirdQuestion(qid, db, text, "", f"SELECT {qid}", "simple", "official", "")

    pool = [q(3, "x", "count schools"), q(1, "x", "count schools in town"), q(2, "y", "schools")]
    pools = bird_example_pools({3}, pool)
    assert [e.source for e in pools["x"].examples] == ["bird:1"]
    assert [e.source for e in pools["y"].examples] == ["bird:2"]
    assert pools["x"].select("count schools") == (pools["x"].examples[0],)


def canonical_sql(query: str) -> str:
    """A query as parsed SQL with its output aliases removed (layout and case ignored)."""
    tree = sqlglot.parse_one(query, read="postgres")
    for node in tree.find_all(sqlglot.exp.Select):
        node.set(
            "expressions",
            [e.this if isinstance(e, sqlglot.exp.Alias) else e for e in node.expressions],
        )
    return tree.sql(dialect="postgres", normalize=True, comments=False)


def test_documented_examples_do_not_carry_an_evaluation_gold_query() -> None:
    # M13: a worked example that repeats a gold query would make the second pass agree with
    # the first mainly when the first is right, which favours the agreement hypothesis.
    gold = {
        canonical_sql(q["sql"]) for q in yaml.safe_load(EVAL_QUESTIONS.read_text())["questions"]
    }
    assert len(gold) == 40
    for relation, example in documented():
        assert canonical_sql(example["sql"]) not in gold, (relation, example["question"])


def test_the_gold_query_check_would_catch_a_reformatted_gold_query() -> None:
    first = yaml.safe_load(EVAL_QUESTIONS.read_text())["questions"][0]["sql"]
    disguised = "select   COUNT(*) as how_many\n  from MARTS.FCT_EDITS -- the total\n"
    assert canonical_sql(disguised) == canonical_sql(first)
    assert canonical_sql("SELECT count(*) FROM marts.fct_edits WHERE is_bot") != canonical_sql(
        first
    )
