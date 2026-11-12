"""The guard's differential oracle, offline: recorded ev_action text and a fake resolver.

The fixture ``tests/fixtures/guard_oracle/ev_action.json`` was recorded from
PostgreSQL 17 with ``python -m eval.guard.differential record``: each
statement's ``pg_rewrite.ev_action`` and the catalog rows its ids name.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from eval.guard.differential import (
    Case,
    Catalog,
    Finding,
    Outcome,
    Proc,
    Report,
    generate,
    gold_rewrites,
    judge,
    literal_casts,
    load_templates,
    parse_ev_action,
    quote,
    run,
)
from tributary.agent.guard import (
    HISTORICAL_FIXES,
    Guard,
    fixes_disabled,
    function_refusal,
)

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "guard_oracle" / "ev_action.json"
MARTS = (
    "marts.agg_editor_activity",
    "marts.agg_edits_hourly",
    "marts.dim_page",
    "marts.dim_wiki",
    "marts.fct_edits",
)


@pytest.fixture(scope="module")
def recorded() -> dict[str, Any]:
    document: dict[str, Any] = json.loads(FIXTURE.read_text())
    return document


@pytest.fixture(scope="module")
def catalog(recorded: dict[str, Any]) -> Catalog:
    return Catalog.from_json(recorded["catalog"])


def _case(recorded: dict[str, Any], prefix: str) -> str:
    for case in recorded["cases"]:
        if case["sql"].startswith(prefix):
            return str(case["ev_action"])
    raise KeyError(prefix)


def _names(text: str, catalog: Catalog) -> list[tuple[str, str]]:
    out = []
    for ref in parse_ev_action(text).references:
        if ref.kind == "operator":
            out.append((ref.kind, catalog.operators[ref.oid].name))
        elif ref.kind == "relation":
            out.append((ref.kind, catalog.relations[ref.oid]))
        elif ref.oid in catalog.procs:
            out.append((ref.kind, catalog.procs[ref.oid].name))
        else:
            out.append((ref.kind, str(ref.oid)))
    return out


def _judge(text: str, catalog: Catalog) -> tuple[Finding, ...]:
    return judge(parse_ev_action(text), catalog, frozenset(MARTS))


# -- reading ev_action ------------------------------------------------------------------


def test_whole_row_notation_resolves_to_a_call(recorded: dict[str, Any], catalog: Catalog) -> None:
    text = _case(recorded, "SELECT w.row_to_json")
    assert _names(text, catalog) == [
        ("relation", "marts.dim_wiki"),
        ("call", "row_to_json"),
        ("aggregate", "count"),
    ]
    # row_to_json is on the guard's allowlist: resolved, not flagged.
    assert _judge(text, catalog) == ()


def test_a_denied_function_reached_as_a_column_is_flagged(
    recorded: dict[str, Any], catalog: Catalog
) -> None:
    findings = _judge(_case(recorded, "SELECT w.pg_typeof"), catalog)
    assert findings == (Finding("denied", "pg_catalog.pg_typeof", "call", "s"),)


def test_a_function_off_the_allowlist_is_flagged(
    recorded: dict[str, Any], catalog: Catalog
) -> None:
    findings = _judge(_case(recorded, "SELECT 1 AS x FROM marts.dim_wiki AS w WHERE"), catalog)
    assert findings == (Finding("not_allowed", "pg_catalog.quote_literal", "call", "s"),)


def test_aggregates_windows_operators_and_sort_operators_are_read(
    recorded: dict[str, Any], catalog: Catalog
) -> None:
    names = _names(_case(recorded, "SELECT wiki, count(*) AS n, rank()"), catalog)
    kinds = {kind for kind, _ in names}
    assert {"relation", "call", "aggregate", "window", "operator", "cast"} <= kinds
    assert ("window", "rank") in names
    assert ("call", "length") in names
    assert ("cast", "int8") in names  # count(*) compared with an int4 literal
    # ORDER BY n DESC and GROUP BY wiki leave their sort and equality operators.
    assert sum(kind == "operator" for kind, _ in names) >= 4
    assert _judge(_case(recorded, "SELECT wiki, count(*) AS n, rank()"), catalog) == ()


def test_casts_syntax_and_io_conversions(recorded: dict[str, Any], catalog: Catalog) -> None:
    text = _case(recorded, "SELECT EXTRACT(YEAR")
    resolution = parse_ev_action(text)
    names = _names(text, catalog)
    assert ("syntax", "extract") in names
    assert ("cast", "int8") in names
    # CAST(edits AS text) is an I/O conversion: no function id, counted instead.
    assert resolution.io_casts == 1
    # CAST('7' AS integer) was folded into a constant by the parser: no trace at all.
    assert all(name != "int4in" for _, name in names)
    assert _judge(text, catalog) == ()


def test_identity_functions_and_relations_outside_the_allowlist(
    recorded: dict[str, Any], catalog: Catalog
) -> None:
    assert _judge(_case(recorded, "SELECT current_user"), catalog) == (
        Finding("identity", "current_user", "sql_function"),
    )
    assert _judge(_case(recorded, "SELECT count(*) AS n FROM raw"), catalog) == (
        Finding("relation", "raw.recentchange", "relation"),
    )


def test_row_comparison_in_list_distinct_and_nullif(
    recorded: dict[str, Any], catalog: Catalog
) -> None:
    names = _names(_case(recorded, "SELECT 1 AS x WHERE (1, 2)"), catalog)
    assert [n for k, n in names if k == "operator"].count("=") == 4
    assert ("operator", "<") in names


def test_a_statement_postgres_refused_left_no_rule(recorded: dict[str, Any]) -> None:
    refused = [c for c in recorded["cases"] if c["error"]]
    assert {c["error"] for c in refused} == {
        "column w.no_such_function does not exist",
        "column d.text does not exist",
    }
    assert all(c["ev_action"] == "" for c in refused)


def test_fields_belong_to_their_own_node() -> None:
    # The relation id comes after the RTE's alias children; a quoted alias holding a
    # brace is escaped and must not open a node.
    text = (
        "({QUERY :rtable ({RANGETBLENTRY :alias {ALIAS :aliasname w\\{x :colnames <>}"
        ' :eref {ALIAS :aliasname w :colnames ("a")} :rtekind 0 :relid 42}) :targetList'
        " ({TARGETENTRY :expr {FUNCEXPR :funcid 7 :funcformat 0 :args ({CONST :constvalue"
        " 4 [ 1 0 0 0 ]})} :resno 1})"
        " :windowClause ({WINDOWCLAUSE :orderClause ({SORTGROUPCLAUSE :eqop 96 :sortop 97})"
        " :startInRangeFunc 11 :endInRangeFunc 0})})"
    )
    resolution = parse_ev_action(text)
    assert [(r.kind, r.oid) for r in resolution.references] == [
        ("relation", 42),
        ("call", 7),
        ("operator", 96),
        ("operator", 97),
        ("syntax", 11),
    ]
    assert resolution.nodes["ALIAS"] == 2


def test_unknown_ids_and_other_schemas_are_findings() -> None:
    catalog = Catalog(
        procs={1: Proc(1, "public", "f", "f", "v", "integer")},
        operators={2: Operator_(2)},
    )
    text = "({QUERY :x {FUNCEXPR :funcid 1 :funcformat 0} {OPEXPR :opno 2 :opfuncid 1}"
    text += " {FUNCEXPR :funcid 99 :funcformat 0}})"
    findings = judge(parse_ev_action(text), catalog, frozenset())
    rules = {(f.rule, f.name) for f in findings}
    assert ("schema", "public.f") in rules
    assert ("schema", "public.===") in rules
    assert ("not_allowed", "public.f") in rules
    assert ("unknown", "oid 99") in rules


def Operator_(oid: int) -> Any:  # noqa: N802 - a fixture builder named for what it returns
    from eval.guard.differential import Operator

    return Operator(oid, "public", "===", 1)


def test_catalog_round_trips(catalog: Catalog) -> None:
    assert Catalog.from_json(catalog.to_json()) == catalog


# -- the guard's policy and the switches ------------------------------------------------


def test_function_refusal_is_the_guards_policy() -> None:
    assert function_refusal("pg_sleep", explicit_call=True) == "denied"
    assert function_refusal("pg_sleep", explicit_call=False) == "denied"
    assert function_refusal("quote_literal", explicit_call=True) == "not_allowed"
    # A function reached through an operator or a cast is named by nobody.
    assert function_refusal("textcat", explicit_call=False) is None
    assert function_refusal("row_to_json", explicit_call=True) is None  # allowlisted
    assert function_refusal("count", explicit_call=True) is None  # typed by sqlglot
    guard = Guard(MARTS)
    assert not guard.check("SELECT quote_literal(wiki) FROM marts.dim_wiki").allowed
    assert guard.check("SELECT row_to_json(w) FROM marts.dim_wiki AS w").allowed


# One statement per historical bypass, each admitted only with its fix switched off.
HISTORICAL = {
    "field_notation": "SELECT (0.01).pg_sleep",
    "function_alias": "SELECT t.pg_sleep FROM generate_series(1, 10) AS t",
    "alias_intersection": (
        "SELECT p.pg_sleep FROM unnest(ARRAY[1]) AS p(x)"
        " WHERE EXISTS (SELECT 1 FROM generate_series(1, 1) AS p(pg_sleep))"
    ),
}


# The rebound alias is a refinement of the function-alias check, so switching that check
# off reopens both bypasses.
REOPENS = {
    "field_notation": {"field_notation"},
    "function_alias": {"function_alias", "alias_intersection"},
    "alias_intersection": {"alias_intersection"},
}


@pytest.mark.parametrize("fix", HISTORICAL_FIXES)
def test_each_historical_fix_can_be_switched_off_alone(fix: str) -> None:
    guard = Guard(MARTS)
    assert set(HISTORICAL) == set(HISTORICAL_FIXES)
    for name, sql in HISTORICAL.items():
        assert not guard.check(sql).allowed, name
    with fixes_disabled(fix):
        for name, sql in HISTORICAL.items():
            assert guard.check(sql).allowed == (name in REOPENS[fix]), name
    for name, sql in HISTORICAL.items():
        assert not guard.check(sql).allowed, name


def test_an_unknown_fix_name_is_refused() -> None:
    with pytest.raises(ValueError, match="unknown guard fix"), fixes_disabled("nope"):
        pass


# -- the generator ----------------------------------------------------------------------


PROCS = (
    Proc(1, "pg_catalog", "pg_typeof", "f", "s", '"any"'),
    Proc(2, "pg_catalog", "lower", "f", "i", "text"),
    Proc(3, "pg_catalog", "timestamp", "f", "i", "date"),
)


def test_every_template_in_every_position_for_every_function() -> None:
    templates = load_templates()
    cases = list(generate(PROCS, templates, frozenset({"timestamp"})))
    assert len(cases) == len(PROCS) * len(templates.templates) * len(templates.positions)
    ids = {t["id"] for t in templates.templates}
    # The spellings the plan names, fixed before the guard change.
    assert {
        "field",
        "mart_alias",
        "subquery",
        "cte",
        "values",
        "from_function",
        "scalar_from_function",
        "reused_alias",
    } <= ids
    assert set(templates.positions) == {"select", "where", "order_by", "filter"}
    by_key = {(c.group, c.position, c.function): c.sql for c in cases}
    assert by_key["mart_alias", "select", "pg_typeof"] == (
        "SELECT w.pg_typeof AS x FROM marts.dim_wiki AS w"
    )
    assert by_key["cte", "filter", "lower"] == (
        "WITH c AS (SELECT wiki, edits FROM marts.dim_wiki) "
        "SELECT count(*) FILTER (WHERE (c.lower) IS NOT NULL) AS x FROM c"
    )
    # Scalar FROM functions are crossed by argument type.
    assert by_key["scalar_from_function", "where", "lower"] == (
        "SELECT 1 AS x FROM lower('x') AS t WHERE (t.lower) IS NOT NULL"
    )
    assert "make_date(2024, 1, 1) AS t" in by_key["scalar_from_function", "select", "timestamp"]
    assert by_key["field", "order_by", "pg_typeof"] == (
        "SELECT 1 AS x ORDER BY ((abs(1)).pg_typeof) IS NULL"
    )
    # A keyword is quoted wherever it is a name.
    assert 'p("timestamp")' in by_key["reused_alias", "select", "timestamp"]


def test_quote() -> None:
    assert quote("lower", frozenset()) == "lower"
    assert quote("timestamp", frozenset({"timestamp"})) == '"timestamp"'
    assert quote("Odd", frozenset()) == '"Odd"'


def test_gold_rewrites() -> None:
    gold = [
        ("q1", "SELECT f.wiki, count(*) AS n FROM marts.fct_edits AS f GROUP BY f.wiki"),
        ("q2", "SELECT 1 AS a UNION SELECT 2"),
    ]
    rewrites = ("identity", "outer_alias", "as_subquery", "as_cte")
    cases = list(gold_rewrites(gold, PROCS[:1], rewrites, frozenset()))
    groups = [(c.group, c.source) for c in cases]
    assert groups == [
        ("gold:identity", "q1"),
        ("gold:outer_alias", "q1"),
        ("gold:as_subquery", "q1"),
        ("gold:as_cte", "q1"),
        ("gold:identity", "q2"),
        # A set operation has no outer FROM alias to rewrite.
        ("gold:as_subquery", "q2"),
        ("gold:as_cte", "q2"),
    ]
    assert "WHERE (f.pg_typeof) IS NOT NULL" in cases[1].sql
    assert cases[2].sql.startswith("SELECT g.pg_typeof AS x FROM (SELECT f.wiki")


def test_literal_casts_are_counted() -> None:
    assert literal_casts("SELECT CAST('7' AS integer), '1'::bigint, -1::numeric") == 3
    assert literal_casts("SELECT CAST(edits AS text) FROM marts.dim_wiki") == 0


# -- a run ------------------------------------------------------------------------------


def test_run_tallies_generated_admitted_refused_and_flagged() -> None:
    cases = [
        Case("t", "select", "f", "", "SELECT pg_sleep(1)"),  # blocked by the guard
        Case("t", "select", "f", "", "SELECT w.wiki FROM marts.dim_wiki AS w"),
        Case("t", "where", "f", "", "SELECT w.pg_typeof FROM marts.dim_wiki AS w"),
        Case("t", "where", "f", "", "SELECT w.nothing FROM marts.dim_wiki AS w"),
    ]
    flagged = Finding("denied", "pg_catalog.pg_typeof", "call", "s")

    def resolve(sql: str) -> Outcome:
        if "pg_typeof" in sql:
            return Outcome(True, findings=(flagged,), nodes={"FUNCEXPR": 1})
        if "nothing" in sql:
            return Outcome(False, "42703", "column w.nothing does not exist")
        return Outcome(True, io_casts=1)

    report = run(cases, Guard(MARTS), resolve, Report("test", ()))
    document = report.to_json()
    assert document["total"] == {
        "generated": 4,
        "admitted": 3,
        "refused_by_postgres": 1,
        "resolved": 2,
        "flagged": 1,
        "with_io_cast": 1,
        "with_literal_cast": 0,
    }
    assert document["by_position"]["where"]["flagged"] == 1
    assert document["refused_by_postgres_sqlstates"] == {"42703": 1}
    assert document["reachable_by_volatility"] == {"stable": ["pg_catalog.pg_typeof"]}
    entry = document["reachable"]["pg_catalog.pg_typeof"]
    assert entry["statements"] == 1
    assert entry["groups"] == {"t": 1}
    assert "pg_typeof" in entry["example"]


def test_run_applies_the_disabled_fixes_only_inside_the_run() -> None:
    cases = [Case("field", "select", "pg_sleep", "", "SELECT (0.01).pg_sleep")]
    seen: list[str] = []

    def resolve(sql: str) -> Outcome:
        seen.append(sql)
        return Outcome(True)

    run(cases, Guard(MARTS), resolve, Report("off", ("field_notation",)))
    assert len(seen) == 1
    assert not Guard(MARTS).check("SELECT (0.01).pg_sleep").allowed
