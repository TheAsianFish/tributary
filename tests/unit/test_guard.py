"""The SQL guard: every reason code, the rewrites, scoping, and benign analytics."""

from __future__ import annotations

import pytest
import sqlglot

from tributary.agent.guard import ANONYMOUS_ALLOWED, Guard, GuardBlocked, GuardResult

MARTS = (
    "marts.agg_editor_activity",
    "marts.agg_edits_hourly",
    "marts.dim_page",
    "marts.dim_wiki",
    "marts.fct_edits",
)


@pytest.fixture
def guard() -> Guard:
    return Guard(MARTS, max_rows=1000)


def _blocked(guard: Guard, sql: str) -> GuardResult:
    result = guard.check(sql)
    assert not result.allowed, result
    assert result.sql == ""
    assert result.limit is None
    assert len(result.reasons) == len(set(result.reasons))
    return result


# -- reason codes ----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("sql", "code"),
    [
        ("SELEC wiki FROM marts.dim_wiki", "parse_error"),
        ("SELECT * FROM (", "parse_error"),
        ('SELECT * FROM U&"\\0070g_shadow"', "parse_error"),
        ("TABLE marts.fct_edits", "parse_error"),
        ("", "empty"),
        ("   ", "empty"),
        (";", "empty"),
        ("-- nothing here", "empty"),
        ("/* nothing */", "empty"),
        ("SELECT 1; SELECT 2", "multiple_statements"),
        ("SELECT 1;;SELECT 2", "multiple_statements"),
        ("VALUES (1)", "not_select"),
        ("EXPLAIN SELECT 1", "not_select"),
        ("SHOW search_path", "not_select"),
        ("SET statement_timeout = 0", "not_select"),
        ("SET ROLE postgres", "not_select"),
        ("RESET ALL", "not_select"),
        ("BEGIN", "not_select"),
        ("START TRANSACTION", "not_select"),
        ("COMMIT", "not_select"),
        ("ROLLBACK", "not_select"),
        ("CALL p()", "not_select"),
        ("DO $$ BEGIN NULL; END $$", "not_select"),
        ("PREPARE p AS SELECT 1", "not_select"),
        ("EXECUTE p", "not_select"),
        ("LISTEN c", "not_select"),
        ("NOTIFY c", "not_select"),
        ("VACUUM marts.fct_edits", "not_select"),
        ("CHECKPOINT", "not_select"),
        ("DISCARD ALL", "not_select"),
        ("LOAD 'plugin'", "not_select"),
        ("INSERT INTO marts.dim_wiki (wiki) VALUES ('x')", "write_or_ddl"),
        ("UPDATE marts.dim_wiki SET edits = 0", "write_or_ddl"),
        ("DELETE FROM marts.dim_wiki", "write_or_ddl"),
        (
            "MERGE INTO marts.dim_wiki d USING marts.dim_wiki s ON true WHEN MATCHED THEN DELETE",
            "write_or_ddl",
        ),
        ("TRUNCATE marts.dim_wiki", "write_or_ddl"),
        ("DROP TABLE marts.dim_wiki", "write_or_ddl"),
        ("CREATE TABLE t (x int)", "write_or_ddl"),
        ("CREATE TEMP TABLE t AS SELECT 1", "write_or_ddl"),
        ("ALTER TABLE marts.dim_wiki ADD COLUMN y int", "write_or_ddl"),
        ("REVOKE ALL ON marts.dim_wiki FROM public", "write_or_ddl"),
        ("COPY marts.dim_wiki TO PROGRAM 'id'", "write_or_ddl"),
        ("ANALYZE marts.dim_wiki", "write_or_ddl"),
        ("WITH d AS (DELETE FROM marts.dim_wiki RETURNING *) SELECT * FROM d", "write_or_ddl"),
        (
            "WITH d AS (INSERT INTO marts.dim_wiki (wiki) VALUES ('x') RETURNING *) SELECT 1",
            "write_or_ddl",
        ),
        ("SELECT * INTO t FROM marts.dim_wiki", "select_into"),
        ("SELECT * INTO TEMP t FROM marts.dim_wiki", "select_into"),
        ("SELECT * FROM marts.dim_wiki FOR UPDATE", "locking_clause"),
        ("SELECT * FROM marts.dim_wiki FOR NO KEY UPDATE", "locking_clause"),
        ("SELECT * FROM marts.dim_wiki FOR SHARE NOWAIT", "locking_clause"),
        ("SELECT * FROM marts.dim_wiki FOR KEY SHARE SKIP LOCKED", "locking_clause"),
        ("SELECT * FROM (SELECT * FROM marts.dim_wiki FOR UPDATE) s", "locking_clause"),
        (
            "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r) SELECT n FROM r",
            "recursive_cte",
        ),
        ("SELECT * FROM raw.recentchange", "relation_not_allowed"),
        ("SELECT * FROM ops.quality_runs", "relation_not_allowed"),
        ("SELECT * FROM staging.stg_recentchange", "relation_not_allowed"),
        ("SELECT * FROM recentchange", "relation_not_allowed"),
        ("SELECT * FROM db.marts.fct_edits", "relation_not_allowed"),
        ('SELECT * FROM "MARTS"."FCT_EDITS"', "relation_not_allowed"),
        ('SELECT * FROM "Fct_Edits"', "relation_not_allowed"),
        ("SELECT * FROM public.fct_edits", "relation_not_allowed"),
        ("SELECT * FROM pg_catalog.pg_roles", "system_catalog"),
        ("SELECT * FROM information_schema.columns", "system_catalog"),
        ("SELECT * FROM INFORMATION_SCHEMA.TABLES", "system_catalog"),
        ("SELECT * FROM pg_shadow", "system_catalog"),
        ("SELECT * FROM pg_toast.pg_toast_1", "system_catalog"),
        ("SELECT 'raw.recentchange'::regclass", "system_catalog"),
        ("SELECT pg_catalog.pg_roles.rolname", "system_catalog"),
        ("SELECT pg_sleep(1)", "function_not_allowed"),
        ('SELECT "PG_SLEEP"(1)', "function_not_allowed"),
        ('SELECT "pg_sleep"(1)', "function_not_allowed"),
        ("SELECT Pg_Sleep(1)", "function_not_allowed"),
        ("SELECT pg_sleep  (1)", "function_not_allowed"),
        ("SELECT * FROM pg_ls_dir('.')", "function_not_allowed"),
        ("SELECT * FROM marts.dim_wiki, LATERAL pg_ls_dir('.') AS f", "function_not_allowed"),
        ("SELECT * FROM ROWS FROM (pg_ls_dir('.'))", "function_not_allowed"),
        ("SELECT version()", "function_not_allowed"),
        ("SELECT current_user", "function_not_allowed"),
        ("SELECT set_config('statement_timeout', '0', false)", "function_not_allowed"),
        ("SELECT current_setting('data_directory')", "function_not_allowed"),
        ("SELECT nextval('s')", "function_not_allowed"),
        ("SELECT txid_current()", "function_not_allowed"),
        ("SELECT lo_import('/etc/passwd')", "function_not_allowed"),
        ("SELECT dblink_exec('x', 'drop table y')", "function_not_allowed"),
        ("SELECT query_to_xml('delete from t', true, true, '')", "function_not_allowed"),
        ("SELECT has_table_privilege('raw.recentchange', 'select')", "function_not_allowed"),
        ("SELECT xpath('/a', '<a/>'::xml)", "function_not_allowed"),
        ("SELECT setseed(0.5)", "function_not_allowed"),
        ("SELECT my_udf(1)", "function_not_allowed"),
        ('SELECT "Age"(now())', "function_not_allowed"),
        ("SELECT x::my_type FROM marts.dim_wiki", "function_not_allowed"),
        ("SELECT wiki FROM marts.dim_wiki ORDER BY pg_sleep(1)", "function_not_allowed"),
        (
            "SELECT wiki FROM marts.dim_wiki GROUP BY wiki HAVING max(pg_sleep(1)::text) > ''",
            "function_not_allowed",
        ),
        ("SELECT CAST(pg_sleep(1) AS text)", "function_not_allowed"),
        ("SELECT row_number() OVER (ORDER BY pg_sleep(1))", "function_not_allowed"),
        ("SELECT pg_catalog.pg_sleep(1)", "qualified_function"),
        ("SELECT public.f()", "qualified_function"),
        ("SELECT pg_catalog.lower('A')", "qualified_function"),
        ("SELECT $1", "parameter"),
        ("SELECT %s", "parameter"),
        ("SELECT ?", "parameter"),
        ("SELECT :name", "parameter"),
        ("SELECT %(name)s", "parameter"),
    ],
)
def test_reason_codes(guard: Guard, sql: str, code: str) -> None:
    assert code in _blocked(guard, sql).codes


def test_ambiguous_relation() -> None:
    guard = Guard(("marts.events", "archive.events"))
    result = _blocked(guard, "SELECT * FROM events")
    assert result.codes == ("ambiguous_relation",)
    assert result.tables == ("archive.events", "marts.events")
    assert guard.check("SELECT * FROM archive.events").allowed


def test_reasons_are_collected_and_deduplicated(guard: Guard) -> None:
    result = _blocked(
        guard,
        "SELECT pg_sleep(1), pg_sleep(2) FROM raw.recentchange; DELETE FROM marts.fct_edits",
    )
    assert set(result.codes) == {
        "multiple_statements",
        "function_not_allowed",
        "relation_not_allowed",
        "write_or_ddl",
    }
    assert result.codes.count("function_not_allowed") == 1
    assert all(": " in reason for reason in result.reasons)


def test_blocked_tables_are_best_effort(guard: Guard) -> None:
    result = _blocked(guard, "SELECT * FROM marts.fct_edits JOIN raw.recentchange USING (wiki)")
    assert result.tables == ("marts.fct_edits", "raw.recentchange")


def test_enforce_raises_with_result(guard: Guard) -> None:
    with pytest.raises(GuardBlocked) as caught:
        guard.enforce("DROP TABLE marts.fct_edits")
    assert caught.value.result.codes == ("write_or_ddl",)
    assert "write_or_ddl" in str(caught.value)
    assert guard.enforce("SELECT 1").allowed


@pytest.mark.parametrize("relation", ["fct_edits", "a.b.c", "", "marts."])
def test_relations_must_be_schema_qualified(relation: str) -> None:
    with pytest.raises(ValueError, match=r"schema\.table"):
        Guard((relation,))


def test_max_rows_must_be_positive() -> None:
    with pytest.raises(ValueError, match="max_rows"):
        Guard(MARTS, max_rows=0)


def test_allowlist_is_case_insensitive_for_unquoted(guard: Guard) -> None:
    upper = Guard(("MARTS.FCT_EDITS",))
    assert upper.check("SELECT * FROM marts.fct_edits").allowed
    result = guard.check("SELECT * FROM MARTS.Fct_Edits")
    assert result.allowed
    assert result.tables == ("marts.fct_edits",)
    assert guard.check('SELECT * FROM "marts"."fct_edits"').allowed


# -- comments and stacking -------------------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 1 /* ; DROP TABLE marts.fct_edits */",
        "SELECT 1 -- ; DROP TABLE marts.fct_edits",
        "/* lead */ SELECT /* mid */ 1 /* a /* nested */ ; DROP TABLE x */",
        "SELECT 1 -- trailing\n",
    ],
)
def test_comments_never_reach_the_database(guard: Guard, sql: str) -> None:
    result = guard.check(sql)
    assert result.allowed
    assert result.sql == "SELECT 1 LIMIT 1000"


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 1 -- comment\n; DROP TABLE marts.fct_edits",
        "SELECT 1 /* a /* nested */ b */; DROP TABLE marts.fct_edits",
        "SELECT 'a\\' ; DROP TABLE marts.fct_edits; --'",
        "SELECT $$x$$; DROP TABLE marts.fct_edits",
    ],
)
def test_statements_after_comments_and_strings_are_seen(guard: Guard, sql: str) -> None:
    assert {"multiple_statements", "write_or_ddl"} <= set(_blocked(guard, sql).codes)


@pytest.mark.parametrize(
    ("sql", "literal"),
    [
        ("SELECT $q$ ; DROP TABLE x; $q$", "' ; DROP TABLE x; '"),
        ("SELECT 'it''s; DROP TABLE x'", "'it''s; DROP TABLE x'"),
        ("SELECT E'a\\' ; DROP TABLE x; --'", "e'a'' ; DROP TABLE x; --'"),
    ],
)
def test_strings_stay_strings(guard: Guard, sql: str, literal: str) -> None:
    result = guard.check(sql)
    assert result.allowed
    assert result.sql == f"SELECT {literal} LIMIT 1000"


# -- relation qualification and CTE scoping --------------------------------------------


def test_unqualified_relation_is_pinned_to_its_schema(guard: Guard) -> None:
    result = guard.check(
        "SELECT f.wiki, d.domain FROM fct_edits f JOIN dim_wiki d ON d.wiki = f.wiki"
    )
    assert result.allowed
    assert result.sql == (
        "SELECT f.wiki, d.domain FROM marts.fct_edits AS f "
        "JOIN marts.dim_wiki AS d ON d.wiki = f.wiki LIMIT 1000"
    )
    assert result.tables == ("marts.dim_wiki", "marts.fct_edits")


def test_unqualified_relation_in_subquery_is_pinned(guard: Guard) -> None:
    result = guard.check("SELECT wiki FROM dim_wiki WHERE wiki IN (SELECT wiki FROM fct_edits)")
    assert result.allowed
    assert "FROM marts.dim_wiki" in result.sql
    assert "FROM marts.fct_edits" in result.sql


def test_cte_names_are_not_relations(guard: Guard) -> None:
    result = guard.check(
        "WITH top AS (SELECT wiki, edits FROM marts.dim_wiki ORDER BY edits DESC LIMIT 5), "
        "names AS (SELECT wiki FROM top) SELECT * FROM names"
    )
    assert result.allowed
    assert result.tables == ("marts.dim_wiki",)
    assert "FROM marts.top" not in result.sql


def test_cte_may_shadow_a_blocked_name(guard: Guard) -> None:
    result = guard.check("WITH pg_shadow AS (SELECT 1 AS x) SELECT x FROM pg_shadow")
    assert result.allowed
    assert result.tables == ()


def test_cte_named_like_a_mart_does_not_launder_raw(guard: Guard) -> None:
    result = _blocked(
        guard, "WITH fct_edits AS (SELECT * FROM raw.recentchange) SELECT * FROM fct_edits"
    )
    assert result.codes == ("relation_not_allowed",)


def test_cte_shadowing_a_mart_reads_the_cte(guard: Guard) -> None:
    result = guard.check("WITH fct_edits AS (SELECT 1 AS x) SELECT x FROM fct_edits")
    assert result.allowed
    assert result.tables == ()
    assert "marts.fct_edits" not in result.sql


def test_qualified_reference_is_never_a_cte(guard: Guard) -> None:
    result = _blocked(guard, "WITH recentchange AS (SELECT 1) SELECT * FROM raw.recentchange")
    assert result.codes == ("relation_not_allowed",)


def test_cte_in_a_subquery_does_not_shadow_outside(guard: Guard) -> None:
    sql = (
        "SELECT * FROM (WITH recentchange AS (SELECT 1 AS x) SELECT x FROM recentchange) s, "
        "recentchange"
    )
    assert _blocked(guard, sql).codes == ("relation_not_allowed",)


def test_cte_is_not_visible_to_earlier_ctes(guard: Guard) -> None:
    sql = "WITH b AS (SELECT * FROM a), a AS (SELECT 1 AS x) SELECT * FROM b"
    assert _blocked(guard, sql).codes == ("relation_not_allowed",)


def test_cte_visible_in_every_branch_of_a_set_operation(guard: Guard) -> None:
    result = guard.check(
        "WITH w AS (SELECT wiki FROM marts.dim_wiki) SELECT wiki FROM w UNION SELECT wiki FROM w"
    )
    assert result.allowed
    assert result.tables == ("marts.dim_wiki",)


# -- the row limit ---------------------------------------------------------------------


@pytest.mark.parametrize(
    ("sql", "expected", "limit"),
    [
        ("SELECT 1", "SELECT 1 LIMIT 1000", 1000),
        ("SELECT 1 LIMIT 10", "SELECT 1 LIMIT 10", 10),
        ("SELECT 1 LIMIT 0", "SELECT 1 LIMIT 0", 0),
        ("SELECT 1 LIMIT 1000", "SELECT 1 LIMIT 1000", 1000),
        ("SELECT 1 LIMIT 5000", "SELECT 1 LIMIT 1000", 1000),
        ("SELECT 1 LIMIT ALL", "SELECT 1 LIMIT 1000", 1000),
        ("SELECT 1 LIMIT NULL", "SELECT 1 LIMIT 1000", 1000),
        ("SELECT 1 LIMIT 2 + 3", "SELECT 1 LIMIT 1000", 1000),
        ("SELECT 1 LIMIT -1", "SELECT 1 LIMIT 1000", 1000),
        ("SELECT 1 LIMIT '5'", "SELECT 1 LIMIT 1000", 1000),
        ("SELECT 1 LIMIT 5 OFFSET 20", "SELECT 1 LIMIT 5 OFFSET 20", 5),
        ("SELECT 1 OFFSET 20", "SELECT 1 LIMIT 1000 OFFSET 20", 1000),
        ("SELECT 1 FETCH FIRST 5 ROWS ONLY", "SELECT 1 LIMIT 5", 5),
        ("SELECT 1 FETCH FIRST ROW ONLY", "SELECT 1 LIMIT 1", 1),
        ("SELECT 1 FETCH NEXT 5000 ROWS ONLY", "SELECT 1 LIMIT 1000", 1000),
        (
            "SELECT x FROM marts.dim_wiki ORDER BY x FETCH FIRST 3 ROWS WITH TIES",
            "SELECT * FROM (SELECT x FROM marts.dim_wiki ORDER BY x "
            "FETCH FIRST 3 ROWS WITH TIES) AS q LIMIT 1000",
            1000,
        ),
        ("SELECT 1 UNION SELECT 2", "SELECT 1 UNION SELECT 2 LIMIT 1000", 1000),
        ("SELECT 1 UNION ALL SELECT 2 LIMIT 7", "SELECT 1 UNION ALL SELECT 2 LIMIT 7", 7),
        ("SELECT 1 INTERSECT SELECT 1 LIMIT 9999", "SELECT 1 INTERSECT SELECT 1 LIMIT 1000", 1000),
        ("SELECT 1 EXCEPT SELECT 2", "SELECT 1 EXCEPT SELECT 2 LIMIT 1000", 1000),
        (
            "(SELECT 1 LIMIT 3) UNION (SELECT 2 LIMIT 3)",
            "(SELECT 1 LIMIT 3) UNION (SELECT 2 LIMIT 3) LIMIT 1000",
            1000,
        ),
        ("(SELECT 1)", "(SELECT 1) LIMIT 1000", 1000),
        (
            "WITH a AS (SELECT 1 AS x) SELECT x FROM a",
            "WITH a AS (SELECT 1 AS x) SELECT x FROM a LIMIT 1000",
            1000,
        ),
        ("SELECT (SELECT 1 LIMIT 50000)", "SELECT (SELECT 1 LIMIT 50000) LIMIT 1000", 1000),
    ],
)
def test_limit_rewrite(guard: Guard, sql: str, expected: str, limit: int) -> None:
    result = guard.check(sql)
    assert result.allowed, result.reasons
    assert result.sql == expected
    assert result.limit == limit


def test_limit_follows_max_rows() -> None:
    small = Guard(MARTS, max_rows=10)
    assert small.check("SELECT 1 LIMIT 50").sql == "SELECT 1 LIMIT 10"
    assert small.check("SELECT 1").limit == 10


# -- benign analytics ------------------------------------------------------------------


BENIGN = [
    "SELECT date_trunc('hour', event_ts) AS h, count(*) FROM marts.fct_edits GROUP BY 1 ORDER BY 1",
    "SELECT extract(hour FROM event_ts) AS h, count(*) FROM fct_edits GROUP BY 1",
    "SELECT date_part('dow', event_ts), count(*) FROM marts.fct_edits GROUP BY 1",
    "SELECT wiki, count(*) FILTER (WHERE is_bot) AS bots, count(*) AS n "
    "FROM marts.fct_edits GROUP BY wiki HAVING count(*) > 10",
    "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY byte_delta) FROM marts.fct_edits",
    "SELECT percentile_disc(0.9) WITHIN GROUP (ORDER BY byte_delta) FROM marts.fct_edits",
    "SELECT mode() WITHIN GROUP (ORDER BY wiki) FROM marts.fct_edits",
    "SELECT wiki, edits, rank() OVER (ORDER BY edits DESC), "
    "row_number() OVER w, dense_rank() OVER w, ntile(4) OVER w, percent_rank() OVER w, "
    "cume_dist() OVER w, lag(edits) OVER w, lead(edits, 2) OVER w, "
    "first_value(wiki) OVER w, last_value(wiki) OVER w, nth_value(wiki, 2) OVER w "
    "FROM marts.dim_wiki WINDOW w AS (ORDER BY edits)",
    "SELECT wiki, sum(edits) OVER (PARTITION BY project_family ORDER BY edits "
    "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM marts.dim_wiki",
    "SELECT event_ts::date, byte_delta::numeric, wiki::text FROM marts.fct_edits",
    "SELECT round(avg(byte_delta)::numeric, 2), stddev(byte_delta), variance(byte_delta), "
    "stddev_samp(byte_delta), corr(length_old, length_new) FROM marts.fct_edits",
    "SELECT count(*) FROM marts.fct_edits WHERE event_ts >= TIMESTAMPTZ '2026-10-17 00:00+00' "
    "- INTERVAL '1 hour'",
    "SELECT now() - interval '1 day', current_date, age(now(), timestamp '2026-01-01')",
    "SELECT CASE WHEN is_bot THEN 'bot' WHEN is_anonymous THEN 'anon' ELSE 'user' END AS kind, "
    "count(*) FROM marts.fct_edits GROUP BY 1",
    "SELECT coalesce(language_code, 'n/a'), nullif(edits, 0), greatest(edits, 1), least(edits, 1) "
    "FROM marts.dim_wiki",
    "SELECT project_family, string_agg(wiki, ', ' ORDER BY wiki), array_agg(wiki) "
    "FROM marts.dim_wiki GROUP BY project_family",
    "SELECT w.wiki, w.domain, h.event_hour, h.edits FROM marts.dim_wiki AS w "
    "JOIN marts.agg_edits_hourly AS h ON h.wiki = w.wiki "
    "LEFT JOIN marts.dim_page AS p ON p.wiki = w.wiki",
    "SELECT wiki FROM marts.dim_wiki WHERE EXISTS "
    "(SELECT 1 FROM marts.fct_edits f WHERE f.wiki = dim_wiki.wiki AND f.is_new_page)",
    "SELECT wiki, (SELECT max(edits) FROM marts.agg_edits_hourly h WHERE h.wiki = d.wiki) "
    "FROM marts.dim_wiki d",
    "SELECT d.wiki, top.title FROM marts.dim_wiki d CROSS JOIN LATERAL "
    "(SELECT title FROM marts.dim_page p WHERE p.wiki = d.wiki ORDER BY edits DESC LIMIT 1) top",
    "SELECT ARRAY(SELECT wiki FROM marts.dim_wiki ORDER BY edits DESC LIMIT 3)",
    "SELECT split_part(domain, '.', 1), initcap(project_family), upper(wiki), lower(wiki), "
    "length(wiki), char_length(wiki), left(wiki, 2), right(wiki, 4), lpad(wiki, 10, '.'), "
    "replace(wiki, 'wiki', ''), reverse(wiki), md5(wiki), trim(wiki), btrim(wiki), "
    "substr(wiki, 1, 3), substring(wiki FROM 1 FOR 3), position('wiki' IN wiki), "
    "strpos(wiki, 'wiki'), concat(wiki, '!'), concat_ws('-', wiki, domain), "
    "regexp_replace(wiki, 'wiki$', '') FROM marts.dim_wiki",
    "SELECT width_bucket(byte_delta, -1000, 1000, 10), abs(byte_delta), sign(byte_delta), "
    "ceil(byte_delta / 3.0), floor(byte_delta / 3.0), trunc(byte_delta / 3.0), "
    "sqrt(abs(byte_delta)), "
    "power(2, 3), ln(2), log(100), mod(byte_delta, 7), div(byte_delta, 7) FROM marts.fct_edits",
    "SELECT to_char(event_hour, 'YYYY-MM-DD HH24'), to_timestamp(0), "
    "make_interval(hours => 2), justify_interval(interval '36 hours') FROM marts.agg_edits_hourly",
    "SELECT bool_and(is_bot), bool_or(is_minor), every(is_patrolled) FROM marts.fct_edits",
    "SELECT h FROM generate_series(TIMESTAMPTZ '2026-10-16 23:00+00', "
    "TIMESTAMPTZ '2026-10-17 00:00+00', INTERVAL '1 hour') AS g(h)",
    "SELECT wiki, sum(edits) FROM marts.agg_editor_activity GROUP BY ROLLUP (wiki)",
    "SELECT DISTINCT ON (wiki) wiki, user_name, edits FROM marts.agg_editor_activity "
    "ORDER BY wiki, edits DESC",
    "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS v(n, s)",
    "SELECT title FROM marts.dim_page WHERE title ILIKE '%wiki%' AND title ~ '^[A-Z]'",
    "SELECT count(DISTINCT user_name) FROM marts.agg_editor_activity WHERE NOT is_bot",
    "SELECT wiki FROM marts.dim_wiki WHERE wiki = ANY (ARRAY['enwiki', 'dewiki'])",
    "SELECT event_ts AT TIME ZONE 'UTC' FROM marts.fct_edits",
    "SELECT jsonb_build_object('wiki', wiki, 'edits', edits) FROM marts.dim_wiki",
    "SELECT random() < 0.5",
    "SELECT 'pg_sleep(1)' AS not_a_call, 'DROP TABLE x' AS text_only",
    "SELECT 1 AS pg_x FROM marts.dim_wiki AS pg_alias",
]


@pytest.mark.parametrize("sql", BENIGN)
def test_benign_analytics_are_allowed(guard: Guard, sql: str) -> None:
    result = guard.check(sql)
    assert result.allowed, result.reasons
    assert result.reasons == ()
    assert result.limit is not None
    # Executable-looking: valid PostgreSQL by sqlglot, one statement, stable on recheck.
    assert len(sqlglot.parse(result.sql, dialect="postgres")) == 1
    assert guard.check(result.sql) == result
    assert all(t in MARTS for t in result.tables)


def test_tables_tuple_is_sorted_and_deduplicated(guard: Guard) -> None:
    result = guard.check(
        "SELECT * FROM marts.fct_edits f JOIN dim_wiki d USING (wiki) "
        "JOIN marts.fct_edits g USING (wiki) "
        "WHERE wiki IN (SELECT wiki FROM marts.agg_edits_hourly)"
    )
    assert result.allowed
    assert result.tables == ("marts.agg_edits_hourly", "marts.dim_wiki", "marts.fct_edits")


def test_anonymous_allowlist_is_really_anonymous() -> None:
    """Every allowlisted name must be one sqlglot leaves untyped, or the entry is dead."""
    from sqlglot import exp

    for name in ANONYMOUS_ALLOWED:
        call = None
        for arity in range(4):
            try:
                tree = sqlglot.parse_one(
                    f"SELECT {name}({', '.join(['x'] * arity)})", dialect="postgres"
                )
            except sqlglot.errors.ParseError:
                continue
            call = tree.expressions[0]
            break
        assert isinstance(call, exp.Anonymous), name


# -- field notation and keyword functions ------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        # PostgreSQL reads (x).f as f(x): the denied function never looks like a call
        "SELECT (10).pg_sleep",
        "SELECT ((NULL::int).pg_stat_get_activity).*",
        "SELECT (pid).pg_terminate_backend FROM (SELECT ((NULL::int).pg_stat_get_activity).*) s",
        "SELECT ('statement_timeout').current_setting",
        "SELECT ('/etc/hostname').pg_read_file",
        # harmless-looking field access: no mart has a composite column, so fail closed
        "SELECT (w).wiki FROM marts.dim_wiki w",
        # a name with too many parts is not a relation the allowlist checked
        "SELECT * FROM tributary.marts.fct_edits.wiki",
    ],
)
def test_field_notation_is_blocked(guard: Guard, sql: str) -> None:
    assert "field_notation" in _blocked(guard, sql).codes


@pytest.mark.parametrize("sql", ["SELECT user", "SELECT current_role", "SELECT system_user"])
def test_keyword_identity_functions_are_denied(guard: Guard, sql: str) -> None:
    """The SQL-standard niladic functions parse as columns, not calls."""
    result = _blocked(guard, sql)
    assert result.codes == ("function_not_allowed",)


def test_quoted_or_qualified_user_is_a_column(guard: Guard) -> None:
    assert guard.check('SELECT w."user" FROM marts.dim_wiki w').allowed
    assert guard.check('SELECT "user" FROM marts.dim_wiki').allowed
    assert guard.check("SELECT current_date, localtime FROM marts.dim_wiki").allowed


# -- calls in column notation on a function in FROM -------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        # p.f is f(p), and the whole row of a scalar function is the scalar
        "SELECT count(*) FILTER (WHERE p.pg_terminate_backend) FROM generate_series(1, 99999) AS p",
        "SELECT t.pg_sleep FROM generate_series(1, 10) AS t",
        "SELECT s.current_setting FROM unnest(ARRAY['data_directory']) AS s",
        "SELECT n.pg_advisory_lock FROM generate_series(42, 42) AS n",
        # unaliased: the item is named after the function
        "SELECT generate_series.pg_sleep FROM generate_series(1, 1)",
        "SELECT unnest.pg_sleep FROM unnest(ARRAY[1])",
        # a column list does not stop it, nor does LATERAL, a join or ROWS FROM
        "SELECT t.pg_sleep FROM generate_series(1, 1) AS t(c)",
        "SELECT t.pg_sleep FROM marts.dim_wiki, LATERAL generate_series(1, 1) AS t",
        "SELECT u.pg_sleep FROM marts.dim_wiki JOIN unnest(ARRAY[1]) AS u ON true",
        "SELECT t.pg_sleep FROM ROWS FROM (generate_series(1, 1)) AS t",
        # and a name the denylist does not know is not a column either
        "SELECT t.upper FROM unnest(ARRAY['a']) AS t",
        # an alias reused in another scope with a column of the call's name does not
        # vouch for it where the alias has no such column
        "SELECT p.pg_sleep FROM unnest(ARRAY[0.5]) AS p(x)"
        " WHERE EXISTS (SELECT 1 FROM generate_series(1, 1) AS p(pg_sleep))",
        "SELECT p.pg_sleep FROM unnest(ARRAY[0.5]) AS p(x)"
        " CROSS JOIN LATERAL (SELECT 1 FROM generate_series(1, 1) AS p(pg_sleep)) AS z",
    ],
)
def test_call_in_column_notation_on_a_function_is_blocked(guard: Guard, sql: str) -> None:
    assert "field_notation" in _blocked(guard, sql).codes


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT t.c FROM generate_series(1, 3) AS t(c)",
        "SELECT t.t, t.* FROM generate_series(1, 3) AS t",
        "SELECT generate_series.generate_series FROM generate_series(1, 2)",
        "SELECT j.key, j.value FROM jsonb_each('{}') AS j",
        "SELECT t.a, t.ordinality FROM unnest(ARRAY[1]) WITH ORDINALITY AS t(a)",
        "SELECT d.wiki, g.g FROM marts.dim_wiki AS d, generate_series(1, 2) AS g",
    ],
)
def test_columns_of_a_function_in_from_are_allowed(guard: Guard, sql: str) -> None:
    assert guard.check(sql).allowed


# -- parser differentials and the guard's own cost ----------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT wiki IS DOCUMENT FROM marts.dim_wiki",
        "SELECT '<a/>' IS NOT DOCUMENT",
        "SELECT 'x' IS NORMALIZED",
    ],
)
def test_is_with_an_operand_sqlglot_reads_as_a_column_is_blocked(guard: Guard, sql: str) -> None:
    """sqlglot reads IS DOCUMENT as a comparison with a column named document."""
    assert "parse_error" in _blocked(guard, sql).codes


def test_is_null_true_and_json_are_allowed(guard: Guard) -> None:
    sql = "SELECT wiki IS NULL, wiki IS NOT TRUE, '{}' IS JSON OBJECT FROM marts.dim_wiki"
    assert guard.check(sql).allowed


def test_xmltable_is_denied_like_the_xml_functions(guard: Guard) -> None:
    sql = "SELECT * FROM XMLTABLE('/a' PASSING '<a>1</a>' COLUMNS x int PATH '.')"
    assert "function_not_allowed" in _blocked(guard, sql).codes


@pytest.mark.parametrize("sql", ["SELECT 1 OPERATOR(public.+) 1", "SELECT 1 OPERATOR(+) 1"])
def test_operator_syntax_is_blocked(guard: Guard, sql: str) -> None:
    assert "qualified_function" in _blocked(guard, sql).codes


def test_long_subscript_chains_are_refused_before_parsing(guard: Guard) -> None:
    """sqlglot's subscript parse is quadratic: 300 of them took about a second."""
    result = _blocked(guard, "SELECT x" + "[1]" * 300 + " FROM marts.dim_wiki")
    assert result.codes == ("too_complex",)
    assert guard.check("SELECT (ARRAY[1, 2, 3])[2], ARRAY[[1]][1][1]").allowed
