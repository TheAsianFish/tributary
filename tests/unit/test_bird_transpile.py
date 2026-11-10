"""The SQLite to PostgreSQL transpiler of the corrected-label track."""

from __future__ import annotations

from decimal import Decimal

import pytest

from eval.bird.corrected import TranspileError, column_types, same_rows_approx, transpile
from tributary.agent.catalog import Catalog, Column, Table
from tributary.agent.execute import QueryResult


def test_plain_identifiers_are_lowercased_and_unquoted() -> None:
    out = transpile("SELECT T2.`CDSCode` FROM schools AS T2")

    assert "t2.cdscode" in out
    assert '"' not in out
    assert "`" not in out


def test_names_that_need_quoting_keep_their_spelling() -> None:
    out = transpile("SELECT `Free Meal Count (K-12)` FROM frpm")

    assert '"Free Meal Count (K-12)"' in out


def test_real_casts_become_double_precision() -> None:
    out = transpile("SELECT CAST(a AS REAL) / b FROM t")

    assert "DOUBLE PRECISION" in out
    assert "REAL" not in out.replace("DOUBLE PRECISION", "")


def test_like_becomes_ilike() -> None:
    assert "ILIKE" in transpile("SELECT a FROM t WHERE name LIKE 'A%'")


def test_a_number_compared_with_a_text_column_becomes_a_string() -> None:
    out = transpile("SELECT a FROM t WHERE a2 = 1", {"a2": "text"})

    assert "a2 = '1'" in out


def test_the_number_may_be_on_the_left_and_negative() -> None:
    assert "'1' = a2" in transpile("SELECT a FROM t WHERE 1 = a2", {"a2": "text"})
    assert "a2 = '-5'" in transpile("SELECT a FROM t WHERE a2 = -5", {"a2": "text"})


def test_in_lists_on_text_columns_become_strings() -> None:
    out = transpile("SELECT a FROM t WHERE a2 IN (1, 2, 'x')", {"a2": "text"})

    assert "a2 IN ('1', '2', 'x')" in out


def test_integer_columns_are_left_alone() -> None:
    types = {"a2": "bigint"}

    assert "a2 = 1" in transpile("SELECT a FROM t WHERE a2 = 1", types)
    assert "a2 IN (1, 2)" in transpile("SELECT a FROM t WHERE a2 IN (1, 2)", types)


def test_without_types_the_comparison_is_untouched() -> None:
    assert "a2 = 1" in transpile("SELECT a FROM t WHERE a2 = 1")


def test_a_comparison_inside_sum_is_cast_to_integer() -> None:
    out = transpile("SELECT SUM(gender = 'M') FROM t")

    assert out == "SELECT SUM(CAST(gender = 'M' AS INT)) FROM t"


def test_avg_of_a_comparison_is_cast_too() -> None:
    assert "AS INT)" in transpile("SELECT AVG(a > 3) FROM t")


def test_sum_of_a_plain_column_is_not_cast() -> None:
    assert "CAST" not in transpile("SELECT SUM(a) FROM t")


def test_iif_becomes_case() -> None:
    out = transpile("SELECT IIF(a > 1, 'x', 'y') FROM t")

    assert "CASE WHEN" in out
    assert "IIF" not in out


def test_two_statements_raise() -> None:
    with pytest.raises(TranspileError, match="one statement"):
        transpile("SELECT 1; SELECT 2")


def test_garbage_raises() -> None:
    with pytest.raises(TranspileError):
        transpile("SELECT FROM WHERE ((")


# --- column_types and same_rows_approx ----------------------------------------------------------


def test_column_types_keeps_only_names_with_one_type_across_the_database() -> None:
    def tbl(name: str, *columns: Column) -> Table:
        return Table("db", name, "", columns, (), (), ())

    catalog = Catalog(
        (
            tbl("a", Column("A2", "text", ""), Column("n", "bigint", ""), Column("x", "text", "")),
            tbl("b", Column("a2", "TEXT", ""), Column("n", "text", "")),
        ),
        "information_schema",
    )

    assert column_types(catalog) == {"a2": "text", "x": "text"}


def test_same_rows_approx_compares_six_significant_digits() -> None:
    assert same_rows_approx([(1.0000001,)], [(1.0,)])
    assert same_rows_approx([(Decimal("0.3333333"),)], [(0.333333333,)])
    assert not same_rows_approx([(1.00001,)], [(1.0,)])


def test_same_rows_approx_is_a_set_comparison() -> None:
    assert same_rows_approx([(1,), (2,), (2,)], [(2,), (1,)])
    assert not same_rows_approx([(1,)], [(1,), (2,)])


def test_same_rows_approx_accepts_a_query_result_and_leaves_text_exact() -> None:
    result = QueryResult(("a", "b"), ((Decimal("2.50"), "x"),), False, 1.0)

    assert same_rows_approx(result, [(2.5, "x")])
    assert not same_rows_approx(result, [(2.5, "X")])


def test_same_rows_approx_treats_nan_as_equal_to_nan() -> None:
    assert same_rows_approx([(float("nan"),)], [(float("nan"),)])


def test_not_like_keeps_its_negation() -> None:
    out = transpile("SELECT 1 FROM t WHERE x NOT LIKE 'z' AND NOT (y LIKE 'w')")
    assert "x NOT ILIKE 'z'" in out
    assert "NOT (y ILIKE 'w')" in out


def test_like_nested_inside_like_is_rewritten_with_its_identifiers() -> None:
    out = transpile(
        "SELECT 1 FROM t AS T1 WHERE T1.`District` LIKE '%' || "
        "(SELECT c FROM u AS U WHERE U.D LIKE 'y')"
    )
    assert "t1.district ILIKE" in out
    assert "u.d ILIKE 'y'" in out
    assert " LIKE " not in out


def test_reserved_words_stay_quoted() -> None:
    assert 'FROM "order"' in transpile("SELECT COUNT(*) FROM `order`")
    assert '"user"' in transpile("SELECT `User` FROM t")


def test_numbers_become_text_the_way_sqlite_renders_them() -> None:
    types = {"a2": "text"}
    out = transpile("SELECT a FROM t WHERE a2 = 05 OR a2 = 1e3 OR a2 = -1.50", types)
    assert "a2 = '5'" in out
    assert "a2 = '1000.0'" in out
    assert "a2 = '-1.5'" in out
