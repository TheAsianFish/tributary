"""The warehouse evaluator's comparison, offline."""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

from eval.warehouse_eval import (
    _display,
    limits_of,
    report_name,
    result_key,
    same_result,
    summarise,
    variant,
)
from tributary.config import REPO_ROOT, Settings


def test_numbers_compare_by_value_across_types() -> None:
    assert same_result([(Decimal("10168"),)], [(10168,)])
    assert same_result([(Decimal("45.1"),)], [(45.1,)])
    assert same_result([(0.1 + 0.2,)], [(Decimal("0.3"),)])


def test_a_boolean_does_not_match_a_count() -> None:
    assert not same_result([(True,)], [(1,)])
    assert same_result([(True,)], [(True,)])


def test_rows_are_a_multiset_and_columns_ordered() -> None:
    assert same_result([(1, "a"), (2, "b")], [(2, "b"), (1, "a")])
    assert not same_result([(1, "a")], [(1, "a"), (1, "a")])
    assert not same_result([("a", 1)], [(1, "a")])


def test_json_values_are_hashable() -> None:
    assert result_key([({"b": 1, "a": 2},)]) == result_key([({"a": 2, "b": 1},)])


def test_display_outside_the_repo() -> None:
    assert _display(REPO_ROOT / "eval" / "x.json") == "eval/x.json"
    assert _display(REPO_ROOT.parent / "elsewhere.json").endswith("elsewhere.json")


def test_summary_of_nothing() -> None:
    summary = summarise([])
    assert summary["correct"] == 0
    assert summary["unpriced_calls"] == 0


def test_a_run_with_planner_features_gets_its_own_report() -> None:
    # The second pass must not overwrite the plain pass's report.
    provider = SimpleNamespace(model="qwen2.5-coder:7b")
    plain = Settings(provider="ollama")
    second = Settings(provider="ollama", agent_values=True, agent_examples=3)
    assert variant(plain) == ""
    assert variant(second) == "vf"
    assert report_name(plain, provider) == "warehouse-ollama-qwen2.5-coder-7b.json"
    assert report_name(second, provider) == "warehouse-ollama-qwen2.5-coder-7b-vf.json"
    assert limits_of(second)["examples"] == 3
