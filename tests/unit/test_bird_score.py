"""BIRD execution accuracy, the strict rule and the check against the official scorer."""

from __future__ import annotations

import hashlib
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from eval.bird import score
from eval.bird.score import (
    Rows,
    ScorerUnavailable,
    cross_check,
    execution_match,
    lenient_match,
    official_calculate_ex,
    rows_digest,
    strict_match,
)

OFFICIAL_SOURCE = """\
import func_timeout  # not installed: executing the module top level would fail


def calculate_ex(predicted_res, ground_truth_res):
    res = 0
    if set(predicted_res) == set(ground_truth_res):
        res = 1
    return res


def other():
    raise RuntimeError("never compiled")
"""


def test_execution_match_ignores_row_order_and_duplicates() -> None:
    assert execution_match([(1,), (2,)], [(2,), (1,)])
    assert execution_match([(1,), (1,), (2,)], [(1,), (2,)])


def test_execution_match_respects_column_order() -> None:
    assert not execution_match([(1, 2)], [(2, 1)])


def test_execution_match_detects_different_rows() -> None:
    assert not execution_match([(1,)], [(1,), (2,)])
    assert not execution_match([], [(1,)])
    assert execution_match([], [])


def test_execution_match_compares_values_as_python_does_like_bird() -> None:
    # BIRD's calculate_ex is set(predicted) == set(gold) on the driver's values.
    assert not execution_match([(Decimal("0.1"),)], [(0.1,)])
    assert not execution_match([(Decimal("61.5714285714285714"),)], [(61.57142857142857,)])
    assert execution_match([(Decimal("2"),)], [(2,)])
    assert execution_match([(Decimal("2.5"),)], [(2.5,)])  # exactly representable
    assert not execution_match([(Decimal("2.5"),)], [(2,)])


def test_lenient_match_compares_numbers_by_value_whatever_their_type() -> None:
    assert lenient_match([(Decimal("0.1"),)], [(0.1,)])
    assert lenient_match([(Decimal("61.5714285714285714"),)], [(61.57142857142857,)])
    assert lenient_match([(Decimal("2.00"),)], [(2,)])
    assert not lenient_match([(Decimal("2.5"),)], [(2,)])


def test_execution_match_treats_lists_as_values() -> None:
    assert execution_match([([1, 2],)], [((1, 2),)])


def test_strict_match_counts_duplicates() -> None:
    assert strict_match([(1,), (2,)], [(2,), (1,)])
    assert not strict_match([(1,), (1,), (2,)], [(1,), (2,)])


def test_strict_match_distinguishes_true_from_one() -> None:
    assert not strict_match([(True,)], [(1,)])
    assert strict_match([(True,)], [(True,)])
    assert execution_match([(True,)], [(1,)])  # Python equality, as BIRD's scorer


def test_strict_match_rounds_floats_to_six_decimals() -> None:
    assert strict_match([(0.1234567,)], [(0.1234568,)])
    assert not strict_match([(0.123,)], [(0.124,)])
    assert strict_match([(float("nan"),)], [(float("nan"),)])


def test_rows_digest_is_order_independent_and_content_sensitive() -> None:
    assert rows_digest([(1, "a"), (2, "b")]) == rows_digest([(2, "b"), (1, "a")])
    assert rows_digest([(1, "a")]) != rows_digest([(1, "b")])
    assert rows_digest([(1,), (1,)]) == rows_digest([(1,)])  # a set, like EX
    assert len(rows_digest([])) == 16


def test_cross_check_agrees_on_ordinary_pairs() -> None:
    def official(p: Any, g: Any) -> int:
        return int(set(p) == set(g))

    pairs: list[tuple[str, Rows, Rows]] = [
        ("same", [(1,), (2,)], [(2,), (1,)]),
        ("different", [(1,)], [(2,)]),
        ("empty", [], []),
    ]

    report = cross_check(pairs, official)

    assert (report.pairs, report.agree) == (3, 3)
    assert report.disagreements == ()


def test_cross_check_agrees_with_the_official_rule_on_typed_values() -> None:
    def official(p: Any, g: Any) -> int:
        return int(set(p) == set(g))

    pairs: list[tuple[str, Rows, Rows]] = [
        ("fine", [(1,)], [(1,)]),
        ("decimal", [(Decimal("0.1"),)], [(0.1,)]),
        ("array", [([1, 2],)], [([1, 2],)]),
    ]

    report = cross_check(pairs, official)

    assert (report.pairs, report.agree) == (3, 3)


def test_cross_check_reports_a_scorer_that_differs() -> None:
    def lenient_official(p: Any, g: Any) -> int:
        return int(lenient_match(p, g))

    pairs: list[tuple[str, Rows, Rows]] = [("decimal", [(Decimal("0.1"),)], [(0.1,)])]

    report = cross_check(pairs, lenient_official)

    (only,) = report.disagreements
    assert (only["item"], only["official"], only["ours"]) == ("decimal", True, False)


# --- the official scorer ------------------------------------------------------------------------


def cached_scorer(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, text: str) -> Path:
    cache = tmp_path / "cache"
    cache.mkdir()
    (cache / "evaluation_ex.py").write_text(text, encoding="utf-8")
    sha = hashlib.sha256(text.encode()).hexdigest()
    monkeypatch.setattr(score, "OFFICIAL_SCORER_SHA256", sha)
    return cache


def test_official_calculate_ex_compiles_only_that_function(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cache = cached_scorer(tmp_path, monkeypatch, OFFICIAL_SOURCE)

    calculate_ex = official_calculate_ex(cache, download=False)

    assert calculate_ex([(1,), (2,)], [(2,), (1,)]) == 1
    assert calculate_ex([(1,)], [(2,)]) == 0


def test_official_calculate_ex_rejects_a_file_with_the_wrong_sha(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cache = cached_scorer(tmp_path, monkeypatch, OFFICIAL_SOURCE)
    (cache / "evaluation_ex.py").write_text(OFFICIAL_SOURCE + "# edited\n", encoding="utf-8")

    with pytest.raises(ScorerUnavailable, match="sha256"):
        official_calculate_ex(cache, download=False)


def test_official_calculate_ex_needs_exactly_one_calculate_ex(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cache = cached_scorer(tmp_path, monkeypatch, "x = 1\n")

    with pytest.raises(ScorerUnavailable, match="calculate_ex"):
        official_calculate_ex(cache, download=False)


def test_official_calculate_ex_without_a_file_and_no_download(tmp_path: Path) -> None:
    with pytest.raises(ScorerUnavailable, match="not cached"):
        official_calculate_ex(tmp_path, download=False)
