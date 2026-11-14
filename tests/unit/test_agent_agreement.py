"""``same_rows`` is BIRD's set rule, the same verdicts as ``eval.bird.score.execution_match``."""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal

import pytest

from eval.bird.score import execution_match
from tributary.agent.agreement import same_rows

CASES: list[tuple[Sequence[Sequence[object]], Sequence[Sequence[object]], bool]] = [
    ([(1, "a"), (2, "b")], [(2, "b"), (1, "a")], True),  # row order ignored
    ([(1,), (1,), (2,)], [(2,), (1,)], True),  # duplicates ignored
    # numeric vs float8: different unless the float is exactly the decimal's value
    ([(Decimal("61.5714285714285714"),)], [(61.57142857142857,)], False),
    ([(Decimal("1.5"),)], [(1.5,)], True),
    ([(1, "a")], [("a", 1)], False),  # column order counts
    ([([1, 2],)], [((1, 2),)], True),  # arrays as tuples
    ([([1, [2, 3]],)], [([1, [2, 4]],)], False),  # nested arrays compared
    ([({"b": 1, "a": 2},)], [({"a": 2, "b": 1},)], True),  # JSON key order ignored
    ([({"a": 1},)], [({"a": 1.0},)], False),  # JSON values by repr
    ([], [], True),
    ([(None,)], [], False),
]


@pytest.mark.parametrize(("a", "b", "expected"), CASES)
def test_same_rows_matches_execution_match(
    a: Sequence[Sequence[object]], b: Sequence[Sequence[object]], expected: bool
) -> None:
    assert same_rows(a, b) is expected
    assert same_rows(b, a) is expected
    assert execution_match(a, b) is expected
