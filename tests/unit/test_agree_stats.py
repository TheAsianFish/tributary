"""Fisher's exact test, the M13 rule and its simulation."""

from __future__ import annotations

import math

import pytest

from eval.agree_stats import Table, fisher_greater, needed, rates_from, simulate, verdict


def test_fisher_matches_a_hand_computed_tail() -> None:
    # 4 drawn from 8 with 4 right; P(X >= 3) = (C(4,3)C(4,1) + C(4,4)C(4,0)) / C(8,4) = 17/70.
    assert fisher_greater(3, 4, 4, 8) == pytest.approx(17 / 70)
    assert fisher_greater(0, 4, 4, 8) == pytest.approx(1.0)


def test_fisher_tail_sums_to_one_with_its_complement() -> None:
    for a in range(0, 15):
        upper = fisher_greater(a, 14, 25, 40)
        lower = 1 - fisher_greater(a + 1, 14, 25, 40) if a < 14 else 1.0
        assert upper + lower - 1 == pytest.approx(
            math.comb(25, a) * math.comb(15, 14 - a) / math.comb(40, 14)
        )


def test_fisher_refuses_an_impossible_table() -> None:
    with pytest.raises(ValueError, match="not a 2x2 table"):
        fisher_greater(5, 4, 4, 8)


def test_the_thresholds_stated_in_plan_md() -> None:
    assert [needed(a, 25, 40) for a in (14, 16, 20, 24)] == [12, 13, 16, 18]


def test_the_rule_reads_three_ways() -> None:
    assert verdict(Table(13, 1, 12, 14)) == "transfers"
    assert verdict(Table(10, 4, 15, 11)) == "not seen"  # 71% vs 58%: a 13-point gap
    assert verdict(Table(11, 3, 14, 12)) == "inconclusive"  # 79% vs 54%, p >= 0.05
    assert Table(11, 3, 14, 12).p >= 0.05
    assert verdict(Table(0, 0, 25, 15)) == "not seen"  # nothing agreed: no gap to read


def test_simulation_is_seeded_and_sums_to_one() -> None:
    labels = [True] * 25 + [False] * 15
    first = simulate(labels, 0.51, 0.24, runs=2_000)
    assert first == simulate(labels, 0.51, 0.24, runs=2_000)
    assert sum(first.values()) == pytest.approx(1.0)
    independent = simulate(labels, 0.41, 0.41, runs=2_000)
    assert independent["transfers"] < 0.06  # Fisher's test holds its level under the null


def test_rates_from_a_table() -> None:
    assert rates_from(Table(32, 9, 6, 53)) == (32 / 38, 9 / 62)
