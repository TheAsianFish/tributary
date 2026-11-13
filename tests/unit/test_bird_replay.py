"""The recorded BIRD runs replay offline, verdict for verdict, with no model and no database.

The recordings are real: every model call of ``qwen2.5-coder:7b`` through
Ollama and every query the run sent to PostgreSQL, captured once by
``make eval PROVIDER=ollama RECORD=1``. Replaying them through the same
planner, guard and scorer must reproduce the recorded report exactly.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from eval.bird import run
from tributary.config import REPO_ROOT

RESULTS = REPO_ROOT / "eval" / "results"
RECORDED = RESULTS / "bird-dev50-ollama-qwen2.5-coder-7b.json"
RUNS = sorted(RESULTS.glob("bird-*-ollama-*.json"))


def _load(path: Path) -> dict[str, Any]:
    loaded: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return loaded


@pytest.fixture(scope="module")
def recorded() -> dict[str, Any]:
    return _load(RECORDED)


@pytest.mark.parametrize("path", RUNS, ids=lambda p: p.name)
def test_the_replay_reproduces_every_verdict(path: Path, tmp_path: Path) -> None:
    recorded = _load(path)
    out = tmp_path / "replay.json"
    assert run.main(["--provider", "fake", "--recorded", str(path), "--out", str(out)]) == 0
    replayed = _load(out)
    assert replayed["subset"] == recorded["subset"]
    assert run.compare(replayed, recorded) == []
    for name, tracks in recorded["variants"].items():
        for track, entry in tracks.items():
            for repairs in ("r0", "r2"):
                got = replayed["variants"][name][track][repairs]
                want = entry[repairs]
                assert (got["correct"], got["total"]) == (want["correct"], want["total"])
                assert got["ex_wilson95"] == want["ex_wilson95"]


def test_the_run_covers_every_variant_and_track_on_dev50(recorded: dict[str, Any]) -> None:
    assert recorded["subset"] == "dev50"
    assert set(recorded["variants"]) == {"full", "bm25-k4", "full+a", "full+v+f+a"}
    # M7's cells were added by resuming M4's run: its rows must not have moved.
    m4 = {("full", "official"): 21, ("full", "corrected"): 18, ("bm25-k4", "official"): 21}
    m4[("bm25-k4", "corrected")] = 17
    for (variant, track), correct in m4.items():
        assert recorded["variants"][variant][track]["r2"]["correct"] == correct
    for tracks in recorded["variants"].values():
        assert tracks["official"]["r2"]["total"] == 50
        assert tracks["corrected"]["r2"]["total"] == 45


def test_a_replay_refuses_a_subset_its_run_was_not_graded_on() -> None:
    with pytest.raises(SystemExit):
        run.main(["--provider", "fake", "--recorded", str(RECORDED), "--subset", "holdout50"])


@pytest.mark.parametrize("path", RUNS, ids=lambda p: p.name)
def test_bird_s_own_scorer_agreed_on_every_live_pair(path: Path) -> None:
    recorded = _load(path)
    check = recorded["cross_check"]
    assert check["pairs"] > 0
    assert check["agree"] == check["pairs"]
    for tracks in recorded["variants"].values():
        for entry in tracks.values():
            for v in entry["verdicts"]:
                if v["official_ex"] is not None:
                    assert v["official_ex"] == v["correct"]


@pytest.mark.parametrize("path", RUNS, ids=lambda p: p.name)
def test_a_local_model_costs_nothing_and_says_so(path: Path) -> None:
    recorded = _load(path)
    assert all(row["cost_usd"] == 0 for row in recorded["ledger"])
    assert all(row["estimate_usd"]["claude-haiku-5-5"] > 0 for row in recorded["ledger"])
