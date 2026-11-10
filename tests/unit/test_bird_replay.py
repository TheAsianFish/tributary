"""The recorded BIRD run replays offline, verdict for verdict, with no model and no database.

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

RECORDED = REPO_ROOT / "eval" / "results" / "bird-dev50-ollama-qwen2.5-coder-7b.json"


@pytest.fixture(scope="module")
def recorded() -> dict[str, Any]:
    loaded: dict[str, Any] = json.loads(RECORDED.read_text(encoding="utf-8"))
    return loaded


@pytest.fixture(scope="module")
def replayed(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    out: Path = tmp_path_factory.mktemp("bird") / "replay.json"
    assert run.main(["--provider", "fake", "--out", str(out)]) == 0
    loaded: dict[str, Any] = json.loads(out.read_text(encoding="utf-8"))
    return loaded


def test_the_replay_reproduces_every_verdict(
    replayed: dict[str, Any], recorded: dict[str, Any]
) -> None:
    assert run.compare(replayed, recorded) == []
    for name, tracks in recorded["variants"].items():
        for track, entry in tracks.items():
            for repairs in ("r0", "r2"):
                got = replayed["variants"][name][track][repairs]
                want = entry[repairs]
                assert (got["correct"], got["total"]) == (want["correct"], want["total"])
                assert got["ex_wilson95"] == want["ex_wilson95"]


def test_the_run_covers_both_variants_and_tracks_on_dev50(recorded: dict[str, Any]) -> None:
    assert recorded["subset"] == "dev50"
    assert set(recorded["variants"]) == {"full", "bm25-k4"}
    for tracks in recorded["variants"].values():
        assert tracks["official"]["r2"]["total"] == 50
        assert tracks["corrected"]["r2"]["total"] == 45


def test_bird_s_own_scorer_agreed_on_every_live_pair(recorded: dict[str, Any]) -> None:
    check = recorded["cross_check"]
    assert check["pairs"] > 0
    assert check["agree"] == check["pairs"]
    for tracks in recorded["variants"].values():
        for entry in tracks.values():
            for v in entry["verdicts"]:
                if v["official_ex"] is not None:
                    assert v["official_ex"] == v["correct"]


def test_a_local_model_costs_nothing_and_says_so(recorded: dict[str, Any]) -> None:
    assert all(row["cost_usd"] == 0 for row in recorded["ledger"])
    assert all(row["estimate_usd"]["claude-haiku-5-5"] > 0 for row in recorded["ledger"])
