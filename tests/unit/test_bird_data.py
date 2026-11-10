"""BIRD question loading, the fixed subsets and the checksum-pinned downloads."""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from pathlib import Path

import httpx
import pytest

from eval.bird import data
from eval.bird.data import (
    BirdQuestion,
    Source,
    SourceError,
    corrected_questions,
    fetch,
    official_questions,
    select,
    subset_ids,
)

BODY = b"hello bird"
SHA = hashlib.sha256(BODY).hexdigest()


def fake_source(sha: str = SHA) -> Source:
    return Source("thing.json", "https://example.test/thing.json", sha, len(BODY))


def client(body: bytes = BODY, status: int = 200) -> httpx.Client:
    return httpx.Client(
        transport=httpx.MockTransport(lambda r: httpx.Response(status, content=body))
    )


def question(qid: int, difficulty: str = "simple", evidence: str = "") -> BirdQuestion:
    return BirdQuestion(qid, "db", f"q{qid}", evidence, "SELECT 1", difficulty, "official", "x")


# --- the committed questions --------------------------------------------------------------------


def test_official_questions_are_500_with_the_published_difficulty_mix() -> None:
    questions = official_questions()

    assert len(questions) == 500
    assert len({q.id for q in questions}) == 500
    assert Counter(q.difficulty for q in questions) == {
        "simple": 148,
        "moderate": 250,
        "challenging": 102,
    }
    assert {q.track for q in questions} == {"official"}
    assert all(q.sql == q.source_sql for q in questions)


def test_corrected_questions_keep_only_verified_ids_with_transpiled_sql() -> None:
    transpiled = json.loads((data.DATA_DIR / "corrected_pg.json").read_text(encoding="utf-8"))
    entries = transpiled["questions"]
    verified = {int(k): v for k, v in entries.items() if v["status"] == "verified"}
    official = {q.id: q for q in official_questions()}

    questions = corrected_questions()

    rows = json.loads((data.DATA_DIR / data.CORRECTED.name).read_text(encoding="utf-8"))
    arcwise = {int(r["question_id"]) for r in rows}
    assert questions
    assert {q.id for q in questions} == set(verified) & arcwise
    for q in questions:
        assert q.track == "corrected"
        assert q.sql == verified[q.id]["postgres"]
        assert q.difficulty == official[q.id].difficulty


def test_corrected_questions_filter_and_difficulty_on_hand_built_files(tmp_path: Path) -> None:
    corrected = tmp_path / "c.json"
    corrected.write_text(
        json.dumps(
            [
                {"question_id": 1, "db_id": "d", "question": "a", "evidence": "e", "SQL": "S1"},
                {"question_id": 2, "db_id": "d", "question": "b", "SQL": "S2"},
                {"question_id": 3, "db_id": "d", "question": "c", "SQL": "S3"},
                {"question_id": 4, "db_id": "d", "question": "d", "SQL": "S4"},
            ]
        )
    )
    transpiled = tmp_path / "t.json"
    transpiled.write_text(
        json.dumps(
            {
                "questions": {
                    "1": {"status": "verified", "postgres": "PG1"},
                    "2": {"status": "mismatch", "postgres": "PG2"},
                    "3": {"status": "verified", "postgres": "PG3"},
                }
            }
        )
    )
    official = [question(1, "challenging"), question(2, "simple")]

    out = corrected_questions(corrected, transpiled, official)

    assert [q.id for q in out] == [1, 3]
    assert out[0].sql == "PG1"
    assert out[0].source_sql == "S1"
    assert out[0].evidence == "e"
    assert out[0].difficulty == "challenging"
    assert out[1].difficulty == "unknown"  # not in the official release


# --- subsets ------------------------------------------------------------------------------------


def test_dev50_is_deterministic_with_the_difficulty_quota() -> None:
    pool = official_questions()
    by_id = {q.id: q for q in pool}

    first = subset_ids("dev50", pool)
    second = subset_ids("dev50", list(reversed(pool)))

    assert first == second == sorted(first)
    assert len(set(first)) == 50
    assert Counter(by_id[i].difficulty for i in first) == {
        "simple": 15,
        "moderate": 25,
        "challenging": 10,
    }


def test_subset_respects_also_in() -> None:
    pool = official_questions()
    allowed = {q.id for q in pool if q.id % 2 == 0}

    ids = subset_ids("dev50", pool, also_in=allowed)

    assert len(ids) == 50
    assert set(ids) <= allowed


def test_subset_fails_when_a_difficulty_has_too_few_questions() -> None:
    pool = [question(i, "simple") for i in range(20)]
    with pytest.raises(ValueError, match="only 0 moderate"):
        subset_ids("dev50", pool)


def test_select_all_keeps_everything_and_none_too() -> None:
    pool = [question(i) for i in range(5)]
    assert select(pool, "all") == pool
    assert select(pool, None) == pool


def test_select_dev50_keeps_exactly_the_subset_members() -> None:
    pool = official_questions()

    chosen = select(pool, "dev50")

    assert len(chosen) == 50
    assert [q.id for q in chosen] == [q.id for q in pool if q.id in {c.id for c in chosen}]


# --- prompt -------------------------------------------------------------------------------------


def test_prompt_appends_evidence_only_when_present() -> None:
    assert question(1, evidence="x means y").prompt == "q1\nEvidence: x means y"
    assert question(1, evidence="").prompt == "q1"
    assert question(1, evidence="  \n ").prompt == "q1"


# --- fetch --------------------------------------------------------------------------------------


def test_fetch_downloads_into_the_cache_when_the_sha_matches(tmp_path: Path) -> None:
    cache = tmp_path / "cache"

    path = fetch(fake_source(), cache, client=client())

    assert path == cache / "thing.json"
    assert path.read_bytes() == BODY
    assert [p.name for p in cache.iterdir()] == ["thing.json"]


def test_fetch_rejects_a_sha_mismatch_and_leaves_nothing_behind(tmp_path: Path) -> None:
    cache = tmp_path / "cache"

    with pytest.raises(SourceError, match="expected"):
        fetch(fake_source(), cache, client=client(b"tampered"))

    assert list(cache.iterdir()) == []


def test_fetch_rejects_an_http_error_and_leaves_nothing_behind(tmp_path: Path) -> None:
    with pytest.raises(SourceError, match="HTTP 404"):
        fetch(fake_source(), tmp_path, client=client(b"", status=404))

    assert list(tmp_path.iterdir()) == []


def test_fetch_uses_a_valid_cached_file_without_the_network(tmp_path: Path) -> None:
    (tmp_path / "thing.json").write_bytes(BODY)

    def boom(request: httpx.Request) -> httpx.Response:
        raise AssertionError("no request expected")

    path = fetch(fake_source(), tmp_path, client=httpx.Client(transport=httpx.MockTransport(boom)))

    assert path.read_bytes() == BODY


def test_fetch_refuses_a_cached_file_with_the_wrong_sha(tmp_path: Path) -> None:
    (tmp_path / "thing.json").write_bytes(b"corrupt")

    with pytest.raises(SourceError, match="does not match"):
        fetch(fake_source(), tmp_path, client=client())

    assert (tmp_path / "thing.json").read_bytes() == b"corrupt"
