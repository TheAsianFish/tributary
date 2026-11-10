"""The adversarial guard suite and its evaluator, offline."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
import yaml

from eval import guard_eval as ge
from tributary.agent.guard import Guard

SUITE = ge.load_unsafe(ge.UNSAFE_PATH)


@pytest.fixture
def guard() -> Guard:
    return Guard(ge.catalog_relations())


def test_suite_is_large_ids_unique_and_categories_valid() -> None:
    raw = yaml.safe_load(ge.UNSAFE_PATH.read_text())["unsafe"]
    assert len(raw) >= 60
    ids = [entry["id"] for entry in raw]
    assert len(ids) == len(set(ids))
    for entry in raw:
        assert re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", entry["id"]), entry["id"]
        assert entry["category"] in ge.CATEGORIES, entry["id"]
        assert entry["sql"].strip(), entry["id"]
        assert entry["why"].strip(), entry["id"]
    assert {entry["category"] for entry in raw} == ge.CATEGORIES


@pytest.mark.parametrize("item", SUITE, ids=[item.id for item in SUITE])
def test_every_unsafe_statement_is_blocked(guard: Guard, item: ge.Item) -> None:
    result = guard.check(item.sql)
    assert not result.allowed, f"{item.id} was allowed as {result.sql!r}"
    assert result.sql == ""


def _item(id_: str, sql: str, category: str = "benign") -> ge.Item:
    return ge.Item(id_, category, sql, "test")


def test_report_passes_when_unsafe_blocked_and_benign_allowed(guard: Guard) -> None:
    report = ge.evaluate(
        guard,
        [
            _item("drop", "DROP TABLE marts.fct_edits", "ddl"),
            _item("sleep", "SELECT pg_sleep(1)", "dangerous_function"),
        ],
        [_item("count", "SELECT count(*) FROM marts.fct_edits")],
    )
    assert report.passed
    assert report.unsafe_blocked == 2
    assert report.benign_blocked == ()
    assert report.by_category() == {"dangerous_function": (1, 1), "ddl": (1, 1)}
    assert report.by_code() == {"function_not_allowed": 1, "write_or_ddl": 1}
    assert "unsafe blocked     2/2 (100.0%)" in ge.render(report)
    assert ge.render(report).endswith("PASS")


def test_report_fails_on_a_missed_unsafe_item(guard: Guard) -> None:
    report = ge.evaluate(guard, [_item("oops", "SELECT 1", "dml")], [])
    assert not report.passed
    assert [v.item.id for v in report.unsafe_missed] == ["oops"]
    assert "MISSED oops" in ge.render(report)


def test_report_fails_on_a_false_block(guard: Guard) -> None:
    report = ge.evaluate(guard, [], [_item("raw", "SELECT * FROM raw.recentchange")])
    assert not report.passed
    assert [v.item.id for v in report.benign_blocked] == ["raw"]
    text = ge.render(report)
    assert "benign false blocks 1/1" in text
    assert "test:raw: relation_not_allowed" in text


def test_json_is_deterministic(guard: Guard) -> None:
    unsafe = [_item("drop", "DROP TABLE marts.fct_edits", "ddl")]
    benign = [_item("one", "SELECT 1")]
    first = json.dumps(ge.evaluate(guard, unsafe, benign).to_json(), sort_keys=True)
    second = json.dumps(ge.evaluate(guard, unsafe, benign).to_json(), sort_keys=True)
    assert first == second
    data = json.loads(first)
    assert data["passed"] is True
    assert data["benign"]["items"][0]["sql"] == "SELECT 1 LIMIT 1000"
    assert data["unsafe"]["by_code"] == {"write_or_ddl": 1}


def _write(path: Path, data: object) -> Path:
    path.write_text(yaml.safe_dump(data))
    return path


def test_main_exit_codes_and_output(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    unsafe = _write(
        tmp_path / "unsafe.yaml",
        {"unsafe": [{"id": "drop", "category": "ddl", "sql": "DROP TABLE x", "why": "w"}]},
    )
    good = _write(tmp_path / "good.yaml", {"questions": [{"id": "q", "sql": "SELECT 1"}]})
    bad = _write(
        tmp_path / "bad.yaml", {"questions": [{"id": "r", "sql": "SELECT * FROM raw.recentchange"}]}
    )
    out = tmp_path / "guard.json"
    args = ["--unsafe", str(unsafe), "--output", str(out)]

    assert ge.main([*args, "--benign", str(good)]) == 0
    first = out.read_bytes()
    assert ge.main([*args, "--benign", str(good)]) == 0
    assert out.read_bytes() == first

    assert ge.main([*args, "--benign", str(good), "--benign", str(bad)]) == 1
    assert ge.main([*args, "--benign", str(bad), "--relations", "raw.recentchange"]) == 0


def test_missing_benign_set_is_loud(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    unsafe = _write(
        tmp_path / "unsafe.yaml",
        {"unsafe": [{"id": "drop", "category": "ddl", "sql": "DROP TABLE x", "why": "w"}]},
    )
    code = ge.main(
        [
            "--unsafe",
            str(unsafe),
            "--benign",
            str(tmp_path / "missing.yaml"),
            "--output",
            str(tmp_path / "g.json"),
        ]
    )
    assert code == 0
    assert "benign set is EMPTY" in capsys.readouterr().err


def test_loader_rejects_bad_suites(tmp_path: Path) -> None:
    item = {"id": "a", "category": "ddl", "sql": "DROP TABLE x", "why": "w"}
    with pytest.raises(ValueError, match="duplicate ids: a"):
        ge.load_unsafe(_write(tmp_path / "dup.yaml", {"unsafe": [item, item]}))
    with pytest.raises(ValueError, match="unknown category"):
        ge.load_unsafe(_write(tmp_path / "cat.yaml", {"unsafe": [{**item, "category": "nope"}]}))
    with pytest.raises(ValueError, match="'unsafe'"):
        ge.load_unsafe(_write(tmp_path / "shape.yaml", {"items": []}))
