"""Recorded database executions: record, save, replay."""

from __future__ import annotations

import gzip
import json
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import pytest

from eval.bird.recorded import (
    ExecutionMissing,
    Recorder,
    RecordingExecutor,
    ReplayExecutor,
    decode,
    encode,
    execution_key,
    load_executions,
    to_record,
    write_executions,
)
from tests.unit.agent_doubles import FakeExecutor, result
from tributary.agent.execute import ExecutionError, QueryResult

SQL = "SELECT a, b, c FROM t"


def rich_result() -> QueryResult:
    return QueryResult(
        ("a", "b", "c"),
        ((Decimal("2"), date(2026, 1, 2), None), (Decimal("2.5"), date(2026, 1, 3), "x")),
        True,
        1.23456,
        cost=12.5,
    )


def test_every_value_type_round_trips() -> None:
    values: list[object] = [
        None,
        True,
        7,
        "x",
        1.5,
        float("nan"),
        float("inf"),
        Decimal("61.5714285714285714"),
        datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC),
        date(2026, 1, 2),
        time(3, 4, 5),
        timedelta(days=1, seconds=2, microseconds=3),
        UUID(int=7),
        b"\x00\xff",
        [1, Decimal("2.5"), None],
        {"k": [1, 2]},
    ]
    for value in values:
        back = decode(json.loads(json.dumps(encode(value))))
        if isinstance(value, float) and value != value:
            assert isinstance(back, float)
            assert back != back
        else:
            assert back == value
            assert type(back) is type(value)


def test_an_unknown_tag_is_refused() -> None:
    with pytest.raises(ValueError, match="not a recorded value"):
        decode({"$nope": 1})


def replay_of(recorder: Recorder, database: str = "db") -> ReplayExecutor:
    recorder.save()
    return ReplayExecutor(load_executions(recorder.path), database)


def test_replay_returns_the_recorded_columns_and_rows_with_their_types(tmp_path: Path) -> None:
    recorder = Recorder(tmp_path / "exec.jsonl.gz")
    live = RecordingExecutor(FakeExecutor(default=rich_result()), "db", recorder)
    live.run(SQL)

    replayed = replay_of(recorder).run(SQL)

    assert replayed.columns == ("a", "b", "c")
    assert replayed.rows == (
        (Decimal("2"), date(2026, 1, 2), None),
        (Decimal("2.5"), date(2026, 1, 3), "x"),
    )
    assert type(replayed.rows[1][0]) is Decimal
    assert replayed.truncated is True
    assert replayed.cost == 12.5
    assert replayed.elapsed_ms == 1.235


def test_replay_reraises_a_recorded_error_with_identical_feedback(tmp_path: Path) -> None:
    error = ExecutionError("database", 'column "nope" does not exist', "42703")
    recorder = Recorder(tmp_path / "exec.jsonl.gz")
    live = RecordingExecutor(FakeExecutor(errors={SQL: error}), "db", recorder)
    with pytest.raises(ExecutionError):
        live.run(SQL)

    with pytest.raises(ExecutionError) as caught:
        replay_of(recorder).run(SQL)

    assert caught.value.kind == "database"
    assert caught.value.message == error.message
    assert caught.value.sqlstate == "42703"
    assert caught.value.feedback() == error.feedback()


def test_a_connection_error_is_raised_but_not_recorded(tmp_path: Path) -> None:
    outage = ExecutionError("connection", "refused")
    recorder = Recorder(tmp_path / "exec.jsonl.gz")
    live = RecordingExecutor(FakeExecutor(errors={SQL: outage}), "db", recorder)

    with pytest.raises(ExecutionError) as caught:
        live.run(SQL)

    assert caught.value is outage
    assert recorder.records == {}


def test_a_permission_error_is_not_recorded_either(tmp_path: Path) -> None:
    recorder = Recorder(tmp_path / "exec.jsonl.gz")
    live = RecordingExecutor(
        FakeExecutor(errors={SQL: ExecutionError("permission", "denied")}), "db", recorder
    )
    with pytest.raises(ExecutionError):
        live.run(SQL)
    assert recorder.records == {}


def test_a_timeout_is_recorded(tmp_path: Path) -> None:
    recorder = Recorder(tmp_path / "exec.jsonl.gz")
    live = RecordingExecutor(
        FakeExecutor(errors={SQL: ExecutionError("timeout", "slow", "57014")}), "db", recorder
    )
    with pytest.raises(ExecutionError):
        live.run(SQL)
    assert len(recorder.records) == 1


def test_an_unrecorded_query_raises_execution_missing(tmp_path: Path) -> None:
    replay = ReplayExecutor({}, "db")

    with pytest.raises(ExecutionMissing, match="no recorded execution on db"):
        replay.run("SELECT 1")


def test_the_key_depends_on_the_database_and_the_query(tmp_path: Path) -> None:
    assert execution_key("a", SQL) != execution_key("b", SQL)
    assert execution_key("a", SQL) != execution_key("a", SQL + " ")
    assert execution_key("ab", "c") != execution_key("a", "bc")

    recorder = Recorder(tmp_path / "exec.jsonl.gz")
    RecordingExecutor(FakeExecutor(default=result()), "one", recorder).run(SQL)

    with pytest.raises(ExecutionMissing):
        replay_of(recorder, "two").run(SQL)
    assert replay_of(recorder, "one").run(SQL).rows == ((42,),)


def two_writes(tmp_path: Path) -> tuple[Path, Path, dict[str, dict[str, object]]]:
    records = {
        r["key"]: r
        for r in (
            to_record("db", "SELECT 1", result()),
            to_record("db", "SELECT 2", rich_result()),
            to_record("db", "SELECT 3", ExecutionError("database", "bad", "42601")),
        )
    }
    first, second = tmp_path / "one" / "x.gz", tmp_path / "two" / "x.gz"
    write_executions(first, records)
    write_executions(second, dict(reversed(records.items())))
    return first, second, records


def test_write_executions_content_is_independent_of_insertion_order(tmp_path: Path) -> None:
    first, second, records = two_writes(tmp_path)

    assert gzip.decompress(first.read_bytes()) == gzip.decompress(second.read_bytes())
    assert load_executions(first) == records
    assert [p.name for p in first.parent.iterdir()] == ["x.gz"]


def test_write_executions_is_byte_identical_for_the_same_records(tmp_path: Path) -> None:
    first, second, _ = two_writes(tmp_path)

    assert first.read_bytes() == second.read_bytes()


def test_a_recorder_keeps_earlier_records_when_reopened(tmp_path: Path) -> None:
    path = tmp_path / "exec.jsonl.gz"
    first = Recorder(path)
    RecordingExecutor(FakeExecutor(default=result()), "db", first).run("SELECT 1")
    first.save()

    second = Recorder(path)
    RecordingExecutor(FakeExecutor(default=result()), "db", second).run("SELECT 2")
    second.save()

    assert len(load_executions(path)) == 2


def test_load_executions_of_a_missing_file_is_empty(tmp_path: Path) -> None:
    assert load_executions(tmp_path / "nope.gz") == {}
