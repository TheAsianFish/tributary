"""The dbt runner: command line, environment and exit-code handling, with no dbt process."""

from __future__ import annotations

import json
import shutil
import sys
from collections.abc import Mapping
from pathlib import Path

import pytest
from pydantic import SecretStr

from tributary.config import Settings
from tributary.warehouse import dbt

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures" / "dbt"


def settings(project: Path) -> Settings:
    return Settings(
        dbt_project_dir=project,
        pg_host="db.internal",
        pg_port=6543,
        pg_password=SecretStr("s3cret"),
        _env_file=None,
    )


class FakeDbt:
    """Records the call and, like dbt, writes run_results.json into --target-path."""

    def __init__(self, returncode: int, results: str | None) -> None:
        self.returncode = returncode
        self.results = results
        self.calls: list[tuple[list[str], Mapping[str, str]]] = []

    def __call__(self, cmd: list[str], env: Mapping[str, str]) -> int:
        self.calls.append((cmd, env))
        target = Path(cmd[cmd.index("--target-path") + 1])
        if self.results is not None:
            shutil.copyfile(FIXTURES / self.results, target / "run_results.json")
        return self.returncode


def test_command_runs_dbt_from_this_interpreter(tmp_path: Path) -> None:
    fake = FakeDbt(0, "run_results_run.json")
    result = dbt.run_dbt(settings(tmp_path / "proj"), ["run"], tmp_path / "work", runner=fake)

    cmd, env = fake.calls[0]
    assert cmd[:4] == [sys.executable, "-m", "dbt.cli.main", "run"]
    assert cmd[cmd.index("--project-dir") + 1] == str(tmp_path / "proj")
    assert cmd[cmd.index("--profiles-dir") + 1] == str(tmp_path / "proj")
    assert cmd[cmd.index("--target-path") + 1] == str(tmp_path / "work/run/target")
    assert cmd[cmd.index("--log-path") + 1] == str(tmp_path / "work/run/logs")
    # The profile reads TRIBUTARY_PG_*: settings from a .env file reach dbt too.
    assert env["TRIBUTARY_PG_HOST"] == "db.internal"
    assert env["TRIBUTARY_PG_PORT"] == "6543"
    assert env["TRIBUTARY_PG_PASSWORD"] == "s3cret"
    assert "--vars" not in cmd
    assert result.ok
    assert not result.crashed
    assert result.target_path == tmp_path / "work/run/target"


def test_configured_vars_reach_every_command(tmp_path: Path) -> None:
    fake = FakeDbt(0, "run_results_run.json")
    configured = settings(tmp_path / "proj").model_copy(
        update={"dbt_vars": "{freshness_severity: warn}"}
    )
    dbt.run_dbt(configured, ["run"], tmp_path / "work", runner=fake)
    cmd, _ = fake.calls[0]
    assert cmd[cmd.index("--vars") + 1] == "{freshness_severity: warn}"
    # The vars are not part of the run's directory name.
    assert cmd[cmd.index("--target-path") + 1] == str(tmp_path / "work/run/target")


def test_multi_word_commands_get_their_own_directories(tmp_path: Path) -> None:
    fake = FakeDbt(0, "run_results_run.json")
    result = dbt.run_dbt(settings(tmp_path), ["docs", "generate"], tmp_path, runner=fake)
    assert result.target_path == tmp_path / "docs-generate/target"


def test_exit_code_1_with_results_is_a_normal_outcome(tmp_path: Path) -> None:
    fake = FakeDbt(1, "run_results_test.json")
    result = dbt.run_dbt(settings(tmp_path), ["test"], tmp_path, runner=fake)

    assert not result.crashed
    assert not result.ok
    run_results = result.require_results()
    assert run_results.command == "test"
    assert {r.status for r in run_results.results} == {"pass", "warn", "fail", "error"}


@pytest.mark.parametrize(
    ("returncode", "results"), [(2, None), (1, None), (2, "run_results_run.json")]
)
def test_crash_is_distinguished_from_failing_nodes(
    tmp_path: Path, returncode: int, results: str | None
) -> None:
    fake = FakeDbt(returncode, results)
    result = dbt.run_dbt(settings(tmp_path), ["run"], tmp_path, runner=fake)

    assert result.crashed
    with pytest.raises(dbt.DbtCrashError, match=f"exited {returncode}"):
        result.require_results()


def test_a_used_target_directory_is_refused(tmp_path: Path) -> None:
    fake = FakeDbt(0, "run_results_run.json")
    dbt.run_dbt(settings(tmp_path), ["run"], tmp_path, runner=fake)
    with pytest.raises(FileExistsError):
        dbt.run_dbt(settings(tmp_path), ["run"], tmp_path, runner=fake)


def test_parse_run_results_reads_the_fields_the_recorders_use() -> None:
    data = json.loads((FIXTURES / "run_results_run.json").read_text())
    parsed = dbt.parse_run_results(data)

    assert parsed.invocation_id == data["metadata"]["invocation_id"]
    assert parsed.command == "run"
    by_id = {r.unique_id: r for r in parsed.results}
    wikis = by_id["model.tributary_fixture.wikis"]
    assert wikis.resource_type == "model"
    assert wikis.status == "success"
    assert wikis.rows_affected == 160
    assert wikis.relation_name == '"tributary"."dbt_fixture"."wikis"'
    assert [t.name for t in wikis.timing] == ["compile", "execute"]
    assert wikis.started_at is not None
    assert wikis.completed_at is not None
    assert wikis.started_at < wikis.completed_at
    assert wikis.started_at.tzinfo is not None


def test_errored_test_has_no_failure_count() -> None:
    parsed = dbt.parse_run_results(json.loads((FIXTURES / "run_results_test.json").read_text()))
    errored = [r for r in parsed.results if r.status == "error"]
    assert errored
    assert all(r.failures is None and r.rows_affected is None for r in errored)
    assert all(r.message and "Database Error" in r.message for r in errored)


def test_stream_copies_child_output(tmp_path: Path) -> None:
    out = tmp_path / "out.txt"
    with out.open("w") as f:
        code = dbt._stream(
            [sys.executable, "-c", "print('one'); print('two'); raise SystemExit(1)"], {}, f
        )
    assert code == 1
    assert out.read_text() == "one\ntwo\n"
