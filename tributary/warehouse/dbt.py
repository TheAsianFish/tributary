"""Run dbt as a child process and read back what it did.

dbt is started with ``python -m dbt.cli.main`` from the interpreter running this
code, so it is always the dbt of the same virtual environment (no reliance on
``PATH`` or on where the ``dbt`` script was installed). A subprocess rather than
the in-process ``dbtRunner`` because dbt keeps global state (flags, the adapter
registry, the event manager and its log handlers) that is not safe to reuse
across invocations in one long-lived process, and because it reconfigures
logging: inside an Airflow worker that would fight with the task logger. A
child process starts clean, can be killed on timeout, and its output is just
lines on a pipe that end up in the task log.

Every invocation gets its own ``--target-path`` and ``--log-path`` so that
concurrent or retried runs never read each other's ``run_results.json`` (a stale
file from an earlier run would otherwise look like this run's result). The
price is a full project parse each time (no partial-parse cache), a few seconds.

Exit codes follow dbt's contract: 0 all nodes succeeded, 1 dbt ran but some node
failed (failing tests included, a normal outcome that must still be recorded),
2 dbt itself failed (bad project, unreachable database) and wrote no results.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, TextIO

import structlog

from tributary.config import Settings

log = structlog.get_logger(__name__)

DBT_MODULE = "dbt.cli.main"


class DbtCrashError(RuntimeError):
    """dbt failed before producing results (exit code 2, or no ``run_results.json``)."""


@dataclass(frozen=True, slots=True)
class Timing:
    """One phase of a node (``compile`` or ``execute``)."""

    name: str
    started_at: datetime | None
    completed_at: datetime | None


@dataclass(frozen=True, slots=True)
class NodeResult:
    """The parts of one ``run_results.json`` entry that the recorders use."""

    unique_id: str
    status: str
    failures: int | None
    message: str | None
    execution_time: float
    timing: tuple[Timing, ...]
    rows_affected: int | None
    relation_name: str | None

    @property
    def resource_type(self) -> str:
        """``model``, ``seed``, ``test``, ... (the unique id's first component)."""
        return self.unique_id.split(".", 1)[0]

    @property
    def started_at(self) -> datetime | None:
        starts = [t.started_at for t in self.timing if t.started_at is not None]
        return min(starts) if starts else None

    @property
    def completed_at(self) -> datetime | None:
        ends = [t.completed_at for t in self.timing if t.completed_at is not None]
        return max(ends) if ends else None


@dataclass(frozen=True, slots=True)
class RunResults:
    """A parsed ``run_results.json``."""

    invocation_id: str
    generated_at: datetime
    command: str | None
    elapsed_time: float
    results: tuple[NodeResult, ...]


@dataclass(frozen=True, slots=True)
class DbtResult:
    """Outcome of one dbt invocation."""

    args: tuple[str, ...]
    returncode: int
    target_path: Path
    run_results: RunResults | None
    seconds: float

    @property
    def crashed(self) -> bool:
        """dbt did not get as far as running nodes; there is nothing to record."""
        return self.returncode not in (0, 1) or self.run_results is None

    @property
    def ok(self) -> bool:
        return self.returncode == 0 and self.run_results is not None

    def require_results(self) -> RunResults:
        """The run results, or :class:`DbtCrashError` when dbt crashed."""
        if self.crashed or self.run_results is None:
            raise DbtCrashError(
                f"dbt {' '.join(self.args)} exited {self.returncode} "
                f"without results (target {self.target_path})"
            )
        return self.run_results


def _ts(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    return datetime.fromisoformat(value)


def parse_run_results(data: Mapping[str, Any]) -> RunResults:
    """Turn a ``run_results.json`` document into :class:`RunResults` (pure)."""
    # Any: the document is untyped JSON; every field read here is converted explicitly.
    metadata = data.get("metadata", {})
    results = []
    for raw in data.get("results", []):
        adapter = raw.get("adapter_response") or {}
        rows = adapter.get("rows_affected")
        results.append(
            NodeResult(
                unique_id=str(raw["unique_id"]),
                status=str(raw["status"]),
                failures=None if raw.get("failures") is None else int(raw["failures"]),
                message=raw.get("message"),
                execution_time=float(raw.get("execution_time") or 0.0),
                timing=tuple(
                    Timing(str(t["name"]), _ts(t.get("started_at")), _ts(t.get("completed_at")))
                    for t in raw.get("timing", [])
                ),
                rows_affected=None if rows is None else int(rows),
                relation_name=raw.get("relation_name"),
            )
        )
    generated = _ts(metadata.get("generated_at"))
    if generated is None:
        raise ValueError("run_results.json has no metadata.generated_at")
    return RunResults(
        invocation_id=str(metadata.get("invocation_id", "")),
        generated_at=generated,
        command=data.get("args", {}).get("which"),
        elapsed_time=float(data.get("elapsed_time") or 0.0),
        results=tuple(results),
    )


def dbt_command(
    args: Sequence[str],
    *,
    project_dir: Path,
    target_path: Path,
    log_path: Path,
    dbt_vars: str = "",
) -> list[str]:
    """The argv for one dbt invocation (exposed for tests).

    ``dbt_vars`` (a YAML or JSON mapping, from ``TRIBUTARY_DBT_VARS``) is passed
    as ``--vars`` to every command, e.g. to demote the freshness test to a
    warning during a backfill without editing the project.
    """
    extra = ["--vars", dbt_vars] if dbt_vars.strip() else []
    return [
        sys.executable,
        "-m",
        DBT_MODULE,
        *args,
        "--project-dir",
        str(project_dir),
        "--profiles-dir",
        str(project_dir),
        "--target-path",
        str(target_path),
        "--log-path",
        str(log_path),
        "--no-use-colors",
        *extra,
    ]


def dbt_env(settings: Settings, base: Mapping[str, str] | None = None) -> dict[str, str]:
    """The child's environment: ours, plus the connection settings dbt's profile reads.

    Settings may come from a ``.env`` file that pydantic reads but never exports;
    exporting them here means dbt connects where the rest of the process does.
    """
    env = dict(os.environ if base is None else base)
    env.update(
        TRIBUTARY_PG_HOST=settings.pg_host,
        TRIBUTARY_PG_PORT=str(settings.pg_port),
        TRIBUTARY_PG_DATABASE=settings.pg_database,
        TRIBUTARY_PG_USER=settings.pg_user,
        TRIBUTARY_PG_PASSWORD=settings.pg_password.get_secret_value(),
    )
    # A scheduler should not phone home on every run unless asked to.
    env.setdefault("DBT_SEND_ANONYMOUS_USAGE_STATS", "false")
    return env


Runner = Callable[[list[str], Mapping[str, str]], int]


def _stream(cmd: list[str], env: Mapping[str, str], out: TextIO | None = None) -> int:
    """Run ``cmd``, copying its output line by line to ``out`` (the task log)."""
    sink = out or sys.stdout
    with subprocess.Popen(  # noqa: S603  (argv built from our own settings, no shell)
        cmd,
        env=dict(env),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    ) as proc:
        assert proc.stdout is not None
        for line in proc.stdout:
            sink.write(line)
        sink.flush()
        return proc.wait()


def run_dbt(
    settings: Settings,
    args: Sequence[str],
    work_dir: Path,
    *,
    runner: Runner = _stream,
    env: Mapping[str, str] | None = None,
) -> DbtResult:
    """Run one dbt command with its own target and log directories under ``work_dir``.

    Never raises for dbt's own failures: the caller decides, from
    :attr:`DbtResult.crashed` and the node statuses, what is an error.
    """
    name = "-".join(a for a in args if not a.startswith("-")) or "dbt"
    target_path = work_dir / name / "target"
    log_path = work_dir / name / "logs"
    if target_path.exists() and any(target_path.iterdir()):
        raise FileExistsError(f"{target_path} is not empty; each dbt run needs a fresh target")
    target_path.mkdir(parents=True, exist_ok=True)
    log_path.mkdir(parents=True, exist_ok=True)
    cmd = dbt_command(
        args,
        project_dir=settings.dbt_project_dir,
        target_path=target_path,
        log_path=log_path,
        dbt_vars=settings.dbt_vars,
    )
    log.info("dbt.start", args=list(args), target_path=str(target_path))
    start = time.perf_counter()
    returncode = runner(cmd, dbt_env(settings) if env is None else env)
    seconds = time.perf_counter() - start
    results_file = target_path / "run_results.json"
    run_results = (
        parse_run_results(json.loads(results_file.read_text())) if results_file.exists() else None
    )
    result = DbtResult(tuple(args), returncode, target_path, run_results, seconds)
    log.info(
        "dbt.done",
        args=list(args),
        returncode=returncode,
        crashed=result.crashed,
        nodes=0 if run_results is None else len(run_results.results),
        seconds=round(seconds, 2),
    )
    return result
