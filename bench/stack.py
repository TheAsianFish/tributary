"""Drive the Compose stack: start the stream job with per-run settings, kill and restart services.

A kill is ``SIGKILL`` to the container's main process (``docker kill``): no
shutdown hook runs, so the stream job loses its in-flight micro-batches and
the broker loses whatever it had not handed to the page cache. Docker counts
a ``docker kill`` as a deliberate stop and does not apply the restart policy,
so the benchmark restarts the service itself after a fixed downtime; the
outage is then the same length in every run.
"""

from __future__ import annotations

import json
import os
import platform
import subprocess
import time
from collections.abc import Mapping, Sequence
from typing import Any

import structlog

log = structlog.get_logger("bench.stack")

PROFILES = ("--profile", "lakehouse", "--profile", "airflow")


def compose(
    *args: str, env: Mapping[str, str] | None = None, check: bool = True, timeout: float = 600
) -> subprocess.CompletedProcess[str]:
    """``docker compose <args>`` with ``env`` layered over this process's environment."""
    full_env = {**os.environ, **(env or {})}
    return subprocess.run(
        ["docker", "compose", *PROFILES, *args],
        env=full_env,
        capture_output=True,
        text=True,
        check=check,
        timeout=timeout,
    )


def container_id(service: str) -> str:
    """The id of the service's (single) container, running or not."""
    out = compose("ps", "-a", "-q", service).stdout.split()
    if not out:
        raise RuntimeError(f"no container for {service}; is the stack up?")
    return out[0]


def running_services() -> dict[str, str]:
    """Running service names to container ids."""
    out = compose("ps", "--format", "json", "--no-trunc").stdout
    services: dict[str, str] = {}
    for line in out.splitlines():
        if line.strip():
            row = json.loads(line)
            services[str(row["Service"])] = str(row["ID"])
    return services


def wait_healthy(service: str, timeout: float = 240.0) -> float:
    """Block until the service's container reports healthy; returns seconds waited."""
    cid = container_id(service)
    start = time.monotonic()
    while True:
        status = subprocess.run(
            [
                "docker",
                "inspect",
                "-f",
                "{{.State.Status}} {{if .State.Health}}{{.State.Health.Status}}{{end}}",
                cid,
            ],
            capture_output=True,
            text=True,
            check=False,
        ).stdout.split()
        if status[:2] == ["running", "healthy"] or status == ["running"]:
            return time.monotonic() - start
        if time.monotonic() - start > timeout:
            raise TimeoutError(f"{service} not healthy after {timeout:.0f} s: {status}")
        time.sleep(0.5)


def kill(service: str) -> float:
    """SIGKILL the service's container; returns ``time.time()`` of the kill."""
    at = time.time()
    compose("kill", "-s", "KILL", service)
    log.info("bench.killed", service=service)
    return at


def start(service: str, *, wait: bool = True) -> float:
    """Start a stopped service again (same container, same volumes); returns its start time."""
    at = time.time()
    compose("start", service)
    if wait:
        wait_healthy(service)
    log.info("bench.started", service=service)
    return at


def start_stream(env: Mapping[str, str], *, wait: bool = True) -> float:
    """Recreate the stream container with ``env`` (topic, partitions, sink, checkpoints).

    ``--no-deps``: Kafka and PostgreSQL keep running across runs.
    """
    at = time.time()
    compose("up", "-d", "--no-deps", "--force-recreate", "stream", env=env)
    if wait:
        wait_healthy("stream")
    log.info("bench.stream_started", **{k: v for k, v in env.items() if "PASSWORD" not in k})
    return at


def stop_stream() -> None:
    compose("stop", "-t", "30", "stream")


def exec_in(service: str, argv: Sequence[str], timeout: float = 900) -> str:
    """Run a command inside a service's container; returns its stdout."""
    return compose("exec", "-T", service, *argv, timeout=timeout).stdout


def resources() -> dict[str, Any]:
    """The machine and every running container's limits, for the results files."""
    containers: dict[str, Any] = {}
    for service, cid in sorted(running_services().items()):
        raw = subprocess.run(
            ["docker", "inspect", cid],
            capture_output=True,
            text=True,
            check=True,
        ).stdout
        info = json.loads(raw)[0]
        host = info["HostConfig"]
        containers[service] = {
            "image": info["Config"]["Image"],
            "mem_limit_bytes": host.get("Memory") or None,
            "nano_cpus": host.get("NanoCpus") or None,
            "cpu_shares": host.get("CpuShares") or None,
            "cpuset": host.get("CpusetCpus") or None,
        }
    docker = subprocess.run(
        ["docker", "version", "--format", "{{.Server.Version}}"],
        capture_output=True,
        text=True,
        check=False,
    ).stdout.strip()
    compose_version = compose("version", "--short", check=False).stdout.strip()
    meminfo = open("/proc/meminfo").read().split()  # noqa: SIM115
    return {
        "nproc": os.cpu_count(),
        "cpu_model": _cpu_model(),
        "mem_total_kb": int(meminfo[1]),
        "kernel": platform.release(),
        "docker": docker,
        "compose": compose_version,
        "containers": containers,
    }


def _cpu_model() -> str | None:
    try:
        for line in open("/proc/cpuinfo"):  # noqa: SIM115
            if line.startswith("model name"):
                return line.split(":", 1)[1].strip()
    except OSError:
        return None
    return None


def subprocess_run(argv: Sequence[str], timeout: float = 60) -> str:
    """Run a command (no shell) and return its stdout; raises on a non-zero exit."""
    return subprocess.run(
        list(argv), capture_output=True, text=True, check=True, timeout=timeout
    ).stdout


class CgroupKiller:
    """SIGKILL every process of a container within a millisecond of :meth:`kill`.

    ``docker kill`` goes through the CLI and the daemon, tens of milliseconds:
    long enough for Spark to write the commit the benchmark is trying to beat.
    Writing ``1`` to the container's cgroup v2 ``cgroup.kill`` kills all of its
    processes at once; it needs root, so a ``sudo -n tee`` is opened in advance
    and the kill is a single write to its stdin. :meth:`available` says whether
    this machine allows it (passwordless sudo and cgroup v2); otherwise the
    benchmark falls back to ``docker kill`` and records that it did.

    Docker sees the container's processes die and would restart it at once under
    ``restart: unless-stopped``, so the policy is suspended until :meth:`restore`.
    """

    def __init__(self, container: str) -> None:
        from bench.observe import CGROUP_ROOT

        self.container = container
        self.path = CGROUP_ROOT / f"docker-{container}.scope" / "cgroup.kill"
        self._policy = subprocess_run(
            ["docker", "inspect", "-f", "{{.HostConfig.RestartPolicy.Name}}", container]
        ).strip()
        subprocess_run(["docker", "update", "--restart", "no", container])
        self._tee = subprocess.Popen(
            ["sudo", "-n", "tee", str(self.path)],
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    @staticmethod
    def available(container: str) -> bool:
        from bench.observe import CGROUP_ROOT

        if not (CGROUP_ROOT / f"docker-{container}.scope" / "cgroup.kill").exists():
            return False
        return (
            subprocess.run(["sudo", "-n", "true"], capture_output=True, check=False).returncode == 0
        )

    def kill(self) -> float:
        """Kill now; returns ``time.time()`` of the write."""
        assert self._tee.stdin is not None
        at = time.time()
        self._tee.stdin.write(b"1\n")
        self._tee.stdin.flush()
        self._tee.stdin.close()
        code = self._tee.wait(10)
        if code != 0:
            raise RuntimeError(f"writing {self.path} failed (tee exited {code})")
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            state = subprocess_run(
                ["docker", "inspect", "-f", "{{.State.Running}}", self.container]
            )
            if state.strip() == "false":
                return at
            time.sleep(0.1)
        raise RuntimeError(f"container {self.container} still running 30 s after cgroup.kill")

    def restore(self) -> None:
        """Put the restart policy back and close the ``tee``; safe to call more than once."""
        if self._tee.poll() is None:
            # EOF ends tee (and its sudo) without writing; nothing is killed.
            if self._tee.stdin is not None and not self._tee.stdin.closed:
                self._tee.stdin.close()
            self._tee.wait(10)
        if self._policy:
            subprocess_run(["docker", "update", "--restart", self._policy, self.container])
            self._policy = ""
