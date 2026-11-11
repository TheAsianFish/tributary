"""Prometheus text exposition (format 0.0.4) for the API, without a client library.

Labels are bounded: requests are counted by route template (``/query``, not the
raw path; anything no route matched is ``unmatched``), method and status; answers
by route and agent status. No label ever carries a key, a question or SQL.
"""

from __future__ import annotations

import threading
from collections import defaultdict

CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"
BUCKETS: tuple[float, ...] = (0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0)


def _labels(**labels: str) -> str:
    def escape(value: str) -> str:
        return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')

    return "{" + ",".join(f'{k}="{escape(v)}"' for k, v in labels.items()) + "}"


def _num(value: float) -> str:
    if value == int(value) and abs(value) < 1e15:
        return str(int(value))
    return repr(value)


class _Histogram:
    __slots__ = ("count", "counts", "sum")

    def __init__(self) -> None:
        self.counts = [0] * len(BUCKETS)
        self.sum = 0.0
        self.count = 0

    def observe(self, value: float) -> None:
        for i, bound in enumerate(BUCKETS):
            if value <= bound:
                self.counts[i] += 1
        self.sum += value
        self.count += 1


class Metrics:
    """Process-local counters; thread-safe (handlers run in worker threads)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._requests: defaultdict[tuple[str, str, str], int] = defaultdict(int)
        self._latency: defaultdict[str, _Histogram] = defaultdict(_Histogram)
        self._answers: defaultdict[tuple[str, str], int] = defaultdict(int)
        self._rate_limited = 0
        self._tokens: defaultdict[str, int] = defaultdict(int)
        self._cost_usd = 0.0

    def request(self, route: str, method: str, status: int, seconds: float) -> None:
        with self._lock:
            self._requests[(route, method, str(status))] += 1
            self._latency[route].observe(seconds)

    def answer(
        self, route: str, status: str, *, input_tokens: int, output_tokens: int, cost_usd: float
    ) -> None:
        with self._lock:
            self._answers[(route, status)] += 1
            self._tokens["input"] += input_tokens
            self._tokens["output"] += output_tokens
            self._cost_usd += cost_usd

    def rate_limited(self) -> None:
        with self._lock:
            self._rate_limited += 1

    def render(self) -> str:
        with self._lock:
            lines: list[str] = []

            def family(name: str, kind: str, help_text: str) -> None:
                lines.append(f"# HELP {name} {help_text}")
                lines.append(f"# TYPE {name} {kind}")

            family("tributary_http_requests_total", "counter", "HTTP requests answered.")
            for (route, method, status), n in sorted(self._requests.items()):
                labels = _labels(route=route, method=method, status=status)
                lines.append(f"tributary_http_requests_total{labels} {n}")

            name = "tributary_http_request_duration_seconds"
            family(name, "histogram", "Time from request to response, by route.")
            for route, h in sorted(self._latency.items()):
                for bound, n in zip(BUCKETS, h.counts, strict=True):
                    lines.append(f"{name}_bucket{_labels(route=route, le=_num(bound))} {n}")
                lines.append(f"{name}_bucket{_labels(route=route, le='+Inf')} {h.count}")
                lines.append(f"{name}_sum{_labels(route=route)} {_num(h.sum)}")
                lines.append(f"{name}_count{_labels(route=route)} {h.count}")

            family(
                "tributary_answers_total",
                "counter",
                "Agent answers by route and status (answered, unanswerable, blocked, failed).",
            )
            for (route, status), n in sorted(self._answers.items()):
                lines.append(f"tributary_answers_total{_labels(route=route, status=status)} {n}")

            family(
                "tributary_rate_limited_total", "counter", "Requests refused by the per-key limit."
            )
            lines.append(f"tributary_rate_limited_total {self._rate_limited}")

            family("tributary_model_tokens_total", "counter", "Model tokens spent by answers.")
            for direction in ("input", "output"):
                n = self._tokens[direction]
                lines.append(f"tributary_model_tokens_total{_labels(direction=direction)} {n}")

            family("tributary_model_cost_usd_total", "counter", "Model cost of answers, USD.")
            lines.append(f"tributary_model_cost_usd_total {_num(self._cost_usd)}")
            return "\n".join(lines) + "\n"
