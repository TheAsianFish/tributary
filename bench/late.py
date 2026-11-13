"""``make bench-late``: can the pipeline tell a late copy from a lost event, and recover a lost one?

One run on a fresh Compose stack (``make down`` first), in four phases:

1. **First replay.** The committed sample (50,000 events, event times of
   2026-10-16) through ``tributary produce``; wait until all 50,000 have landed
   and the stream has settled.
2. **Second replay.** The same sample again. Every event is a copy of one that
   landed, so nothing can be lost; the dedup operator drops the copies above the
   watermark from its state and the watermark drops the rest as late, which
   counts them in ``rows_dropped_by_watermark``. The prediction
   (:func:`predict`) is committed before the run.
3. **Load.** 50,000 benchmark events at 200 events/s, timed by the wall clock
   (:mod:`bench.load`), of which :data:`LATE_SEQS` (100) are stamped
   :data:`LATE_BEHIND_S` behind their send time (15 minutes behind the
   watermark, which trails the newest event by 10), one (:data:`MALFORMED_SEQ`)
   is not JSON and one (:data:`FUTURE_SEQ`) is stamped an hour ahead. In arm
   ``after`` a reconciliation pass (``tributary stream reconcile``) runs every
   :data:`PASS_EVERY_S` during the load, and one more once it has landed.
4. **Read-out.** Per arm: rows dropped by the watermark, events missing from
   PostgreSQL (sample and benchmark ids alike), every pass's verdict and the
   ids it reported missing, the ``stream_loss`` checks, ``dbt build`` and the
   recovered events in ``marts.fct_edits``, the provenance line of one mart
   answer, and the per-hour PostgreSQL-against-Iceberg reconciliation.

Arm ``before`` runs no pass: what the platform did before M12 (with no pass on
record the check fails on the drop counter alone, exactly as it did).

``predict`` writes ``prediction.json`` from the sample and this module's
constants alone, before any run. Raw output under ``bench/results/m12-late/``.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from tributary.config import DEFAULT_SAMPLE
from tributary.events import event_time
from tributary.ingest.sample import read_sample

RESULTS = Path(__file__).resolve().parent / "results" / "m12-late"

DELAY_S = 600
"""The stream job's default watermark delay (``TRIBUTARY_WATERMARK_DELAY``)."""
EVENTS = 50_000
RATE = 200.0
LATE_SEQS = tuple(range(10_000, 50_000, 400))
"""100 benchmark events, spread over the last four fifths of the load."""
LATE_BEHIND_S = DELAY_S + 900
"""Stamped this far behind their send time: 15 minutes behind the watermark."""
MALFORMED_SEQ = 20_200
FUTURE_SEQ = 30_200
FUTURE_AHEAD_S = 3_600
PASS_EVERY_S = 30.0
PASS_LIMIT_S = 60.0
"""The hypothesis for one pass over the whole topic."""


def predict_second_replay(times: Sequence[int], delay_s: int = DELAY_S) -> dict[str, int]:
    """Rows the second replay loses to the watermark, from the sample's event times.

    After the first replay has settled, the watermark is the newest event time
    minus the delay, and the second replay's copies cannot move it (the same
    times). Spark drops a row as late when its event time is at or below the
    watermark; the rest of the copies are still keys in the dedup state (a key
    stays until the watermark passes its event time plus the delay) and are
    dropped there, which the drop counter does not count.
    """
    watermark = max(times) - delay_s
    dropped = sum(1 for t in times if t <= watermark)
    return {
        "events": len(times),
        "watermark": watermark,
        "dropped_by_watermark": dropped,
        "caught_by_dedup_state": len(times) - dropped,
        "lost": 0,
    }


def prediction(sample: Path = DEFAULT_SAMPLE) -> dict[str, Any]:
    """Everything the run is expected to show, before it runs."""
    times = [int(event_time(e).timestamp()) for e in read_sample(sample)]
    second = predict_second_replay(times)
    return {
        "made_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "rule": (
            "after the first replay settles the watermark is max(event_ts) - delay; the second"
            " replay drops a copy as late when event_ts <= watermark, and the dedup state"
            " catches every other copy"
        ),
        "sample": {
            "events": len(times),
            "first": datetime.fromtimestamp(min(times), UTC).isoformat(),
            "last": datetime.fromtimestamp(max(times), UTC).isoformat(),
        },
        "delay_s": DELAY_S,
        "second_replay": {
            **second,
            "watermark": datetime.fromtimestamp(second["watermark"], UTC).isoformat(),
            "check_before": "fail",
            "check_after": "pass",
        },
        "load": {
            "events": EVENTS,
            "rate": RATE,
            "late": {"seqs": len(LATE_SEQS), "behind_send_s": LATE_BEHIND_S},
            "malformed_seq": MALFORMED_SEQ,
            "future": {"seq": FUTURE_SEQ, "ahead_s": FUTURE_AHEAD_S},
        },
        "hypotheses": {
            "in_flight_reported_missing": 0,
            "rejected_counted_not_landed": {"malformed": 1, "future": 1},
            "late_missing_before": len(LATE_SEQS),
            "late_missing_after": 0,
            "pass_over_topic_s_under": PASS_LIMIT_S,
        },
    }


def main(argv: Sequence[str] | None = None) -> int:
    """``python -m bench.late predict``."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    pred = sub.add_parser("predict", help="write prediction.json from the sample alone")
    pred.add_argument("--out", type=Path, default=RESULTS)
    args = parser.parse_args(argv)
    if args.command == "predict":
        doc = prediction()
        args.out.mkdir(parents=True, exist_ok=True)
        (args.out / "prediction.json").write_text(json.dumps(doc, indent=1) + "\n")
        print(json.dumps(doc["second_replay"], indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
