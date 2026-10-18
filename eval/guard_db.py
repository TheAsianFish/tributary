"""What the database layer alone stops: the unsafe suite run with the guard switched off.

    uv run python -m eval.guard_db   (needs PostgreSQL with the agent's role)

The guard is meant to be the first of two layers. This runs every statement
of ``eval/guard/unsafe.yaml`` straight through
:class:`~tributary.agent.execute.PostgresExecutor` as the agent's role, with
the executor's settings (read-only transaction, statement timeout, empty
``search_path``, cost ceiling) but no guard, and records what PostgreSQL did:
refused it (and how), or ran it. A statement that ran is not necessarily
harmful (``SELECT version()`` leaks a string, nothing more), but each one is a
case where the guard is the only thing in the way, which is the point of
measuring it.

Run it against a disposable database: the statements are attacks.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import Counter
from collections.abc import Sequence
from pathlib import Path

from eval.guard_eval import UNSAFE_PATH, load_unsafe
from tributary.agent.execute import ExecutionError, PostgresExecutor
from tributary.config import REPO_ROOT, get_settings

RESULT_PATH = REPO_ROOT / "eval" / "results" / "guard-db.json"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--unsafe", type=Path, default=UNSAFE_PATH)
    parser.add_argument("--timeout-ms", type=int, default=1000)
    parser.add_argument("--output", type=Path, default=RESULT_PATH)
    args = parser.parse_args(argv)
    logging.getLogger("sqlglot").setLevel(logging.ERROR)

    settings = get_settings()
    executor = PostgresExecutor(
        settings.agent_conninfo,
        statement_timeout_ms=args.timeout_ms,
        row_cap=settings.agent_row_cap,
        cost_ceiling=settings.agent_cost_ceiling,
    )
    items = []
    for item in load_unsafe(args.unsafe):
        outcome: str
        try:
            result = executor.run(item.sql)
        except ExecutionError as exc:
            outcome, detail = exc.kind, exc.message
        else:
            outcome, detail = "ran", f"{len(result.rows)} row(s)"
        items.append(
            {"id": item.id, "category": item.category, "outcome": outcome, "detail": detail}
        )
    ran = [i for i in items if i["outcome"] == "ran"]
    report = {
        "role": settings.agent_user,
        "statement_timeout_ms": args.timeout_ms,
        "total": len(items),
        "refused": len(items) - len(ran),
        "by_outcome": dict(sorted(Counter(i["outcome"] for i in items).items())),
        "ran": sorted(i["id"] for i in ran),
        "items": sorted(items, key=lambda i: i["id"]),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(f"database alone refused {report['refused']}/{report['total']}: {report['by_outcome']}")
    for i in ran:
        print(f"  ran: {i['id']} ({i['category']}): {i['detail']}")
    print(f"wrote {args.output.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
