"""The agent's PostgreSQL role: it can read the marts and ``ops``, and nothing else.

The guard is the first line; this role is the one that holds if the guard is
wrong. It owns nothing, has ``SELECT`` on the marts (the only relations the
agent answers from) and on ``ops`` (refresh and quality results, for
provenance), and no privileges on ``raw`` or ``staging``, where editor names
are. Its sessions start read-only and carry their own statement timeout, so
even a connection that skips the executor's settings cannot write or run
for long.

Compose creates the role when the database is first initialised
(``docker/postgres/init.sh``); :func:`ensure_agent_role` does the same for any
other PostgreSQL (``tributary warehouse grant-agent``), idempotently. dbt
rebuilds mart tables by creating new ones, so the grant on future tables
(``ALTER DEFAULT PRIVILEGES``) matters as much as the one on existing tables;
it applies to tables created by the role that runs this, which must be the
role dbt runs as.
"""

from __future__ import annotations

from collections.abc import Sequence

import psycopg
import structlog
from psycopg import sql

log = structlog.get_logger(__name__)

AGENT_SCHEMAS: tuple[str, ...] = ("marts", "ops")
SESSION_TIMEOUT = "30s"


def agent_role_statements(
    user: str, password: str, database: str, schemas: Sequence[str] = AGENT_SCHEMAS
) -> list[sql.Composed]:
    """The statements that (re)create the role's privileges; the role must exist."""
    role = sql.Identifier(user)
    names = sql.SQL(", ").join(sql.Identifier(s) for s in schemas)
    return [
        sql.SQL("ALTER ROLE {} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD {}").format(
            role, sql.Literal(password)
        ),
        sql.SQL("ALTER ROLE {} CONNECTION LIMIT 20").format(role),
        sql.SQL("ALTER ROLE {} SET default_transaction_read_only = on").format(role),
        sql.SQL("ALTER ROLE {} SET statement_timeout = {}").format(
            role, sql.Literal(SESSION_TIMEOUT)
        ),
        sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(sql.Identifier(database), role),
        *(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(s)) for s in schemas),
        sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(names, role),
        sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {}").format(names, role),
        sql.SQL("ALTER DEFAULT PRIVILEGES IN SCHEMA {} GRANT SELECT ON TABLES TO {}").format(
            names, role
        ),
    ]


def ensure_agent_role(owner_conninfo: str, user: str, password: str) -> None:
    """Create the agent's role if missing and grant exactly its privileges."""
    with psycopg.connect(owner_conninfo, autocommit=True) as conn:
        database = conn.info.dbname
        exists = conn.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (user,)).fetchone()
        if not exists:
            conn.execute(sql.SQL("CREATE ROLE {} LOGIN").format(sql.Identifier(user)))
        for statement in agent_role_statements(user, password, database):
            conn.execute(statement)
    log.info("agent_role.granted", role=user, schemas=list(AGENT_SCHEMAS), created=not exists)
