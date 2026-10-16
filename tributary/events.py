"""The ``recentchange`` event: which fields are kept, and its flat warehouse row.

Both the committed sample and ``--live`` mode publish *trimmed* events: an
allowlist of factual fields. Free text written by editors (``comment``,
``parsedcomment``, ``log_action_comment``, ``log_params``) never enters Kafka.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import astuple, dataclass
from datetime import UTC, datetime
from typing import Any

# JSON values from the stream are heterogeneous; callers narrow them per field.
Event = dict[str, Any]

TOP_LEVEL_FIELDS = (
    "id",
    "type",
    "namespace",
    "title",
    "timestamp",
    "user",
    "bot",
    "minor",
    "patrolled",
    "length",
    "revision",
    "server_name",
    "wiki",
    "log_id",
    "log_type",
    "log_action",
)
META_FIELDS = ("id", "dt", "domain", "stream")

UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")

# Column order of raw.recentchange as written by the sink (ingested_at is defaulted).
COLUMNS = (
    "event_id",
    "event_ts",
    "emitted_at",
    "rc_id",
    "wiki",
    "domain",
    "event_type",
    "namespace",
    "title",
    "user_name",
    "is_bot",
    "is_minor",
    "is_patrolled",
    "length_old",
    "length_new",
    "rev_old",
    "rev_new",
    "log_type",
    "log_action",
    "kafka_partition",
    "kafka_offset",
    "kafka_ts",
)


class InvalidEventError(ValueError):
    """The payload is not a usable recentchange event."""


def trim_event(raw: Mapping[str, Any]) -> Event:
    """Keep the allowlisted fields of a raw stream event, dropping all free text."""
    meta = raw.get("meta")
    if not isinstance(meta, Mapping):
        raise InvalidEventError("event has no meta object")
    out: Event = {"meta": {k: meta[k] for k in META_FIELDS if k in meta}}
    for key in TOP_LEVEL_FIELDS:
        if key in raw:
            out[key] = raw[key]
    return out


def is_canary(event: Mapping[str, Any]) -> bool:
    """Wikimedia injects canary events (domain ``canary``) to monitor the stream."""
    meta = event.get("meta")
    return isinstance(meta, Mapping) and meta.get("domain") == "canary"


def event_id(event: Mapping[str, Any]) -> str:
    """The event's UUID (``meta.id``): the Kafka key and the warehouse primary key."""
    meta = event.get("meta")
    value = meta.get("id") if isinstance(meta, Mapping) else None
    if not isinstance(value, str) or not UUID_RE.match(value):
        raise InvalidEventError(f"meta.id is not a lowercase UUID: {value!r}")
    return value


def event_time(event: Mapping[str, Any]) -> datetime:
    """When the change happened in MediaWiki (``timestamp``, unix seconds, UTC).

    ``meta.dt`` is when the event was emitted, which trails the change by up to
    tens of seconds; the warehouse keeps it as ``emitted_at``.
    """
    ts = event.get("timestamp")
    if not isinstance(ts, int) or isinstance(ts, bool):
        raise InvalidEventError(f"timestamp is not an integer: {ts!r}")
    return datetime.fromtimestamp(ts, tz=UTC)


@dataclass(frozen=True, slots=True)
class RawRow:
    """One row of ``raw.recentchange``. Mirrors the Spark parser in ``stream/parse.py``."""

    event_id: str
    event_ts: datetime
    emitted_at: datetime | None
    rc_id: int | None
    wiki: str
    domain: str | None
    event_type: str
    namespace: int | None
    title: str | None
    user_name: str | None
    is_bot: bool | None
    is_minor: bool | None
    is_patrolled: bool | None
    length_old: int | None
    length_new: int | None
    rev_old: int | None
    rev_new: int | None
    log_type: str | None
    log_action: str | None
    kafka_partition: int
    kafka_offset: int
    kafka_ts: datetime

    def as_tuple(self) -> tuple[object, ...]:
        return astuple(self)


def _opt_int(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _opt_bool(value: object) -> bool | None:
    return value if isinstance(value, bool) else None


def _opt_str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _obj(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _parse_dt(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def to_row(
    event: Mapping[str, Any], *, partition: int = 0, offset: int = 0, kafka_ts: datetime
) -> RawRow:
    """Flatten a trimmed event into a warehouse row (raises InvalidEventError)."""
    meta = _obj(event.get("meta"))
    wiki, event_type = event.get("wiki"), event.get("type")
    if not isinstance(wiki, str) or not isinstance(event_type, str):
        raise InvalidEventError("wiki and type are required strings")
    length, revision = _obj(event.get("length")), _obj(event.get("revision"))
    return RawRow(
        event_id=event_id(event),
        event_ts=event_time(event),
        emitted_at=_parse_dt(meta.get("dt")),
        rc_id=_opt_int(event.get("id")),
        wiki=wiki,
        domain=_opt_str(meta.get("domain")),
        event_type=event_type,
        namespace=_opt_int(event.get("namespace")),
        title=_opt_str(event.get("title")),
        user_name=_opt_str(event.get("user")),
        is_bot=_opt_bool(event.get("bot")),
        is_minor=_opt_bool(event.get("minor")),
        is_patrolled=_opt_bool(event.get("patrolled")),
        length_old=_opt_int(length.get("old")),
        length_new=_opt_int(length.get("new")),
        rev_old=_opt_int(revision.get("old")),
        rev_new=_opt_int(revision.get("new")),
        log_type=_opt_str(event.get("log_type")),
        log_action=_opt_str(event.get("log_action")),
        kafka_partition=partition,
        kafka_offset=offset,
        kafka_ts=kafka_ts,
    )
