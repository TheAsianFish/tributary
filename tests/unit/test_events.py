from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from tributary.events import (
    COLUMNS,
    DEFAULT_FUTURE_SKEW_S,
    REJECT_REASONS,
    FutureEventError,
    InvalidEventError,
    RawRow,
    default_future_skew_s,
    event_id,
    event_time,
    is_canary,
    to_row,
    trim_event,
)

# The record is appended just after the edit (23:34:15), as it would be live.
KAFKA_TS = datetime(2026, 10, 16, 23, 35, tzinfo=UTC)


def raw_edit(**overrides: Any) -> dict[str, Any]:
    event: dict[str, Any] = {
        "$schema": "/mediawiki/recentchange/1.0.0",
        "meta": {
            "uri": "https://ku.wiktionary.org/wiki/x",
            "request_id": "095d3066-eba7-46ba-9f93-bd500ef32e37",
            "id": "26cc4453-4f29-4e7f-bd43-922efe25a82f",
            "domain": "ku.wiktionary.org",
            "stream": "mediawiki.recentchange",
            "dt": "2026-10-16T23:34:16.644Z",
            "offset": 6615667819,
        },
        "id": 13462683,
        "type": "edit",
        "namespace": 0,
        "title": "top bûn",
        "comment": "free text an editor wrote",
        "parsedcomment": "<span>free text</span>",
        "timestamp": 1792193655,
        "user": "Balyozbot",
        "bot": True,
        "minor": True,
        "length": {"old": 347, "new": 333},
        "revision": {"old": 6654840, "new": 6802263},
        "server_name": "ku.wiktionary.org",
        "wiki": "kuwiktionary",
    }
    event.update(overrides)
    return event


def test_trim_drops_free_text_and_urls() -> None:
    trimmed = trim_event(
        raw_edit(log_action_comment="x", log_params={"a": 1}, notify_url="https://x")
    )
    for dropped in ("comment", "parsedcomment", "log_action_comment", "log_params", "$schema"):
        assert dropped not in trimmed
    assert "notify_url" not in trimmed
    assert set(trimmed["meta"]) == {"id", "dt", "domain", "stream"}
    assert trimmed["title"] == "top bûn"


def test_trim_requires_meta() -> None:
    with pytest.raises(InvalidEventError):
        trim_event({"type": "edit"})


def test_canary_detection() -> None:
    assert is_canary({"meta": {"domain": "canary"}})
    assert not is_canary(raw_edit())


@pytest.mark.parametrize("bad", [None, "", "not-a-uuid", "26CC4453-4F29-4E7F-BD43-922EFE25A82F"])
def test_event_id_must_be_a_lowercase_uuid(bad: object) -> None:
    with pytest.raises(InvalidEventError):
        event_id({"meta": {"id": bad}})


def test_event_time_is_the_mediawiki_timestamp_not_meta_dt() -> None:
    assert event_time(raw_edit()) == datetime(2026, 10, 16, 23, 34, 15, tzinfo=UTC)
    with pytest.raises(InvalidEventError):
        event_time(raw_edit(timestamp=True))


@pytest.mark.parametrize("ts", [-1, 253_402_300_800, 10**20])
def test_event_time_rejects_unrepresentable_timestamps(ts: int) -> None:
    with pytest.raises(InvalidEventError, match="out of range"):
        event_time(raw_edit(timestamp=ts))


def test_to_row_flattens_an_edit() -> None:
    row = to_row(trim_event(raw_edit()), partition=2, offset=7, kafka_ts=KAFKA_TS)
    assert row.event_id == "26cc4453-4f29-4e7f-bd43-922efe25a82f"
    assert row.emitted_at == datetime(2026, 10, 16, 23, 34, 16, 644000, tzinfo=UTC)
    assert (row.length_old, row.length_new, row.rev_old, row.rev_new) == (
        347,
        333,
        6654840,
        6802263,
    )
    assert row.is_patrolled is None
    assert (row.kafka_partition, row.kafka_offset) == (2, 7)


def test_to_row_handles_a_log_event_without_rc_id() -> None:
    event = trim_event(raw_edit(type="log", log_type="abusefilter", log_action="hit"))
    for key in ("id", "length", "revision", "minor"):
        event.pop(key)
    row = to_row(event, kafka_ts=KAFKA_TS)
    assert (row.rc_id, row.length_new, row.is_minor) == (None, None, None)
    assert row.log_type == "abusefilter"


def test_row_fields_match_the_sink_columns() -> None:
    assert tuple(RawRow.__dataclass_fields__) == COLUMNS


def test_to_row_rejects_an_event_time_further_ahead_than_the_skew_bound() -> None:
    kafka_ts = datetime(2026, 10, 16, 23, 0, tzinfo=UTC)
    assert DEFAULT_FUTURE_SKEW_S == 300  # half the default 10-minute watermark delay
    too_far = int(kafka_ts.timestamp()) + DEFAULT_FUTURE_SKEW_S + 1
    event: dict[str, Any] = {
        "meta": {"id": "00000000-0000-4000-8000-000000000001"},
        "timestamp": too_far,
        "wiki": "enwiki",
        "type": "edit",
    }
    with pytest.raises(FutureEventError, match="301 s ahead of the record time") as caught:
        to_row(event, kafka_ts=kafka_ts)
    assert caught.value.reason == "future"
    event["timestamp"] = too_far - 1
    assert to_row(event, kafka_ts=kafka_ts).event_ts == kafka_ts + timedelta(seconds=300)
    # The bound is a parameter: the job passes Settings.future_skew_s.
    event["timestamp"] = int(kafka_ts.timestamp()) + 82_800
    assert to_row(event, kafka_ts=kafka_ts, max_future_skew_s=86_400).event_ts.hour == 22


def test_other_invalid_events_are_counted_as_malformed() -> None:
    with pytest.raises(InvalidEventError) as caught:
        to_row({"meta": {}, "timestamp": 1, "wiki": "enwiki", "type": "edit"}, kafka_ts=KAFKA_TS)
    assert caught.value.reason == "malformed"
    assert REJECT_REASONS == ("malformed", "future")


@pytest.mark.parametrize(("delay_s", "bound"), [(600, 300), (60, 30), (1, 0), (601, 300)])
def test_the_default_bound_is_half_the_watermark_delay(delay_s: int, bound: int) -> None:
    assert default_future_skew_s(delay_s) == bound
