"""The skew benchmark's replay and its prediction (pure; no Spark)."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from bench import skew
from bench.skew import Record, accepted, poisoned_batches, predict


def rec(n: int, ts: int, kafka_s: float) -> Record:
    payload = json.dumps({"meta": {"id": f"00000000-0000-4000-8000-{n:012d}"}, "timestamp": ts})
    return Record(f"00000000-0000-4000-8000-{n:012d}", ts, round(kafka_s * 1000), payload)


# The scenario of test_stream_spark.py's poison tests, which Spark ran: six batches 30 s
# apart, four real events each (one 100 s late), Kafka time 2 s after event time.
T0 = 1_792_194_000


def spark_scenario() -> list[Record]:
    out = []
    for b in range(6):
        base = T0 + 30 * b
        for i, ts in enumerate([base, base + 1, base - 100, base + 2]):
            out.append(rec(10 * b + i, ts, ts + 2))
    return out


def test_the_prediction_matches_what_spark_did_in_the_unit_test() -> None:
    records = spark_scenario()
    # The unit test's poison sits third in batch 1, stamped 82,800 s past its record time.
    batches = poisoned_batches(records, 82_800, at=6, size=4)
    assert [len(b) for b in batches] == [4, 5, 4, 4, 4, 4]
    poison = batches[1][2]
    assert poison.poison
    assert poison.event_id == skew.POISON_ID
    assert poison.event_ts == poison.kafka_s + 82_800

    before = predict(batches, 86_400)
    assert before.poison_accepted
    assert before.lost_by_batch == {3: 4, 4: 4, 5: 4}  # what Spark reported
    assert before.lost == before.lost_strict == 12
    # PLAN.md's first estimate also counts batch 2, which the two-batch lag spares.
    assert before.plan_formula == 16

    after = predict(batches, 300)
    assert not after.poison_accepted
    assert (after.lost, after.real_rejected) == (0, 0)


def test_the_poison_copies_its_model_with_its_own_id() -> None:
    records = spark_scenario()
    [poison] = [r for b in poisoned_batches(records, 450, at=9, size=4) for r in b if r.poison]
    model = records[9]
    assert json.loads(poison.payload)["meta"]["id"] == skew.POISON_ID
    assert json.loads(poison.payload)["timestamp"] == model.kafka_s + 450
    assert poison.kafka_ms == model.kafka_ms
    assert json.loads(model.payload)["meta"]["id"] != skew.POISON_ID  # the model is untouched


def test_no_poison_means_plain_fixed_batches() -> None:
    batches = poisoned_batches(spark_scenario(), None, size=5)
    assert [len(b) for b in batches] == [5, 5, 5, 5, 4]
    assert predict(batches, 300).lost == 0


@pytest.mark.parametrize(("ts", "kafka_s", "ok"), [(1300, 1000.9, True), (1301, 1000.9, False)])
def test_acceptance_truncates_the_record_time_to_whole_seconds(
    ts: int, kafka_s: float, ok: bool
) -> None:
    assert accepted(rec(1, ts, kafka_s), 300) is ok


def test_ties_on_the_threshold_are_dropped() -> None:
    # Batch 2's late threshold is batch 0's newest event minus the delay: 1000 - 600.
    batches = [[rec(1, 1000, 1000)], [rec(2, 1000, 1000)], [rec(3, 400, 1000), rec(4, 401, 1000)]]
    p = predict(batches, 300, delay_s=600)
    assert (p.lost, p.lost_strict) == (1, 0)
    assert p.lost_ids == [rec(3, 0, 0).event_id]


def test_first_lossy_skew_on_a_toy_stream() -> None:
    # Batches of two, 10 s apart, one event 50 s late in each. A poison X s ahead of
    # batch 1's first event (10,010) makes batch 3's threshold 10,010 + X - 600, and
    # batch 3's late event (9,980) is at or below it from X = 570.
    records = []
    for b in range(6):
        base = 10_000 + 10 * b
        records += [rec(2 * b, base, base), rec(2 * b + 1, base - 50, base)]
    assert skew.first_lossy_skew(records, 86_400, limit=700, at=2, size=2) == 570
    assert skew.first_lossy_skew(records, 300, limit=700, at=2, size=2) is None


def test_the_arms_use_the_old_and_the_current_bound() -> None:
    assert skew.arm_bound("before") == 86_400
    assert skew.arm_bound("after") == 300


def test_the_incident_replays_put_the_tests_ahead_of_their_record_time() -> None:
    sample = [
        {
            "meta": {"id": f"00000000-0000-4000-8000-{i:012d}", "dt": "2026-10-16T23:45:36Z"},
            "timestamp": 1_792_194_336 + i,
            "wiki": "enwiki",
            "type": "edit",
        }
        for i in range(skew.TEST_EVENTS)
    ]
    phases = skew.incident_phases(sample)
    assert [p.name for p in phases] == [
        "fault run 1",
        "test_lakehouse",
        "test_pipeline",
        "fault run 2",
    ]
    run1, lake, pipe, run2 = phases
    # A fault run stamps event time at the send time: never ahead of the record time.
    assert all(r.event_ts <= r.kafka_s for r in run1.records)
    # The lakehouse replay starts after run 1's newest event, 2 s apart: 2.8 h of event time
    # sent in one go, so all but its first events are ahead of the clock that sent them.
    assert lake.records[0].event_ts == max(r.event_ts for r in run1.records) + 1
    assert lake.records[-1].event_ts - lake.records[0].event_ts == 2 * (skew.TEST_EVENTS - 1)
    assert max(r.event_ts - r.kafka_s for r in lake.records) > 2.7 * 3600
    # The pipeline replay starts after that, and its sentinel is newer still.
    assert min(r.event_ts for r in pipe.records) == lake.records[-1].event_ts + 1
    assert pipe.records[-1].event_ts == max(r.event_ts for r in pipe.records)
    assert run2.sent_at > pipe.sent_at
    assert datetime.fromtimestamp(run2.records[0].kafka_s, UTC) == run2.sent_at
