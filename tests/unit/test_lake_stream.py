"""The lake query's table definition, catalog settings and the two-query supervisor (no JVM)."""

from __future__ import annotations

import threading

import pytest

pytest.importorskip("pyspark")

from pyspark.errors import StreamingQueryException
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from tributary.config import Settings
from tributary.stream import job
from tributary.stream.lake import TABLE, catalog_conf, create_table_sql


def test_table_is_partitioned_by_hour_of_event_time_with_columns_from_the_stream() -> None:
    schema = StructType(
        [
            StructField("event_id", StringType()),
            StructField("event_ts", TimestampType()),
            StructField("rc_id", LongType()),
        ]
    )
    sql = create_table_sql(schema)
    assert sql.startswith(f"CREATE TABLE IF NOT EXISTS {TABLE} (")
    assert "`event_id` string,\n    `event_ts` timestamp,\n    `rc_id` bigint\n)" in sql
    assert "PARTITIONED BY (hours(event_ts))" in sql
    assert "'format-version' = '2'" in sql
    assert "'write.spark.fanout.enabled' = 'true'" in sql
    assert "'write.metadata.delete-after-commit.enabled' = 'true'" in sql


def test_catalog_is_jdbc_in_postgres_with_s3_file_io(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRIBUTARY_PG_HOST", "postgres")
    monkeypatch.setenv("TRIBUTARY_S3_ENDPOINT", "http://minio:9000")
    conf = catalog_conf(Settings(_env_file=None))
    assert conf["spark.sql.catalog.lake"] == "org.apache.iceberg.spark.SparkCatalog"
    assert conf["spark.sql.catalog.lake.type"] == "jdbc"
    assert conf["spark.sql.catalog.lake.uri"].startswith("jdbc:postgresql://postgres:5432/")
    assert conf["spark.sql.catalog.lake.io-impl"] == "org.apache.iceberg.aws.s3.S3FileIO"
    assert conf["spark.sql.catalog.lake.s3.endpoint"] == "http://minio:9000"
    # MinIO serves buckets by path, not by virtual-host DNS names.
    assert conf["spark.sql.catalog.lake.s3.path-style-access"] == "true"
    assert conf["spark.sql.catalog.lake.warehouse"] == "s3://lake/warehouse"


class FakeQuery:
    """Enough of StreamingQuery for the supervisor: active until stopped or failed."""

    def __init__(
        self, fail_after: int | None = None, fail_silently_after: int | None = None
    ) -> None:
        self.active = True
        self.stopped = False
        self.polls = 0
        self.error: StreamingQueryException | None = None
        self._fail_after = fail_after
        self._fail_silently_after = fail_silently_after

    @property
    def isActive(self) -> bool:  # noqa: N802 - mirrors pyspark's name
        return self.active

    def awaitTermination(self, timeout: int | None = None) -> bool:  # noqa: N802
        self.polls += 1
        if self._fail_after is not None and self.polls > self._fail_after:
            self.active = False
            self.error = StreamingQueryException(message="sink failed")
            raise self.error
        if self._fail_silently_after is not None and self.polls > self._fail_silently_after:
            # The wait timed out, then the query failed before anyone looked again.
            self.active = False
            self.error = StreamingQueryException(message="failed between polls")
            return False
        return not self.active

    def exception(self) -> StreamingQueryException | None:
        return self.error

    def stop(self) -> None:
        self.active = False
        self.stopped = True


def test_one_failing_query_stops_the_other_and_fails_the_process() -> None:
    healthy, failing = FakeQuery(), FakeQuery(fail_after=2)
    with pytest.raises(StreamingQueryException):
        job.supervise([healthy, failing], threading.Event())  # type: ignore[list-item]
    assert healthy.stopped
    assert not healthy.active


def test_a_failure_between_polls_still_fails_the_process() -> None:
    finished, failing = FakeQuery(), FakeQuery(fail_silently_after=1)
    finished.active = False
    with pytest.raises(StreamingQueryException, match="between polls"):
        job.supervise([finished, failing], threading.Event())  # type: ignore[list-item]


def test_a_stop_request_stops_every_query_without_error() -> None:
    queries = [FakeQuery(), FakeQuery()]
    stop = threading.Event()
    stop.set()
    job.supervise(queries, stop)  # type: ignore[arg-type]
    assert all(q.stopped for q in queries)


def test_queries_that_finish_on_their_own_end_the_supervisor() -> None:
    queries = [FakeQuery(), FakeQuery()]
    for q in queries:
        q.active = False
    job.supervise(queries, threading.Event())  # type: ignore[arg-type]
    assert not any(q.stopped for q in queries)
