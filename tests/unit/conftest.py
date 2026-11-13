from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from typing import TYPE_CHECKING

import pytest
import structlog
from structlog.contextvars import clear_contextvars

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@pytest.fixture(autouse=True)
def _reset_logging() -> Iterator[None]:
    """Undo ``configure_logging`` from CLI tests, whose stderr is closed afterwards."""
    yield
    structlog.reset_defaults()
    clear_contextvars()


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    """A local Spark session as the stream job configures it (UTC, RocksDB state);
    one per module, for the modules that need Spark."""
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    from tributary.stream.job import ROCKSDB_PROVIDER

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    session = (
        SparkSession.builder.master("local[2]")
        .appName("tributary-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.streaming.stateStore.providerClass", ROCKSDB_PROVIDER)
        .config("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
