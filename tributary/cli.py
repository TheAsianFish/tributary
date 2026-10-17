"""The ``tributary`` command line."""

from __future__ import annotations

import itertools
from pathlib import Path
from typing import Annotated

import structlog
import typer

from tributary import __version__
from tributary.config import Settings, get_settings
from tributary.logging import bind_run_id, configure_logging

app = typer.Typer(no_args_is_help=True, add_completion=False, help=__doc__)
sample_app = typer.Typer(no_args_is_help=True, help="Capture and describe the committed sample.")
app.add_typer(sample_app, name="sample")
warehouse_app = typer.Typer(
    no_args_is_help=True, help="Build and test the dbt models; publish the agent's catalog."
)
app.add_typer(warehouse_app, name="warehouse")
lake_app = typer.Typer(
    no_args_is_help=True, help="Maintain and check the Iceberg lake through Trino."
)
app.add_typer(lake_app, name="lake")

log = structlog.get_logger(__name__)


@app.callback()
def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_format, settings.log_level)


@app.command()
def version() -> None:
    """Print the installed version."""
    bind_run_id("version")
    typer.echo(__version__)


@app.command()
def produce(
    live: Annotated[
        bool, typer.Option("--live", help="Read the live Wikimedia stream instead of the sample.")
    ] = False,
    limit: Annotated[int | None, typer.Option(min=0, help="Stop after N events.")] = None,
    rate: Annotated[
        float | None,
        typer.Option(min=0.0, help="Events/s for replay; 0 = unthrottled. Ignored with --live."),
    ] = None,
    sample: Annotated[Path | None, typer.Option(help="Sample file to replay.")] = None,
) -> None:
    """Publish recentchange events to Kafka, keyed by event UUID."""
    from tributary.ingest.live import live_events
    from tributary.ingest.producer import ensure_topic, make_producer, produce_events
    from tributary.ingest.sample import read_sample

    bind_run_id("produce")
    settings = get_settings()
    ensure_topic(settings.kafka_bootstrap, settings.topic, settings.topic_partitions)
    if live:
        events = live_events(settings.live_url, settings.user_agent)
        pace = 0.0
    else:
        path = sample or settings.sample_path
        events = read_sample(path)
        pace = settings.produce_rate if rate is None else rate
    log.info("produce_start", source="live" if live else "sample", topic=settings.topic, rate=pace)
    try:
        stats = produce_events(
            events, make_producer(settings.kafka_bootstrap), settings.topic, rate=pace, limit=limit
        )
    except KeyboardInterrupt:
        raise typer.Exit(130) from None
    if stats.failed:
        raise typer.Exit(1)


@app.command()
def stream(
    available_now: Annotated[
        bool,
        typer.Option("--available-now", help="Process what is in Kafka now, then stop."),
    ] = False,
) -> None:
    """Run the Spark job: Kafka → watermark + dedup → raw.recentchange."""
    from tributary.stream.job import run

    bind_run_id("stream")
    run(get_settings(), available_now=available_now)


@sample_app.command("capture")
def sample_capture(
    count: Annotated[int, typer.Option(min=1, help="Events to keep.")] = 50_000,
    out: Annotated[Path | None, typer.Option(help="Output path (gzip JSONL).")] = None,
) -> None:
    """Record trimmed events from the live stream into a sample file."""
    from tributary.ingest.live import DecodeStats, live_events
    from tributary.ingest.sample import write_sample

    bind_run_id("sample-capture")
    settings: Settings = get_settings()
    path = out or settings.sample_path
    decoded = DecodeStats()
    events = itertools.islice(
        live_events(settings.live_url, settings.user_agent, stats=decoded), count
    )
    written = write_sample(path, events)
    log.info(
        "sample_written",
        path=str(path),
        events=written,
        received=decoded.received,
        canary=decoded.canary,
        invalid=decoded.invalid,
    )


@sample_app.command("load")
def sample_load(
    limit: Annotated[int | None, typer.Option(min=0, help="Load at most N events.")] = None,
    path: Annotated[Path | None, typer.Option(help="Sample file to load.")] = None,
) -> None:
    """Insert the sample straight into raw.recentchange (no Kafka, no Spark)."""
    from tributary.ingest.sample import read_sample
    from tributary.warehouse.load import load_events

    bind_run_id("sample-load")
    settings = get_settings()
    offered, inserted, invalid = load_events(
        settings.pg_conninfo, read_sample(path or settings.sample_path, limit=limit)
    )
    typer.echo(f"offered {offered}, inserted {inserted}, invalid {invalid}")


@sample_app.command("stats")
def sample_stats_command(
    path: Annotated[Path | None, typer.Option(help="Sample file to describe.")] = None,
) -> None:
    """Counts, duplicates, emit delay and lateness of a sample, in file order."""
    from tributary.ingest.sample import format_stats, read_sample, sample_stats

    bind_run_id("sample-stats")
    typer.echo(format_stats(sample_stats(read_sample(path or get_settings().sample_path))))


@warehouse_app.command("refresh")
def warehouse_refresh() -> None:
    """dbt seed + run, dbt test (results to ops.quality_runs), then publish the catalog.

    The same steps, in the same order, as the warehouse_refresh DAG; the catalog
    is published only when no check failed.
    """
    from tributary.warehouse.dbt import DbtCrashError
    from tributary.warehouse.refresh import WarehouseError, refresh

    run_id = bind_run_id("warehouse-refresh")
    try:
        summary = refresh(get_settings(), run_id)
    except WarehouseError as exc:  # failed checks or builds: recorded, see ops.*
        typer.echo(f"run {run_id}: {exc}", err=True)
        raise typer.Exit(1) from None
    except DbtCrashError as exc:
        typer.echo(f"run {run_id}: {exc}", err=True)
        raise typer.Exit(2) from None
    typer.echo(
        f"run {run_id}: {len(summary.refreshed)} relations built, "
        f"{len(summary.checks)} checks passed, catalog {summary.catalog}"
    )


@warehouse_app.command("publish-catalog")
def warehouse_publish_catalog(
    max_distinct: Annotated[
        int | None,
        typer.Option(min=1, help="Index the values of text columns with at most N (default 250)."),
    ] = None,
) -> None:
    """dbt docs generate + the value index, published atomically to the catalog dir."""
    from tributary.warehouse.catalog import MAX_DISTINCT
    from tributary.warehouse.refresh import publish_catalog

    run_id = bind_run_id("warehouse-publish-catalog")
    limit = MAX_DISTINCT if max_distinct is None else max_distinct
    path = publish_catalog(get_settings(), run_id, max_distinct=limit)
    typer.echo(str(path))


@lake_app.command("maintain")
def lake_maintain() -> None:
    """Compact closed hours, expire old snapshots, remove orphan files."""
    from tributary.lake.maintenance import (
        expire_snapshots,
        optimize_closed_hours,
        remove_orphan_files,
    )

    bind_run_id("lake-maintain")
    settings = get_settings()
    rewritten = optimize_closed_hours(settings)
    expire_snapshots(settings)
    remove_orphan_files(settings)
    typer.echo(f"rewrote {rewritten} data files")


@lake_app.command("reconcile")
def lake_reconcile() -> None:
    """Compare closed hours between the warehouse and the lake; record in ops.quality_runs."""
    from tributary.lake.reconcile import ReconciliationError, reconcile_closed_hours

    run_id = bind_run_id("lake-reconcile")
    try:
        checks = reconcile_closed_hours(get_settings(), run_id)
    except ReconciliationError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from None
    for check in checks:
        typer.echo(f"{check.check_name}: {check.status} {check.details}")


@lake_app.command("pruning")
def lake_pruning(
    hour: Annotated[str, typer.Argument(help="Hour to filter on, e.g. 2026-10-16T23 (UTC).")],
    plan: Annotated[bool, typer.Option("--plan", help="Print the EXPLAIN ANALYZE plan.")] = False,
) -> None:
    """Check that a one-hour filter reads only that hour's data files."""
    from datetime import UTC, datetime

    from tributary.lake.pruning import check_hour

    bind_run_id("lake-pruning")
    start = datetime.strptime(hour, "%Y-%m-%dT%H").replace(tzinfo=UTC)
    report = check_hour(get_settings(), start)
    typer.echo(
        f"hour {report.hour:%Y-%m-%d %H}:00 UTC: $files says {report.hour_files} files,"
        f" {report.hour_rows} rows (table: {report.table_files} files, {report.table_rows} rows)"
    )
    typer.echo(f"scan read {report.scan_splits} splits, {report.scan_rows} rows")
    typer.echo("pruned: yes" if report.pruned else "pruned: NO")
    if plan:
        typer.echo(report.plan)
    if not report.pruned:
        raise typer.Exit(1)
