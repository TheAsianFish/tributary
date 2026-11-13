"""The ``tributary`` command line."""

from __future__ import annotations

import itertools
import json
from pathlib import Path
from typing import TYPE_CHECKING, Annotated

import structlog
import typer

from tributary import __version__
from tributary.config import Settings, get_settings
from tributary.logging import bind_run_id, configure_logging

if TYPE_CHECKING:
    from tributary.agent.service import Agent

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
bench_app = typer.Typer(
    no_args_is_help=True, help="Generate the query-engine benchmark's data (make bench-query)."
)
app.add_typer(bench_app, name="bench")

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


@warehouse_app.command("check-freshness")
def warehouse_check_freshness() -> None:
    """Only the dbt freshness tests (results to ops.quality_runs); exit 1 if one fails.

    What the raw_recentchange DAG runs when no rows arrived, without a rebuild.
    """
    from tributary.warehouse.dbt import DbtCrashError
    from tributary.warehouse.refresh import WarehouseError, check_freshness

    run_id = bind_run_id("warehouse-check-freshness")
    try:
        checks = check_freshness(get_settings(), run_id)
    except WarehouseError as exc:  # failed checks: recorded, see ops.quality_runs
        typer.echo(f"run {run_id}: {exc}", err=True)
        raise typer.Exit(1) from None
    except DbtCrashError as exc:
        typer.echo(f"run {run_id}: {exc}", err=True)
        raise typer.Exit(2) from None
    typer.echo(f"run {run_id}: {len(checks)} freshness check(s) passed")


@warehouse_app.command("check-stream")
def warehouse_check_stream() -> None:
    """The stream_loss checks (results to ops.quality_runs); exit 1 if one fails.

    Rows dropped by the watermark and records rejected by the parser, summed over
    the batches of the last TRIBUTARY_STREAM_CHECK_WINDOW_HOURS.
    """
    from tributary.warehouse.stream_health import StreamLossError, check_stream, summary

    run_id = bind_run_id("warehouse-check-stream")
    try:
        checks = check_stream(get_settings(), run_id)
    except StreamLossError as exc:  # recorded, see ops.quality_runs
        typer.echo(f"run {run_id}: {exc}", err=True)
        raise typer.Exit(1) from None
    for line in summary(checks):
        typer.echo(json.dumps(line, default=str))
    typer.echo(f"run {run_id}: {len(checks)} stream_loss check(s) passed")


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


@bench_app.command("load")
def bench_load(
    rows: Annotated[int, typer.Option("--rows", min=1, help="Rows to generate, e.g. 1000000.")],
    targets: Annotated[
        str,
        typer.Option(help="Comma-separated stores to write: postgres, iceberg, stream."),
    ] = "postgres,iceberg,stream",
) -> None:
    """Scale the sample to N rows and write bench.events_<N> to PostgreSQL and Iceberg.

    Runs Spark in batch mode (in the stream image: `docker compose run --rm stream
    bench load --rows 1000000`). Prints a JSON summary of what it wrote on stdout.
    """
    from tributary.querybench.generate import load

    bind_run_id("bench-load")
    chosen = [t.strip() for t in targets.split(",") if t.strip()]
    try:
        report = load(get_settings(), rows, targets=chosen)
    except ValueError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(2) from None
    typer.echo(report.to_json())


@warehouse_app.command("grant-agent")
def warehouse_grant_agent() -> None:
    """Create the agent's role (marts and ops, read-only) on the configured database."""
    from tributary.agent.roles import ensure_agent_role

    bind_run_id("warehouse-grant-agent")
    settings = get_settings()
    ensure_agent_role(
        settings.pg_conninfo, settings.agent_user, settings.agent_password.get_secret_value()
    )
    typer.echo(f"role {settings.agent_user}: SELECT on marts and ops, read-only sessions")


ProviderName = Annotated[
    str | None,
    typer.Option(help="anthropic, ollama or fake (default TRIBUTARY_PROVIDER)."),
]
ApiUrl = Annotated[
    str | None,
    typer.Option("--api", help="Ask a running Tributary API instead of the local warehouse."),
]
ApiKey = Annotated[
    str | None,
    typer.Option(envvar="TRIBUTARY_API_KEY", help="Key for --api (X-API-Key)."),
]
AsJson = Annotated[bool, typer.Option("--json", help="Print the full answer as JSON.")]


def _remote(api: str, path: str, body: dict[str, str], key: str | None) -> dict[str, object]:
    import httpx

    headers = {"X-API-Key": key} if key else {}
    try:
        response = httpx.post(api.rstrip("/") + path, json=body, headers=headers, timeout=300)
    except httpx.HTTPError as exc:
        typer.echo(f"cannot reach {api}: {exc}", err=True)
        raise typer.Exit(2) from None
    try:
        document: dict[str, object] = response.json()
    except ValueError:
        typer.echo(f"{response.status_code} from {api}: {response.text[:200]}", err=True)
        raise typer.Exit(2) from None
    if response.status_code >= 400 and "status" not in document:
        typer.echo(f"{response.status_code}: {document.get('detail')}", err=True)
        raise typer.Exit(2)
    return document


PROVIDERS = ("anthropic", "ollama", "fake")


def _local_agent(provider: str | None) -> Agent:
    from tributary.agent.service import build_agent

    settings = get_settings()
    if provider and provider not in PROVIDERS:
        typer.echo(f"error: --provider must be one of {', '.join(PROVIDERS)}", err=True)
        raise typer.Exit(2)
    if provider:
        settings = settings.model_copy(update={"provider": provider})
    return build_agent(settings)


def _print_answer(document: dict[str, object], as_json: bool) -> None:
    import json

    from tributary.agent.render import render_answer

    if as_json:
        typer.echo(json.dumps(document, indent=2, ensure_ascii=False))
    else:
        typer.echo(render_answer(document))
    if document.get("status") not in ("answered", "unanswerable"):
        raise typer.Exit(1)


@app.command()
def ask(
    question: Annotated[str, typer.Argument(help="A question about the warehouse.")],
    api: ApiUrl = None,
    api_key: ApiKey = None,
    provider: ProviderName = None,
    as_json: AsJson = False,
) -> None:
    """Answer a question with guarded SQL; prints the answer, the SQL and its provenance."""
    from tributary.agent.execute import ExecutionError
    from tributary.agent.provider import ProviderError

    request_id = bind_run_id("ask")
    if api:
        document = _remote(api, "/query", {"question": question}, api_key)
    else:
        agent = _local_agent(provider)
        try:
            document = agent.ask(question, request_id=request_id).to_dict()
        except (ProviderError, FileNotFoundError) as exc:
            typer.echo(f"error: {exc}", err=True)
            raise typer.Exit(2) from None
        except ExecutionError as exc:  # unreachable warehouse or a missing privilege
            typer.echo(f"error: {exc.feedback()}", err=True)
            raise typer.Exit(2) from None
    _print_answer(document, as_json)


@app.command("sql")
def sql_command(
    query: Annotated[str, typer.Argument(help="One SELECT statement.")],
    api: ApiUrl = None,
    api_key: ApiKey = None,
    as_json: AsJson = False,
) -> None:
    """Run SQL through the agent's guard, role and limits (no model)."""
    from tributary.agent.execute import ExecutionError

    request_id = bind_run_id("sql")
    if api:
        document = _remote(api, "/sql", {"sql": query}, api_key)
    else:
        agent = _local_agent(None)
        try:
            document = agent.sql(query, request_id=request_id).to_dict()
        except FileNotFoundError as exc:
            typer.echo(f"error: {exc}", err=True)
            raise typer.Exit(2) from None
        except ExecutionError as exc:  # unreachable warehouse or a missing privilege
            typer.echo(f"error: {exc.feedback()}", err=True)
            raise typer.Exit(2) from None
    _print_answer(document, as_json)


@app.command("catalog")
def catalog_command(
    as_json: AsJson = False,
    relation: Annotated[
        list[str] | None, typer.Option(help="Only these relations, e.g. marts.fct_edits.")
    ] = None,
) -> None:
    """Print the schema the agent sees: DDL with descriptions, keys and indexed values."""
    import json

    from tributary.agent.catalog import load_dbt

    bind_run_id("catalog")
    settings = get_settings()
    try:
        catalog = load_dbt(settings.catalog_dir, settings.agent_schemas)
    except FileNotFoundError as exc:
        typer.echo(f"error: {exc}; run `tributary warehouse refresh`", err=True)
        raise typer.Exit(2) from None
    if as_json:
        typer.echo(json.dumps(catalog.to_dict(), indent=2, ensure_ascii=False))
        return
    try:
        typer.echo(catalog.render(relation or None))
    except KeyError as exc:
        typer.echo(f"error: {exc.args[0]}", err=True)
        raise typer.Exit(2) from None


@app.command()
def serve(
    host: Annotated[
        str | None, typer.Option(help="Bind address (default TRIBUTARY_API_HOST).")
    ] = None,
    port: Annotated[int | None, typer.Option(help="Port (default TRIBUTARY_API_PORT).")] = None,
) -> None:
    """Serve the HTTP API (needs the `api` extra and TRIBUTARY_API_KEYS)."""
    import uvicorn

    settings = get_settings()
    uvicorn.run(
        "tributary.api.app:app_from_settings",
        factory=True,
        host=host or settings.api_host,
        port=port or settings.api_port,
        log_config=None,
        access_log=False,
    )
