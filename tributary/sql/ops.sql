-- Operational metadata: data-quality results and model refreshes. Idempotent.
CREATE SCHEMA IF NOT EXISTS ops;

-- One row per check per orchestrator run: dbt tests (generic, freshness, volume
-- anomaly) and the Postgres-vs-Iceberg reconciliation of each closed hour.
CREATE TABLE IF NOT EXISTS ops.quality_runs (
    id          bigserial   PRIMARY KEY,
    run_id      text        NOT NULL,
    source      text        NOT NULL,
    check_name  text        NOT NULL,
    check_type  text        NOT NULL,
    target      text        NOT NULL,
    status      text        NOT NULL CHECK (status IN ('pass', 'warn', 'fail', 'error', 'skipped')),
    failures    bigint,
    details     jsonb       NOT NULL DEFAULT '{}'::jsonb,
    started_at  timestamptz,
    finished_at timestamptz NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    -- A retried task records the same run again; it replaces, never duplicates.
    UNIQUE (run_id, check_name)
);
CREATE INDEX IF NOT EXISTS quality_runs_target_finished
    ON ops.quality_runs (target, finished_at DESC);

-- One row per dbt model (or seed) per run: when each relation was last rebuilt.
CREATE TABLE IF NOT EXISTS ops.refresh_runs (
    run_id            text        NOT NULL,
    node              text        NOT NULL,
    relation          text        NOT NULL,
    status            text        NOT NULL,
    rows_affected     bigint,
    execution_seconds double precision,
    finished_at       timestamptz NOT NULL,
    recorded_at       timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (run_id, node)
);

-- One row per micro-batch the warehouse query has fully written (every partition
-- committed): the newest event time it holds. Rows of a batch commit partition by
-- partition, so max(event_ts) of the table can run ahead of a batch still being
-- written; checks that need "everything up to T has landed" read this instead.
CREATE TABLE IF NOT EXISTS ops.stream_progress (
    query        text        NOT NULL,
    batch_id     bigint      NOT NULL,
    max_event_ts timestamptz NOT NULL,
    rows         bigint      NOT NULL,
    inserted     bigint      NOT NULL,
    committed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (query, batch_id)
);

-- What each batch lost on the way in, written from the query's progress report (so
-- for the lake query too, and for a batch whose every row was dropped, which the
-- warehouse sink never sees): rows the watermark dropped as late, and records the
-- parser rejected, by reason. A row the progress report wrote alone has no
-- max_event_ts and rows = inserted = 0.
--
-- Gated on the catalog: this script runs every time a check is recorded, and ALTER
-- TABLE (even a no-op one) and CREATE INDEX lock the table whether or not there is
-- anything to do. Waiting behind an open reader, that lock would queue every later
-- stream write behind it. The migration is one transaction, so one column tells.
DO $migrate$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_attribute
        WHERE attrelid = 'ops.stream_progress'::regclass AND attname = 'reported_at'
            AND NOT attisdropped
    ) THEN
        ALTER TABLE ops.stream_progress ALTER COLUMN max_event_ts DROP NOT NULL;
        ALTER TABLE ops.stream_progress ALTER COLUMN rows SET DEFAULT 0;
        ALTER TABLE ops.stream_progress ALTER COLUMN inserted SET DEFAULT 0;
        ALTER TABLE ops.stream_progress
            ADD COLUMN IF NOT EXISTS input_rows           bigint,
            ADD COLUMN IF NOT EXISTS dropped_by_watermark bigint,
            ADD COLUMN IF NOT EXISTS malformed_rows       bigint,
            ADD COLUMN IF NOT EXISTS future_rows          bigint,
            ADD COLUMN IF NOT EXISTS canary_rows          bigint,
            ADD COLUMN IF NOT EXISTS watermark            timestamptz,
            ADD COLUMN IF NOT EXISTS reported_at          timestamptz;
    END IF;
    IF to_regclass('ops.stream_progress_committed') IS NULL THEN
        CREATE INDEX stream_progress_committed ON ops.stream_progress (committed_at);
    END IF;
END
$migrate$;

-- Where each batch sat in the topic (M12): the source's start and end offsets from
-- the progress report, as Spark writes them ({"topic": {"partition": offset}}, end
-- exclusive). A warehouse batch's progress report is posted after its rows are in
-- PostgreSQL, so the largest end offset per partition is how far the warehouse has
-- committed: the reconciliation judges nothing past it. Gated like the block above.
DO $migrate_offsets$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_attribute
        WHERE attrelid = 'ops.stream_progress'::regclass AND attname = 'end_offsets'
            AND NOT attisdropped
    ) THEN
        ALTER TABLE ops.stream_progress
            ADD COLUMN IF NOT EXISTS start_offsets jsonb,
            ADD COLUMN IF NOT EXISTS end_offsets   jsonb;
    END IF;
END
$migrate_offsets$;

-- The streaming query's id beside its offsets: a reset checkpoint (or a recreated
-- topic) starts a new query whose offsets restart, and only the current query's
-- rows say how far the warehouse has committed. Gated like the blocks above.
DO $migrate_query_id$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_attribute
        WHERE attrelid = 'ops.stream_progress'::regclass AND attname = 'query_id'
            AND NOT attisdropped
    ) THEN
        ALTER TABLE ops.stream_progress ADD COLUMN IF NOT EXISTS query_id text;
    END IF;
END
$migrate_query_id$;

-- One row per reconciliation pass (`tributary stream reconcile`): the offset window
-- it read, what it found there and what it wrote back. A pass that fails records
-- nothing, so a row means the window was judged in full.
CREATE TABLE IF NOT EXISTS ops.reconcile_runs (
    id            bigserial   PRIMARY KEY,
    run_id        text        NOT NULL,
    query         text        NOT NULL,
    topic         text        NOT NULL,
    start_offsets jsonb       NOT NULL,
    end_offsets   jsonb       NOT NULL,
    records       bigint      NOT NULL,
    landed        bigint      NOT NULL,
    malformed     bigint      NOT NULL,
    future        bigint      NOT NULL,
    canary        bigint      NOT NULL,
    missing       bigint      NOT NULL,
    recovered     bigint      NOT NULL,
    left_missing  bigint      NOT NULL,
    dry_run       boolean     NOT NULL,
    seconds       double precision NOT NULL,
    started_at    timestamptz NOT NULL,
    finished_at   timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- Every event a pass found missing (valid, below the committed offsets, its id not in
-- raw.recentchange), and whether it was still missing when the pass finished.
CREATE TABLE IF NOT EXISTS ops.reconcile_missing (
    pass_id         bigint      NOT NULL REFERENCES ops.reconcile_runs (id) ON DELETE CASCADE,
    event_id        uuid        NOT NULL,
    event_ts        timestamptz NOT NULL,
    kafka_partition integer     NOT NULL,
    kafka_offset    bigint      NOT NULL,
    left_missing    boolean     NOT NULL,
    PRIMARY KEY (pass_id, event_id)
);

-- The latest result of every check: what an answer's provenance reports.
CREATE OR REPLACE VIEW ops.latest_quality AS
SELECT DISTINCT ON (check_name) *
FROM ops.quality_runs
ORDER BY check_name, finished_at DESC, id DESC;

COMMENT ON TABLE ops.quality_runs IS 'Every data-quality check result, one row per check per run.';
COMMENT ON TABLE ops.refresh_runs IS 'Every dbt model build, one row per model per run.';
COMMENT ON TABLE ops.reconcile_runs IS 'Every reconciliation pass of the topic against raw.recentchange.';
