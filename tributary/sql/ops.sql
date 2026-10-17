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

-- The latest result of every check: what an answer's provenance reports.
CREATE OR REPLACE VIEW ops.latest_quality AS
SELECT DISTINCT ON (check_name) *
FROM ops.quality_runs
ORDER BY check_name, finished_at DESC, id DESC;

COMMENT ON TABLE ops.quality_runs IS 'Every data-quality check result, one row per check per run.';
COMMENT ON TABLE ops.refresh_runs IS 'Every dbt model build, one row per model per run.';
