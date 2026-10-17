-- Landing table for the Spark stream job. Idempotent: safe to run on every start.
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.recentchange (
    event_id        uuid        PRIMARY KEY,
    event_ts        timestamptz NOT NULL,
    emitted_at      timestamptz,
    rc_id           bigint,
    wiki            text        NOT NULL,
    domain          text,
    event_type      text        NOT NULL,
    namespace       integer,
    title           text,
    user_name       text,
    is_bot          boolean,
    is_minor        boolean,
    is_patrolled    boolean,
    length_old      integer,
    length_new      integer,
    rev_old         bigint,
    rev_new         bigint,
    log_type        text,
    log_action      text,
    kafka_partition integer     NOT NULL,
    kafka_offset    bigint      NOT NULL,
    kafka_ts        timestamptz NOT NULL,
    ingested_at     timestamptz NOT NULL DEFAULT clock_timestamp()
);

COMMENT ON TABLE raw.recentchange IS
    'Wikimedia recentchange events, one row per event UUID, landed by tributary stream.';
COMMENT ON COLUMN raw.recentchange.event_ts IS 'When the change happened (MediaWiki timestamp, 1 s).';
COMMENT ON COLUMN raw.recentchange.emitted_at IS 'When EventGate emitted the event (meta.dt).';
COMMENT ON COLUMN raw.recentchange.kafka_ts IS 'Kafka record timestamp, set by the producer.';
COMMENT ON COLUMN raw.recentchange.ingested_at IS 'When the sink inserted the row.';

-- fct_edits loads incrementally on arrival time. Rows arrive in roughly
-- ingested_at order, so a BRIN index is a few pages and costs the sink almost nothing.
CREATE INDEX IF NOT EXISTS recentchange_ingested_at_brin
    ON raw.recentchange USING brin (ingested_at);
