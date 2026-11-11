-- Trino has no exact percentile aggregate (approx_percentile is a sketch), so the
-- 95th percentile is the value at rank ceil(0.95 * n), which is what PostgreSQL's
-- percentile_disc(0.95) returns.
WITH d AS (
    SELECT wiki, date_diff('millisecond', event_ts, emitted_at) AS delay_ms
    FROM {events}
    WHERE emitted_at IS NOT NULL
),
ranked AS (
    SELECT wiki, delay_ms,
           row_number() OVER (PARTITION BY wiki ORDER BY delay_ms) AS rn,
           count(*) OVER (PARTITION BY wiki) AS n
    FROM d
)
SELECT wiki, max(n) AS events, min(delay_ms) FILTER (WHERE rn >= ceil(0.95 * n)) AS p95_delay_ms
FROM ranked
GROUP BY wiki
ORDER BY events DESC, wiki
LIMIT 10
