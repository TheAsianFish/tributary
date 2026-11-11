-- title: 95th-percentile emit delay of the 10 busiest wikis
-- kind: scan
SELECT wiki, count(*) AS events,
       percentile_disc(0.95) WITHIN GROUP (ORDER BY delay_ms) AS p95_delay_ms
FROM (
    SELECT wiki, (extract(epoch FROM emitted_at - event_ts) * 1000)::bigint AS delay_ms
    FROM {events}
    WHERE emitted_at IS NOT NULL
) AS d
GROUP BY wiki
ORDER BY events DESC, wiki
LIMIT 10
