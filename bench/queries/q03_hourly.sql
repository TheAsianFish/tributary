-- title: Events and active wikis per hour
-- kind: scan
SELECT date_trunc('hour', event_ts) AS hour, count(*) AS events, count(DISTINCT wiki) AS wikis
FROM {events}
GROUP BY date_trunc('hour', event_ts)
ORDER BY hour
