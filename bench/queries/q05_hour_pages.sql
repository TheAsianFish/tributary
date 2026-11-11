-- title: The 10 most-changed pages in one hour
-- kind: window
SELECT wiki, title, count(*) AS events
FROM {events}
WHERE event_ts >= {hour_start} AND event_ts < {hour_end}
GROUP BY wiki, title
ORDER BY events DESC, wiki, title
LIMIT 10
