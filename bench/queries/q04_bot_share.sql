-- title: Bot share per event type
-- kind: scan
SELECT event_type, count(*) AS events, avg(CASE WHEN is_bot THEN 1 ELSE 0 END) AS bot_share
FROM {events}
GROUP BY event_type
ORDER BY event_type
