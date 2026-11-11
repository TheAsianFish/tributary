-- title: The 20 most active human editors, with distinct pages
-- kind: scan
SELECT user_name, count(*) AS edits, count(DISTINCT title) AS pages
FROM {events}
WHERE is_bot = false AND event_type IN ('edit', 'new')
GROUP BY user_name
ORDER BY edits DESC, user_name
LIMIT 20
