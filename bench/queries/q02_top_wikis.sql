-- title: The 20 wikis with the most edits
-- kind: scan
SELECT wiki, count(*) AS edits
FROM {events}
WHERE event_type = 'edit'
GROUP BY wiki
ORDER BY edits DESC, wiki
LIMIT 20
