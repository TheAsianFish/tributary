-- title: The 10 wikis that grew most in six hours
-- kind: window
SELECT wiki, count(*) AS edits, sum(length_new - length_old) AS bytes_added
FROM {events}
WHERE event_ts >= {hour_start} AND event_ts < {six_hours_end} AND event_type = 'edit'
GROUP BY wiki
ORDER BY bytes_added DESC NULLS LAST, wiki
LIMIT 10
