-- title: One event by id
-- kind: lookup
SELECT event_id, event_ts, wiki, title, user_name
FROM {events}
WHERE event_id = {event_id}
