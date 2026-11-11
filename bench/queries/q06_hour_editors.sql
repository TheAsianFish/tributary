-- title: Events, editors and wikis in one hour
-- kind: window
SELECT count(*) AS events, count(DISTINCT user_name) AS editors, count(DISTINCT wiki) AS wikis
FROM {events}
WHERE event_ts >= {hour_start} AND event_ts < {hour_end}
