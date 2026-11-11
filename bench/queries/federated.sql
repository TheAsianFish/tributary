-- title: The same join inside PostgreSQL, the federated query's reference result
SELECT d.project_family, count(*) AS events, count(DISTINCT e.wiki) AS wikis
FROM {events} AS e
JOIN marts.dim_wiki AS d ON d.wiki = e.wiki
WHERE e.event_ts >= {hour_start} AND e.event_ts < {hour_end}
GROUP BY d.project_family
ORDER BY events DESC, d.project_family
