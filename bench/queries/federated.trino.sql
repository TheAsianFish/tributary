-- title: Events in one hour by project family (lake events joined with the warehouse's dim_wiki)
SELECT d.project_family, count(*) AS events, count(DISTINCT e.wiki) AS wikis
FROM {events} AS e
JOIN postgresql.marts.dim_wiki AS d ON d.wiki = e.wiki
WHERE e.event_ts >= {hour_start} AND e.event_ts < {hour_end}
GROUP BY d.project_family
ORDER BY events DESC, d.project_family
