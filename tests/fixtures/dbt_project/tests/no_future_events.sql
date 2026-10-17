-- A singular test: no edit is dated after it was ingested.
select event_id from {{ source('raw', 'recentchange') }} where event_ts > ingested_at + interval '1 hour'
