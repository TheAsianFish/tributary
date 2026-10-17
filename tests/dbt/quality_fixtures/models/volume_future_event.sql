{#
  A view, so now() is read when the test queries it: the fixture's open hour is then
  always the test's current hour, even when the build and the test straddle an hour.
#}
{{ config(materialized='view') }}

-- Flat history up to the current (open, partial) hour, then one event dated in the
-- next hour, as a client with a fast clock would send.
{{ hourly_events(
    [100, 99, 101, 100, 98, 102, 100, 101, 99, 100, 100, 20],
    origin="date_trunc('hour', now(), 'UTC') - interval '11 hours'"
) }}
union all
select date_trunc('hour', now(), 'UTC') + interval '90 minutes' as event_ts
