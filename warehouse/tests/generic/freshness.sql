{#
  Arrival freshness: fails when the newest value of column_name is more than
  max_lag_minutes old, or when the table is empty. Apply it to an arrival
  timestamp (ingested_at), never to event time: a replayed sample has event times
  in the past and would always look stale.

  Severity defaults to error because a stalled pipeline makes every downstream
  answer silently out of date; set var freshness_severity=warn to tolerate it
  (e.g. while working on a loaded sample).
#}
{% test freshness(model, column_name, max_lag_minutes=none) %}
{{ config(severity=var('freshness_severity', 'error')) }}
{%- set limit_minutes = max_lag_minutes if max_lag_minutes is not none
    else var('freshness_max_lag_minutes', 60) -%}

with latest as (

    select max({{ column_name }}) as latest_value
    from {{ model }}

)

select
    latest_value,
    now() - latest_value as lag,
    interval '{{ limit_minutes | int }} minutes' as max_lag
from latest
where latest_value is null
    or now() - latest_value > interval '{{ limit_minutes | int }} minutes'

{% endtest %}
