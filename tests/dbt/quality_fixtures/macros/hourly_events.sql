{#
  One row per event, counts[i] rows in hour i after a fixed origin; the last entry
  is the open (current) hour. Event times are fixed, so the volume fixtures do not
  depend on when the test runs.
#}
{% macro hourly_events(counts) %}
select
    timestamptz '2026-01-01 00:00:00+00'
        + make_interval(hours => hours.hour_index)
        + make_interval(secs => events.n) as event_ts
from (
    values
    {%- for row_count in counts %}
        ({{ loop.index0 }}, {{ row_count }}){{ "," if not loop.last }}
    {%- endfor %}
) as hours (hour_index, row_count)
cross join lateral generate_series(1, hours.row_count) as events (n)
{% endmacro %}
