{#
  Hourly volume z-score. Buckets rows by the hour of column_name (an event time),
  takes the latest closed hour (the one before the hour holding the newest row) and
  compares its row count with the previous lookback_hours closed hours. Returns one
  row (observed count, mean, stddev, z) when |z| > z_threshold.

  - Hours without rows inside the history window count as zero, so an outage cannot
    hide by leaving no bucket behind.
  - The first hour of data is left out of the history: it is almost always partial
    (the pipeline started mid-hour) and would drag the mean down.
  - With fewer than min_history hours of history, or a stddev of 0, the test passes:
    with nothing to compare against it has no grounds to fire.

  Severity defaults to error (a sudden drop is usually a broken source or sink);
  set var volume_anomaly_severity=warn where bursts are expected.
#}
{% test volume_anomaly(model, column_name, lookback_hours=24, min_history=6, z_threshold=3) %}
{{ config(severity=var('volume_anomaly_severity', 'error')) }}

with hourly as (

    select
        date_trunc('hour', {{ column_name }}, 'UTC') as hour,
        count(*) as row_count
    from {{ model }}
    where {{ column_name }} is not null
    group by 1

),

bounds as (

    select
        min(hour) as first_hour,
        max(hour) - interval '1 hour' as closed_hour
    from hourly

),

history_hours as (

    select generate_series(
        greatest(first_hour + interval '1 hour', closed_hour - interval '{{ lookback_hours | int }} hours'),
        closed_hour - interval '1 hour',
        interval '1 hour'
    ) as hour
    from bounds

),

history as (

    select coalesce(hourly.row_count, 0) as row_count
    from history_hours
    left join hourly on hourly.hour = history_hours.hour

),

history_stats as (

    select
        count(*) as history_hours,
        avg(row_count) as mean_count,
        stddev_samp(row_count) as stddev_count
    from history

),

observed as (

    select
        bounds.closed_hour,
        coalesce(hourly.row_count, 0) as observed_count
    from bounds
    left join hourly on hourly.hour = bounds.closed_hour

),

scored as (

    select
        observed.closed_hour,
        observed.observed_count,
        history_stats.history_hours,
        history_stats.mean_count,
        history_stats.stddev_count,
        (observed.observed_count - history_stats.mean_count)
            / nullif(history_stats.stddev_count, 0) as z_score
    from observed
    cross join history_stats

)

select *
from scored
where history_hours >= {{ min_history | int }}
    and stddev_count > 0
    and abs(z_score) > {{ z_threshold }}

{% endtest %}
