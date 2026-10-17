{#
  Incremental on arrival time (ingested_at), not event time: a late event has an old
  event_ts but a fresh ingested_at, so filtering on arrival is what picks it up. The
  lookback re-reads arrivals just before the high-water mark, because the sink stamps
  ingested_at inside concurrent transactions and one stamped earlier can commit later.

  merge rather than delete+insert: one atomic statement, and the rows re-read by the
  lookback (already present, unchanged; events are immutable) are matched on the
  unique index instead of being deleted and inserted again. MERGE needs PostgreSQL 15+.
#}
{{
    config(
        materialized='incremental',
        unique_key='event_id',
        incremental_strategy='merge',
        on_schema_change='append_new_columns',
        indexes=[
            {'columns': ['event_id'], 'unique': True},
            {'columns': ['ingested_at']},
            {'columns': ['wiki', 'event_ts']},
        ],
    )
}}

with edits as (

    select * from {{ ref('stg_recentchange') }}
    where event_type in ('edit', 'new')
    {% if is_incremental() %}
        and ingested_at > (
            select max(ingested_at) - interval '{{ var("fct_edits_lookback_minutes") | int }} minutes'
            from {{ this }}
        )
    {% endif %}

)

select
    event_id,
    event_ts,
    event_hour,
    wiki,
    md5(wiki || '|' || coalesce(namespace::text, '') || '|' || coalesce(title, '')) as page_key,
    namespace,
    namespace_name,
    title,
    user_name,
    is_bot,
    is_minor,
    is_patrolled,
    is_anonymous,
    event_type = 'new' as is_new_page,
    length_old,
    length_new,
    byte_delta,
    rev_old,
    rev_new,
    ingested_at
from edits
