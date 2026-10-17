{#
  Incremental on arrival time (ingested_at), not event time: a late event has an old
  event_ts but a fresh ingested_at, so filtering on arrival is what picks it up. The
  lookback re-reads arrivals just before the high-water mark, because the sink stamps
  ingested_at inside concurrent transactions and one stamped earlier can commit later.

  The rows the lookback re-reads are mostly present already. Events are immutable, so
  they are dropped before the merge (the not exists below): dbt's merge UPDATEs every
  matched row, and in PostgreSQL each update writes a new tuple, so without the filter
  every run would rewrite the whole lookback window for nothing.

  merge rather than delete+insert: one atomic statement, and unique_key stays as the
  safety net should a duplicate event_id ever get past the filter (it is matched on the
  unique index, not inserted twice). MERGE needs PostgreSQL 15+.
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

    select * from {{ ref('stg_recentchange') }} as recent
    where event_type in ('edit', 'new')
    {% if is_incremental() %}
        -- coalesce: if the first build found no edits, max() is NULL and a bare
        -- comparison would select nothing on every later run.
        and ingested_at > (
            select coalesce(max(ingested_at), '-infinity'::timestamptz)
                - interval '{{ var("fct_edits_lookback_minutes") | int }} minutes'
            from {{ this }}
        )
        and not exists (
            select 1 from {{ this }} as existing
            where existing.event_id = recent.event_id
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
