{#
  dim_wiki counts every event type, so it reads staging (a view over the live raw
  table) rather than fct_edits. Read unbounded, that races fct_edits: with several
  threads dim_wiki could build first, and a new wiki's first edit committing between
  the two builds would land in fct_edits but not here, failing the fct_edits.wiki
  relationship test. Referencing fct_edits makes dbt build it first, and reading only
  arrivals up to its high-water mark means every wiki in it is seen here too
  ('infinity' when it is empty, so a first build is not cut short).
#}
with loaded as (

    select coalesce(max(ingested_at), 'infinity'::timestamptz) as max_ingested_at
    from {{ ref('fct_edits') }}

),

events as (

    select * from {{ ref('stg_recentchange') }}
    where ingested_at <= (select max_ingested_at from loaded)

),

per_wiki as (

    select
        wiki,
        -- Account creations are logged under auth.wikimedia.org for every wiki, so
        -- the wiki's own domain is the most frequent one among the other events.
        mode() within group (order by domain) filter (where project_family is not null)
            as domain,
        min(event_ts) as first_event_ts,
        max(event_ts) as last_event_ts,
        count(*) as events,
        count(*) filter (where event_type in ('edit', 'new')) as edits
    from events
    group by wiki

)

select
    wiki,
    domain,
    {{ project_family('domain') }} as project_family,
    {{ language_code('domain') }} as language_code,
    first_event_ts,
    last_event_ts,
    events,
    edits
from per_wiki
