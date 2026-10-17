with events as (

    select * from {{ ref('stg_recentchange') }}

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
