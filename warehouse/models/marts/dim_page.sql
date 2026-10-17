with edits as (

    select * from {{ ref('fct_edits') }}

)

select
    page_key,
    -- Constant within a page_key (they are what it hashes); max() just picks it.
    max(wiki) as wiki,
    max(namespace) as namespace,
    max(namespace_name) as namespace_name,
    max(title) as title,
    min(event_ts) as first_seen_ts,
    max(event_ts) as last_seen_ts,
    count(*) as edits,
    count(distinct user_name) as distinct_editors,
    bool_or(is_new_page) as created_in_window
from edits
group by page_key
