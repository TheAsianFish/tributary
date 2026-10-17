with edits as (

    select * from {{ ref('fct_edits') }}

)

select
    wiki,
    event_hour,
    count(*) as edits,
    count(*) filter (where is_new_page) as new_pages,
    count(*) filter (where is_bot) as bot_edits,
    count(*) filter (where is_anonymous) as anonymous_edits,
    count(distinct user_name) as distinct_editors,
    coalesce(sum(byte_delta) filter (where byte_delta > 0), 0) as bytes_added,
    coalesce(-sum(byte_delta) filter (where byte_delta < 0), 0) as bytes_removed
from edits
group by wiki, event_hour
