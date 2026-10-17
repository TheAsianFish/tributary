with edits as (

    select * from {{ ref('fct_edits') }}

)

select
    wiki,
    user_name,
    bool_or(is_bot) as is_bot,
    bool_or(is_anonymous) as is_anonymous,
    count(*) as edits,
    count(distinct page_key) as pages_edited,
    min(event_ts) as first_edit_ts,
    max(event_ts) as last_edit_ts,
    coalesce(sum(byte_delta), 0) as net_bytes
from edits
group by wiki, user_name
