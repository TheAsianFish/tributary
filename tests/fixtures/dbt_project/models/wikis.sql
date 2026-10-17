select
    wiki,
    max(domain) as domain,
    case when wiki like '%wiktionary' then 'wiktionary' else 'wikipedia' end as project_family,
    count(*) as edits
from {{ source('raw', 'recentchange') }}
group by wiki
