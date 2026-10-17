with source as (

    select * from {{ source('raw', 'recentchange') }}

),

typed as (

    select
        event_id,
        event_ts,
        date_trunc('hour', event_ts, 'UTC') as event_hour,
        emitted_at,
        rc_id,
        wiki,
        domain,
        {{ project_family('domain') }} as project_family,
        {{ language_code('domain') }} as language_code,
        event_type,
        namespace,
        title,
        user_name,
        user_name ~ '^\d{1,3}(\.\d{1,3}){3}$'
            or user_name ~* '^[0-9a-f]{0,4}(:[0-9a-f]{0,4}){2,7}$' as is_ip_user,
        user_name ~ '^~\d{4}-\d+-\d+$' as is_temporary_account,
        is_bot,
        is_minor,
        is_patrolled,
        length_old,
        length_new,
        -- A creation has no previous revision: its old size is zero, not unknown.
        case
            when event_type = 'new' then length_new - coalesce(length_old, 0)
            else length_new - length_old
        end as byte_delta,
        rev_old,
        rev_new,
        log_type,
        log_action,
        kafka_partition,
        kafka_offset,
        kafka_ts,
        ingested_at
    from source

),

namespaces as (

    select * from {{ ref('namespaces') }}

)

select
    typed.event_id,
    typed.event_ts,
    typed.event_hour,
    typed.emitted_at,
    typed.rc_id,
    typed.wiki,
    typed.domain,
    typed.project_family,
    typed.language_code,
    typed.event_type,
    typed.namespace,
    coalesce(ns_wiki.namespace_name, ns_family.namespace_name, ns_default.namespace_name)
        as namespace_name,
    typed.title,
    typed.user_name,
    typed.is_ip_user or typed.is_temporary_account as is_anonymous,
    typed.is_temporary_account,
    typed.is_bot,
    typed.is_minor,
    typed.is_patrolled,
    typed.length_old,
    typed.length_new,
    typed.byte_delta,
    typed.rev_old,
    typed.rev_new,
    typed.log_type,
    typed.log_action,
    typed.kafka_partition,
    typed.kafka_offset,
    typed.kafka_ts,
    typed.ingested_at
from typed
left join namespaces as ns_wiki
    on ns_wiki.namespace_id = typed.namespace
    and ns_wiki.wiki = typed.wiki
left join namespaces as ns_family
    on ns_family.namespace_id = typed.namespace
    and ns_family.project_family = typed.project_family
    and ns_family.wiki is null
left join namespaces as ns_default
    on ns_default.namespace_id = typed.namespace
    and ns_default.project_family is null
    and ns_default.wiki is null
