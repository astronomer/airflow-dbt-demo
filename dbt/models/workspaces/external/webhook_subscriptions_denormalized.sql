with source_cte as (
    select
    {{ select_star_exclude_ab_cols(ref('webhook_subscriptions_staged')) }}
    from {{ ref('webhook_subscriptions_staged') }}
    {{ common_incremental() }}
),
receiver_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('receiver_cte')}}
    ) as receiver
    from {{ ref('webhook_receivers_staged') }} receiver_cte
    {{ schema_join(
    join_expr="source_cte.receiver_id = receiver_cte.id",
    join_table="source_cte",
    source_table="receiver_cte"
) }}
    where receiver_cte.is_deleted = false
),
source_cte_with_users as (
    {{ users_select('source_cte') }}
)
select source_cte_with_users.*,
receiver_cte.receiver
FROM source_cte_with_users
left outer join receiver_cte on receiver_cte.ion_uid = source_cte_with_users.ion_uid
