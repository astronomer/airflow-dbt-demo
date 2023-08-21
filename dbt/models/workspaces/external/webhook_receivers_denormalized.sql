with source_cte as (
    select
    {{ select_star_exclude_ab_cols(ref('webhook_receivers_staged')) }}
    from {{ ref('webhook_receivers_staged') }}
    {{ common_incremental() }}
),
subscriptions_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('subscriptions_cte')}}
      )
    ) as subscriptions
    from {{ ref('webhook_subscriptions_staged') }} subscriptions_cte
    {{ schema_join(
    join_expr="source_cte.id = subscriptions_cte.receiver_id",
    join_table="source_cte",
    source_table="subscriptions_cte"
) }}
    where subscriptions_cte.is_deleted = false
    group by source_cte.ion_uid
),
headers_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('headers_cte')}}
      )
    ) as headers
    from {{ ref('webhook_headers_staged') }} headers_cte
    {{ schema_join(
    join_expr="webhook_receiver_headers.webhook_header_id = headers_cte.id",
    join_table=ref('webhook_receiver_headers_staged'),
    source_table="headers_cte",
    join_alias="webhook_receiver_headers",
    filter_deleted=true
) }}
    {{ schema_join(
    join_expr="source_cte.id = webhook_receiver_headers.webhook_receiver_id",
    join_table="source_cte",
    source_table="webhook_receiver_headers"
) }}
    where headers_cte.is_deleted = false
    group by source_cte.ion_uid
),
source_cte_with_users as (
    {{ users_select('source_cte') }}
)
select source_cte_with_users.*,
array_contains(subscriptions_cte.subscriptions.active, true) as active,
subscriptions_cte.subscriptions,
headers_cte.headers
FROM source_cte_with_users
left outer join subscriptions_cte on subscriptions_cte.ion_uid = source_cte_with_users.ion_uid
left outer join headers_cte on headers_cte.ion_uid = source_cte_with_users.ion_uid