with source_cte as (
    select
    {{ select_star_exclude_ab_cols(ref('purchase_order_approval_requests_staged')) }}
    from {{ ref('purchase_order_approval_requests_staged') }}
    {{ common_incremental() }}
),
purchase_order_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('purchase_order_cte')}}
    ) as purchase_order
    from {{ ref('purchase_orders_staged') }} purchase_order_cte
    {{ schema_join(
    join_expr="source_cte.purchase_order_id = purchase_order_cte.id",
    join_table="source_cte",
    source_table="purchase_order_cte"
) }}
    where purchase_order_cte.is_deleted = false
),
reviewer_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('reviewer_cte')}}
    ) as reviewer
    from {{ ref('users_staged') }} reviewer_cte
    {{ schema_join(
    join_expr="source_cte.reviewer_id = reviewer_cte.id",
    join_table="source_cte",
    source_table="reviewer_cte"
) }}
    where reviewer_cte.is_deleted = false
),
source_cte_with_users as (
    {{ users_select('source_cte') }}
)
select source_cte_with_users.*,
purchase_order_cte.purchase_order,
reviewer_cte.reviewer
FROM source_cte_with_users
left outer join purchase_order_cte on purchase_order_cte.ion_uid = source_cte_with_users.ion_uid
left outer join reviewer_cte on reviewer_cte.ion_uid = source_cte_with_users.ion_uid