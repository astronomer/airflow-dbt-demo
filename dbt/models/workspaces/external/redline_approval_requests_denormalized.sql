with source_cte as (
    select
    {{ select_star_exclude_ab_cols(ref('redline_approval_requests_staged')) }}
    from {{ ref('redline_approval_requests_staged') }}
    {{ common_incremental() }}
),
redline_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('redline_cte')}}
    ) as redline
    from {{ ref('redlines_staged') }} redline_cte
    {{ schema_join(
    join_expr="source_cte.redline_id = redline_cte.id",
    join_table="source_cte",
    source_table="redline_cte"
) }}
    where redline_cte.is_deleted = false
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
redline_cte.redline,
reviewer_cte.reviewer
FROM source_cte_with_users
left outer join redline_cte on redline_cte.ion_uid = source_cte_with_users.ion_uid
left outer join reviewer_cte on reviewer_cte.ion_uid = source_cte_with_users.ion_uid