with source_cte as (
    select
    {{ select_star_exclude_ab_cols(ref('step_approval_requests_staged')) }}
    from {{ ref('step_approval_requests_staged') }}
    {{ common_incremental() }}
),
step_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('step_cte')}}
    ) as step
    from {{ ref('steps_staged') }} step_cte
    {{ schema_join(
    join_expr="source_cte.step_id = step_cte.id",
    join_table="source_cte",
    source_table="step_cte"
) }}
    where step_cte.is_deleted = false
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
step_cte.step,
reviewer_cte.reviewer
FROM source_cte_with_users
left outer join step_cte on step_cte.ion_uid = source_cte_with_users.ion_uid
left outer join reviewer_cte on reviewer_cte.ion_uid = source_cte_with_users.ion_uid
