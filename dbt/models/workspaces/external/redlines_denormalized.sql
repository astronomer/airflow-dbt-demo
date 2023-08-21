with source_cte as (
    select
    {{ select_star_exclude_ab_cols(ref('redlines_staged')) }}
    from {{ ref('redlines_staged') }}
    {{ common_incremental() }}
),
step_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('step_cte')}}
    ) as step
    from {{ ref('run_steps_staged') }} step_cte
    {{ schema_join(
    join_expr="source_cte.step_id = step_cte.id",
    join_table="source_cte",
    source_table="step_cte"
) }}
    where step_cte.is_deleted = false
),
issue_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('issue_cte')}}
    ) as issue
    from {{ ref('issues_staged') }} issue_cte
    {{ schema_join(
    join_expr="source_cte.issue_id = issue_cte.id",
    join_table="source_cte",
    source_table="issue_cte"
) }}
    where issue_cte.is_deleted = false
),
approvals_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('approvals_cte')}}
      )
    ) as approvals
    from {{ ref('redline_approvals_staged') }} approvals_cte
    {{ schema_join(
    join_expr="source_cte.id = approvals_cte.redline_id",
    join_table="source_cte",
    source_table="approvals_cte"
) }}
    where approvals_cte.is_deleted = false
    group by source_cte.ion_uid
),
approval_requests_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('approval_requests_cte')}}
      )
    ) as approval_requests
    from {{ ref('redline_approval_requests_staged') }} approval_requests_cte
    {{ schema_join(
    join_expr="source_cte.id = approval_requests_cte.redline_id",
    join_table="source_cte",
    source_table="approval_requests_cte"
) }}
    where approval_requests_cte.is_deleted = false
    group by source_cte.ion_uid
),
source_cte_with_users as (
    {{ users_select('source_cte') }}
)
select source_cte_with_users.*,
step_cte.step,
issue_cte.issue,
approvals_cte.approvals,
approval_requests_cte.approval_requests
FROM source_cte_with_users
left outer join step_cte on step_cte.ion_uid = source_cte_with_users.ion_uid
left outer join issue_cte on issue_cte.ion_uid = source_cte_with_users.ion_uid
left outer join approvals_cte on approvals_cte.ion_uid = source_cte_with_users.ion_uid
left outer join approval_requests_cte on approval_requests_cte.ion_uid = source_cte_with_users.ion_uid