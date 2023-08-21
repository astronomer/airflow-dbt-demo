with source_cte as (
    select
    {{ select_star_exclude_ab_cols(ref('datagrid_rows_staged')) }}
    from {{ ref('datagrid_rows_staged') }}
    {{ common_incremental() }}
),
run_step_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('run_step_cte')}}
    ) as run_step
    from {{ ref('run_steps_staged') }} run_step_cte
    {{ schema_join(
    join_expr="source_cte.run_step_id = run_step_cte.id",
    join_table="source_cte",
    source_table="run_step_cte"
) }}
    where run_step_cte.is_deleted = false
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
values_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('values_cte')}}
      )
    ) as values
    from {{ ref('datagrid_values_staged') }} values_cte
    {{ schema_join(
    join_expr="source_cte.id = values_cte.row_id",
    join_table="source_cte",
    source_table="values_cte"
) }}
    where values_cte.is_deleted = false
    group by source_cte.ion_uid
),
source_cte_with_users as (
    {{ users_select('source_cte') }}
)
select source_cte_with_users.*,
run_step_cte.run_step,
step_cte.step,
values_cte.values
FROM source_cte_with_users
left outer join run_step_cte on run_step_cte.ion_uid = source_cte_with_users.ion_uid
left outer join step_cte on step_cte.ion_uid = source_cte_with_users.ion_uid
left outer join values_cte on values_cte.ion_uid = source_cte_with_users.ion_uid