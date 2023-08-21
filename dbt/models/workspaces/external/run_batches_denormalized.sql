with source_cte as (
    select
    {{ select_star_exclude_ab_cols(ref('run_batches_staged')) }}
    from {{ ref('run_batches_staged') }}
    {{ common_incremental() }}
),
runs_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('runs_cte')}}
      )
    ) as runs
    from {{ ref('runs_staged') }} runs_cte
    {{ schema_join(
    join_expr="source_cte.id = runs_cte.run_batch_id",
    join_table="source_cte",
    source_table="runs_cte"
) }}
    where runs_cte.is_deleted = false
    group by source_cte.ion_uid
),
source_cte_with_users as (
    {{ users_select('source_cte') }}
)
select source_cte_with_users.*,
runs_cte.runs
FROM source_cte_with_users
left outer join runs_cte on runs_cte.ion_uid = source_cte_with_users.ion_uid