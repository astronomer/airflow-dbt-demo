with source_cte as (
    select
    {{ select_star_exclude_ab_cols(ref('mrp_jobs_staged')) }}
    from {{ ref('mrp_jobs_staged') }}
    {{ common_incremental() }}
),
comments_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('comments')}}
      )
    ) as comments
    from {{ ref('comments_staged') }} comments
    {{ schema_join(
    join_expr="source_cte.entity_id = comments.entity_id",
    join_table="source_cte",
    source_table="comments"
) }}
    where comments.is_deleted = false
    group by source_cte.ion_uid
),
source_cte_with_users as (
    {{ users_select('source_cte') }}
)
select source_cte_with_users.*,
(source_cte_with_users.id = (select latest_mrp_job.id
                            from {{ ref('mrp_jobs_staged') }} latest_mrp_job
                            where latest_mrp_job.status = 'complete'
                            order by latest_mrp_job._created desc
    limit 1 )) as is_latest,
comments_cte.comments
FROM source_cte_with_users
left outer join comments_cte on comments_cte.ion_uid = source_cte_with_users.ion_uid