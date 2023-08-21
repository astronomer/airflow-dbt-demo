with source_cte as (
    select
    {{ select_star_exclude_ab_cols(ref('review_requests_staged')) }}
    from {{ ref('review_requests_staged') }}
    {{ common_incremental() }}
),
procedure_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('procedure_cte')}}
    ) as procedure
    from {{ ref('procedures_staged') }} procedure_cte
    {{ schema_join(
    join_expr="source_cte.procedure_id = procedure_cte.id",
    join_table="source_cte",
    source_table="procedure_cte"
) }}
    where procedure_cte.is_deleted = false
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
procedure_cte.procedure,
reviewer_cte.reviewer
FROM source_cte_with_users
left outer join procedure_cte on procedure_cte.ion_uid = source_cte_with_users.ion_uid
left outer join reviewer_cte on reviewer_cte.ion_uid = source_cte_with_users.ion_uid