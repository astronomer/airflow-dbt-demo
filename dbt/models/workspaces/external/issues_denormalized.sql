with source_cte as (
    select
		{{ select_star_exclude_ab_cols(ref('issues_staged')) }}
    from {{ ref('issues_staged') }}
    {{ common_incremental() }}
    ),
issue_disposition_types_cte as (
	select
		source_cte.ion_uid
		,issue_disposition_types_cte.title as title
		,struct(
			issue_disposition_types_cte.*
		) as issue_disposition_types
	from {{ ref('issue_disposition_types_staged') }} issue_disposition_types_cte
	{{
		schema_join(
		join_expr="source_cte.issue_disposition_type_id = issue_disposition_types_cte.id",
		join_table="source_cte",
		source_table="issue_disposition_types_cte") 
	}}
	where issue_disposition_types_cte.is_deleted = false
),
source_cte_with_users as (
	{{ users_select('source_cte') }}
)
select source_cte_with_users.*
	,issue_disposition_types_cte.issue_disposition_types
	,issue_disposition_types_cte.title as disposition_type
FROM source_cte_with_users
left outer join issue_disposition_types_cte on issue_disposition_types_cte.ion_uid = source_cte_with_users.ion_uid