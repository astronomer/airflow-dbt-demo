with source_cte as (
    select
		{{ select_star_exclude_ab_cols(ref('mbom_substitute_staged')) }}
    from {{ ref('mbom_substitute_staged') }}
    {{ common_incremental() }}
    ),
mbom_item_cte as (
	select
		source_cte.ion_uid
		,struct(
			{{ select_star_exclude_ab_cols('mbom_item_cte')}}
		) as mbom_item
	from {{ ref('mbom_item_staged') }} mbom_item_cte
	{{
		schema_join(
		join_expr="source_cte.mbom_item_id = mbom_item_cte.id",
		join_table="source_cte",
		source_table="mbom_item_cte") 
	}}
	where mbom_item_cte.is_deleted = false
),
part_cte as (
	select
		source_cte.ion_uid
		,struct(
			{{ select_star_exclude_ab_cols('part_cte')}}
		) as part
	from {{ ref('parts_staged') }} part_cte
	{{
		schema_join(
		join_expr="source_cte.part_id = part_cte.id",
		join_table="source_cte",
		source_table="part_cte") 
	}}
	where part_cte.is_deleted = false
),
source_cte_with_users as (
	{{ users_select('source_cte') }}
)
select source_cte_with_users.*
	,mbom_item_cte.mbom_item
	,part_cte.part
FROM source_cte_with_users
left outer join mbom_item_cte on mbom_item_cte.ion_uid = source_cte_with_users.ion_uid
left outer join part_cte on part_cte.ion_uid = source_cte_with_users.ion_uid