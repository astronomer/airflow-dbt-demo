with source_cte as (
    select
		{{ select_star_exclude_ab_cols(ref('mboms_staged')) }}
    from {{ ref('mboms_staged') }}
    {{ common_incremental() }}
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
mbom_items_cte as (
	select
		source_cte.ion_uid
		,collect_list(
			struct(
			{{ select_star_exclude_ab_cols('mbom_items')}}
		)
	) as mbom_items
	from {{ ref('mbom_item_staged') }} mbom_items
	{{
		schema_join(
		join_expr="source_cte.id = mbom_items.mbom_id",
		join_table="source_cte",
		source_table="mbom_items") 
	}}
	where mbom_items.is_deleted = false
	group by source_cte.ion_uid
),
source_cte_with_users as (
	{{ users_select('source_cte') }}
)
select source_cte_with_users.*
	,part_cte.part
	,mbom_items_cte.mbom_items
FROM source_cte_with_users
left outer join part_cte on part_cte.ion_uid = source_cte_with_users.ion_uid
left outer join mbom_items_cte on mbom_items_cte.ion_uid = source_cte_with_users.ion_uid