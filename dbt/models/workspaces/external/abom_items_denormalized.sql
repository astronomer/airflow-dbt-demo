with source_cte as (
    select
		{{ select_star_exclude_ab_cols(ref('abom_items_staged')) }}
    from {{ ref('abom_items_staged') }}
    {{ common_incremental() }}
    ),
parents_cte as (
	select
		source_cte.ion_uid
		,sum(parents_cte.quantity) as parent_quantity
		,collect_list(
			struct(
			{{ select_star_exclude_ab_cols('parents_cte')}}
		)
	) as parents
	from {{ ref('abom_items_staged') }} parents_cte
	{{
		schema_join(
		source_table="parents_cte",
		join_expr="abom_edges.parent_abom_item_id = parents_cte.id",
		join_table=ref('abom_edges_staged'),
		join_alias="abom_edges",
		filter_deleted=true) 
	}}
	{{
		schema_join(
		source_table="abom_edges",
		join_table="source_cte",
		join_expr="source_cte.id = abom_edges.child_abom_item_id") 
	}}
	where parents_cte.is_deleted = false
	group by source_cte.ion_uid
),
source_cte_with_users as (
	{{ users_select('source_cte') }}
)
select source_cte_with_users.*
	,parents_cte.parents
	,CASE
          WHEN source_cte_with_users.expected_quantity_per IS NOT NULL
          THEN source_cte_with_users.expected_quantity_per * parents_cte.parent_quantity
          ELSE source_cte_with_users.expected_fixed_quantity
        END as expected_quantity
FROM source_cte_with_users
left outer join parents_cte on parents_cte.ion_uid = source_cte_with_users.ion_uid