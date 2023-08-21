with source_cte as (
    select
		{{ select_star_exclude_ab_cols(ref('mbom_item_staged')) }}
    from {{ ref('mbom_item_staged') }}
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
substitutes_cte as (
	select
		source_cte.ion_uid
		,collect_list(
			struct(
			{{ select_star_exclude_ab_cols('substitutes_cte', ['mbom_item'])}}
		)
	) as substitutes
	from {{ ref('mbom_substitute_denormalized') }} substitutes_cte
	{{
		schema_join(
		join_expr="source_cte.id = substitutes_cte.mbom_item_id",
		join_table="source_cte",
		source_table="substitutes_cte") 
	}}
	where substitutes_cte.is_deleted = false
	group by source_cte.ion_uid
),
abom_items_cte as (
	select
		source_cte.ion_uid
		,collect_list(
			struct(
			{{ select_star_exclude_ab_cols('abom_items_cte')}}
		)
	) as abom_items
	from {{ ref('abom_items_staged') }} abom_items_cte
	{{
		schema_join(
		join_expr="source_cte.id = abom_items_cte.origin_mbom_item_id",
		join_table="source_cte",
		source_table="abom_items_cte") 
	}}
	where abom_items_cte.is_deleted = false
	group by source_cte.ion_uid
),
part_kit_items_cte as (
	select
		source_cte.ion_uid
		,collect_list(
			struct(
			{{ select_star_exclude_ab_cols('part_kit_items_cte')}}
		)
	) as part_kit_items
	from {{ ref('part_kit_items_staged') }} part_kit_items_cte
	{{
		schema_join(
		join_expr="source_cte.id = part_kit_items_cte.origin_mbom_item_id",
		join_table="source_cte",
		source_table="part_kit_items_cte") 
	}}
	where part_kit_items_cte.is_deleted = false
	group by source_cte.ion_uid
),
mbom_item_reference_designators_cte as (
	select
		source_cte.ion_uid
		,collect_list(
			struct(
			{{ select_star_exclude_ab_cols('mbom_item_reference_designators_cte')}}
		)
	) as mbom_item_reference_designators
	from {{ ref('mbom_item_reference_designators_staged') }} mbom_item_reference_designators_cte
	{{
		schema_join(
		join_expr="source_cte.id = mbom_item_reference_designators_cte.mbom_item_id",
		join_table="source_cte",
		source_table="mbom_item_reference_designators_cte") 
	}}
	where mbom_item_reference_designators_cte.is_deleted = false
	group by source_cte.ion_uid
),
source_cte_with_users as (
	{{ users_select('source_cte') }}
)
select source_cte_with_users.*
	,part_cte.part
	,substitutes_cte.substitutes
	,abom_items_cte.abom_items
	,part_kit_items_cte.part_kit_items
	,mbom_item_reference_designators_cte.mbom_item_reference_designators
FROM source_cte_with_users
left outer join part_cte on part_cte.ion_uid = source_cte_with_users.ion_uid
left outer join substitutes_cte on substitutes_cte.ion_uid = source_cte_with_users.ion_uid
left outer join abom_items_cte on abom_items_cte.ion_uid = source_cte_with_users.ion_uid
left outer join part_kit_items_cte on part_kit_items_cte.ion_uid = source_cte_with_users.ion_uid
left outer join mbom_item_reference_designators_cte on mbom_item_reference_designators_cte.ion_uid = source_cte_with_users.ion_uid