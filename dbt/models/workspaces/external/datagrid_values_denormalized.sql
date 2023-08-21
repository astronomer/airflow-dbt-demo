with source_cte as (
    select
    {{ select_star_exclude_ab_cols(ref('datagrid_values_staged')) }},
    {{ polymorphic_value_to_string() }} as value
    from {{ ref('datagrid_values_staged') }}
    {{ common_incremental() }}
),
part_inventory_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('part_inventory_cte')}}
    ) as part_inventory
    from {{ ref('parts_inventory_staged') }} part_inventory_cte
    {{ schema_join(
    join_expr="source_cte.part_inventory_id = part_inventory_cte.id",
    join_table="source_cte",
    source_table="part_inventory_cte"
) }}
    where part_inventory_cte.is_deleted = false
),
file_attachment_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('file_attachment_cte')}}
    ) as file_attachment
    from {{ ref('file_attachments_staged') }} file_attachment_cte
    {{ schema_join(
    join_expr="source_cte.file_attachment_id = file_attachment_cte.id",
    join_table="source_cte",
    source_table="file_attachment_cte"
) }}
    where file_attachment_cte.is_deleted = false
),
source_cte_with_users as (
    {{ users_select('source_cte') }}
)
select source_cte_with_users.*,
part_inventory_cte.part_inventory,
file_attachment_cte.file_attachment
FROM source_cte_with_users
left outer join part_inventory_cte on part_inventory_cte.ion_uid = source_cte_with_users.ion_uid
left outer join file_attachment_cte on file_attachment_cte.ion_uid = source_cte_with_users.ion_uid