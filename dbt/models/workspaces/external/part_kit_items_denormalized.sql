with source_cte as (
    select
    {{ select_star_exclude_ab_cols(ref('part_kit_items_staged')) }}
    from {{ ref('part_kit_items_staged') }}
    {{ common_incremental() }}
),
origin_mbom_item_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('origin_mbom_item_cte')}}
    ) as origin_mbom_item
    from {{ ref('mbom_item_staged') }} origin_mbom_item_cte
    {{ schema_join(
    join_expr="source_cte.origin_mbom_item_id = origin_mbom_item_cte.id",
    join_table="source_cte",
    source_table="origin_mbom_item_cte"
) }}
    where origin_mbom_item_cte.is_deleted = false
),
part_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('part_cte')}}
    ) as part
    from {{ ref('parts_staged') }} part_cte
    {{ schema_join(
    join_expr="source_cte.part_id = part_cte.id",
    join_table="source_cte",
    source_table="part_cte"
) }}
    where part_cte.is_deleted = false
),
part_kit_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('part_kit_cte')}}
    ) as part_kit
    from {{ ref('parts_kits_staged') }} part_kit_cte
    {{ schema_join(
    join_expr="source_cte.part_kit_id = part_kit_cte.id",
    join_table="source_cte",
    source_table="part_kit_cte"
) }}
    where part_kit_cte.is_deleted = false
),
part_kit_inventories_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('part_kit_inventories_cte')}}
      )
    ) as part_kit_inventories,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('part_inventories_cte')}},
        part_kit_inventories_cte.quantity as issued_quantity
      )
    ) as part_inventories,
    sum(part_kit_inventories_cte.quantity) as quantity_fulfilled
    from {{ ref('parts_inventory_staged') }} part_inventories_cte
    {{ schema_join(
    join_expr="part_kit_inventories_cte.part_inventory_id = part_inventories_cte.id",
    join_table=ref('part_kit_inventories_staged'),
    source_table="part_inventories_cte",
    join_alias="part_kit_inventories_cte",
    filter_deleted=true
    ) }}
    {{ schema_join(
    join_expr="source_cte.id = part_kit_inventories_cte.part_kit_item_id",
    join_table="source_cte",
    source_table="part_kit_inventories_cte"
) }}
    where part_inventories_cte.is_deleted = false
    group by source_cte.ion_uid
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
labels_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('labels')}}
      )
    ) as labels
    from {{ ref('labels_staged') }} labels
    {{ schema_join(
    join_expr="entities_labels.label_id = labels.id",
    join_table=ref('entities_labels_staged'),
    source_table="labels",
    join_alias="entities_labels",
    filter_deleted=true
    ) }}
    {{ schema_join(
    join_expr="source_cte.entity_id = entities_labels.entity_id",
    join_table="source_cte",
    source_table="entities_labels"
    ) }}
    where labels.is_deleted = false
    group by source_cte.ion_uid
),
attributes_cte as (
    select
    collect_list(
        struct(
            attrs.type,
            attrs.key,
            {{ polymorphic_value_to_string(polymorphic_table='attrs') }} as value
        )
    ) as attributes,
    source_cte.ion_uid
    from {{ ref('part_kit_item_attributes_staged') }} attrs
    {{ schema_join(
    join_expr="source_cte.id = attrs.part_kit_item_id",
    join_table="source_cte",
    source_table="attrs"
    ) }}
    where attrs.is_deleted = false
    group by source_cte.ion_uid
),
file_attachments_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        file_attachments_cte.*
      )
    ) as file_attachments
    from {{ ref('file_attachments_denormalized') }} file_attachments_cte
    {{ schema_join(
    join_expr="entities_file_attachments.file_attachment_id = file_attachments_cte.id",
    join_table=ref('entities_file_attachments_staged'),
    source_table="file_attachments_cte",
    join_alias="entities_file_attachments",
    filter_deleted=true
    ) }}
    {{ schema_join(
    join_expr="source_cte.entity_id = entities_file_attachments.entity_id",
    join_table="source_cte",
    source_table="entities_file_attachments"
    ) }}
    where file_attachments_cte.is_deleted = false
    group by source_cte.ion_uid
),
source_cte_with_users as (
    {{ users_select('source_cte') }}
)
select source_cte_with_users.*,
origin_mbom_item_cte.origin_mbom_item,
part_cte.part,
part_kit_cte.part_kit,
part_kit_inventories_cte.part_kit_inventories,
part_kit_inventories_cte.part_inventories,
coalesce(part_kit_inventories_cte.quantity_fulfilled, 0.0) as quantity_fulfilled,
case
when coalesce(part_kit_inventories_cte.quantity_fulfilled, 0.0) > 0.0
THEN true
else false
end as fulfilled,
source_cte_with_users.quantity - coalesce(part_kit_inventories_cte.quantity_fulfilled, 0.0) as quantity_unfulfilled,
comments_cte.comments,
labels_cte.labels,
attributes_cte.attributes,
file_attachments_cte.file_attachments
FROM source_cte_with_users
left outer join origin_mbom_item_cte on origin_mbom_item_cte.ion_uid = source_cte_with_users.ion_uid
left outer join part_cte on part_cte.ion_uid = source_cte_with_users.ion_uid
left outer join part_kit_cte on part_kit_cte.ion_uid = source_cte_with_users.ion_uid
left outer join part_kit_inventories_cte on part_kit_inventories_cte.ion_uid = source_cte_with_users.ion_uid
left outer join comments_cte on comments_cte.ion_uid = source_cte_with_users.ion_uid
left outer join labels_cte on labels_cte.ion_uid = source_cte_with_users.ion_uid
left outer join attributes_cte on attributes_cte.ion_uid = source_cte_with_users.ion_uid
left outer join file_attachments_cte on source_cte_with_users.ion_uid = file_attachments_cte.ion_uid
