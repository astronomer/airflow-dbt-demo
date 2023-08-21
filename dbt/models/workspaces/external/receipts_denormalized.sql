with source_cte as (
    select
    {{ select_star_exclude_ab_cols(ref('receipts_staged')) }}
    from {{ ref('receipts_staged') }}
    {{ common_incremental() }}
),
received_by_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('received_by_cte')}}
    ) as received_by
    from {{ ref('users_staged') }} received_by_cte
    {{ schema_join(
    join_expr="source_cte.received_by_id = received_by_cte.id",
    join_table="source_cte",
    source_table="received_by_cte"
) }}
    where received_by_cte.is_deleted = false
),
part_inventories_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('part_inventories_cte')}}
      )
    ) as part_inventories
    from {{ ref('parts_inventory_staged') }} part_inventories_cte
    {{ schema_join(
    join_expr="receipt_part_inventories.part_inventory_id = part_inventories_cte.id",
    join_table=ref('receipt_part_inventories_staged'),
    source_table="part_inventories_cte",
    join_alias="receipt_part_inventories",
    filter_deleted=true
) }}
    {{ schema_join(
    join_expr="source_cte.id = receipt_part_inventories.receipt_id",
    join_table="source_cte",
    source_table="receipt_part_inventories"
) }}
    where part_inventories_cte.is_deleted = false
    group by source_cte.ion_uid
),
purchase_order_lines_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('purchase_order_lines_cte')}}
      )
    ) as purchase_order_lines
    from {{ ref('purchase_order_lines_staged') }} purchase_order_lines_cte
    {{ schema_join(
    join_expr="receipt_part_inventories.purchase_order_line_id = purchase_order_lines_cte.id",
    join_table=ref('receipt_part_inventories_staged'),
    source_table="purchase_order_lines_cte",
    join_alias="receipt_part_inventories",
    filter_deleted=true
) }}
    {{ schema_join(
    join_expr="source_cte.id = receipt_part_inventories.receipt_id",
    join_table="source_cte",
    source_table="receipt_part_inventories"
) }}
    where purchase_order_lines_cte.is_deleted = false
    group by source_cte.ion_uid
),
received_parts_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('received_parts_cte')}}
      )
    ) as received_parts
    from {{ ref('receipt_part_inventories_staged') }} received_parts_cte
    {{ schema_join(
    join_expr="source_cte.id = received_parts_cte.receipt_id",
    join_table="source_cte",
    source_table="received_parts_cte"
) }}
    where received_parts_cte.is_deleted = false
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
    from {{ ref('receipts_attributes_staged') }} attrs
    {{ schema_join(
    join_expr="source_cte.id = attrs.receipt_id",
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
received_by_cte.received_by,
part_inventories_cte.part_inventories,
purchase_order_lines_cte.purchase_order_lines,
received_parts_cte.received_parts,
comments_cte.comments,
labels_cte.labels,
attributes_cte.attributes,
file_attachments_cte.file_attachments
FROM source_cte_with_users
left outer join received_by_cte on received_by_cte.ion_uid = source_cte_with_users.ion_uid
left outer join part_inventories_cte on part_inventories_cte.ion_uid = source_cte_with_users.ion_uid
left outer join purchase_order_lines_cte on purchase_order_lines_cte.ion_uid = source_cte_with_users.ion_uid
left outer join received_parts_cte on received_parts_cte.ion_uid = source_cte_with_users.ion_uid
left outer join comments_cte on comments_cte.ion_uid = source_cte_with_users.ion_uid
left outer join labels_cte on labels_cte.ion_uid = source_cte_with_users.ion_uid
left outer join attributes_cte on attributes_cte.ion_uid = source_cte_with_users.ion_uid
left outer join file_attachments_cte on file_attachments_cte.ion_uid = source_cte_with_users.ion_uid
