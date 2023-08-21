with source_cte as (
    select
    {{ select_star_exclude_ab_cols(ref('suppliers_staged')) }}
    from {{ ref('suppliers_staged') }}
    {{ common_incremental() }}
),
parts_inventories_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('parts_inventories_cte')}}
      )
    ) as parts_inventories
    from {{ ref('parts_inventory_staged') }} parts_inventories_cte
    {{ schema_join(
    join_expr="source_cte.id = parts_inventories_cte.supplier_id",
    join_table="source_cte",
    source_table="parts_inventories_cte"
) }}
    where parts_inventories_cte.is_deleted = false
    group by source_cte.ion_uid
),
purchase_orders_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('purchase_orders_cte')}}
      )
    ) as purchase_orders
    from {{ ref('purchase_orders_staged') }} purchase_orders_cte
    {{ schema_join(
    join_expr="source_cte.id = purchase_orders_cte.supplier_id",
    join_table="source_cte",
    source_table="purchase_orders_cte"
) }}
    where purchase_orders_cte.is_deleted = false
    group by source_cte.ion_uid
),
plan_items_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('plan_items_cte')}}
      )
    ) as plan_items
    from {{ ref('plan_items_staged') }} plan_items_cte
    {{ schema_join(
    join_expr="source_cte.id = plan_items_cte.supplier_id",
    join_table="source_cte",
    source_table="plan_items_cte"
) }}
    where plan_items_cte.is_deleted = false
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
    from {{ ref('suppliers_attributes_staged') }} attrs
    {{ schema_join(
    join_expr="source_cte.id = attrs.supplier_id",
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
parts_inventories_cte.parts_inventories,
purchase_orders_cte.purchase_orders,
plan_items_cte.plan_items,
comments_cte.comments,
labels_cte.labels,
attributes_cte.attributes,
file_attachments_cte.file_attachments
FROM source_cte_with_users
left outer join parts_inventories_cte on parts_inventories_cte.ion_uid = source_cte_with_users.ion_uid
left outer join purchase_orders_cte on purchase_orders_cte.ion_uid = source_cte_with_users.ion_uid
left outer join plan_items_cte on plan_items_cte.ion_uid = source_cte_with_users.ion_uid
left outer join comments_cte on comments_cte.ion_uid = source_cte_with_users.ion_uid
left outer join labels_cte on labels_cte.ion_uid = source_cte_with_users.ion_uid
left outer join attributes_cte on attributes_cte.ion_uid = source_cte_with_users.ion_uid
left outer join file_attachments_cte on file_attachments_cte.ion_uid = source_cte_with_users.ion_uid
