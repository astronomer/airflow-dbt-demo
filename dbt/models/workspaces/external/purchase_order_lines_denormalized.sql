with source_cte as (
    select
    {{ select_star_exclude_ab_cols(ref('purchase_order_lines_staged')) }}
    from {{ ref('purchase_order_lines_staged') }}
    {{ common_incremental() }}
),
purchase_order_cte as (
    select
    source_cte.ion_uid,
    struct(
        purchase_order_cte.*
    ) as purchase_order
    from {{ ref('purchase_orders_staged') }} purchase_order_cte
    {{ schema_join(
    join_expr="source_cte.purchase_order_id = purchase_order_cte.id",
    join_table="source_cte",
    source_table="purchase_order_cte"
) }}
    where purchase_order_cte.is_deleted = false
),
part_cte as (
    select
    source_cte.ion_uid,
    struct(
        part_cte.*
    ) as part
    from {{ ref('parts_staged') }} part_cte
    {{ schema_join(
    join_expr="source_cte.part_id = part_cte.id",
    join_table="source_cte",
    source_table="part_cte"
) }}
    where part_cte.is_deleted = false
),
part_inventories_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        part_inventories_cte.*
      )
    ) as part_inventories,
    sum(
        coalesce(part_inventories_cte.quantity, 0)
    ) as inventory_quantity
    from {{ ref('parts_inventory_staged') }} part_inventories_cte
    {{ schema_join(
    join_expr="purchase_order_line_inventories.part_inventory_id = part_inventories_cte.id",
    join_table=ref('purchase_order_line_inventories_staged'),
    source_table="part_inventories_cte",
    join_alias="purchase_order_line_inventories",
    filter_deleted=true,
) }}
    {{ schema_join(
    join_expr="source_cte.id = purchase_order_line_inventories.purchase_order_line_id",
    join_table="source_cte",
    source_table="purchase_order_line_inventories"
) }}
    where part_inventories_cte.is_deleted = false
    group by source_cte.ion_uid
),
receipts_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        receipts_cte.*
      )
    ) as receipts
    from {{ ref('receipts_staged') }} receipts_cte
    {{ schema_join(
    join_expr="receipt_part_inventories.receipt_id = receipts_cte.id",
    join_table=ref('receipt_part_inventories_staged'),
    source_table="receipts_cte",
    join_alias="receipt_part_inventories",
    filter_deleted=true,
) }}
    {{ schema_join(
    join_expr="source_cte.id = receipt_part_inventories.purchase_order_line_id",
    join_table="source_cte",
    source_table="receipt_part_inventories"
) }}
    where receipts_cte.is_deleted = false
    group by source_cte.ion_uid
),
received_part_inventories_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        received_part_inventories_cte.*
      )
    ) as received_part_inventories
    from {{ ref('parts_inventory_staged') }} received_part_inventories_cte
    {{ schema_join(
    join_expr="receipt_part_inventories.part_inventory_id = received_part_inventories_cte.id",
    join_table=ref('receipt_part_inventories_staged'),
    source_table="received_part_inventories_cte",
    join_alias="receipt_part_inventories",
    filter_deleted=true,
) }}
    {{ schema_join(
    join_expr="source_cte.id = receipt_part_inventories.purchase_order_line_id",
    join_table="source_cte",
    source_table="receipt_part_inventories"
) }}
    where received_part_inventories_cte.is_deleted = false
    group by source_cte.ion_uid
),
receipt_part_inventories_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        receipt_part_inventories_cte.*
      )
    ) as receipt_part_inventories,
    sum(
        coalesce(receipt_part_inventories_cte.quantity, 0)
    ) as received_quantity
    from {{ ref('receipt_part_inventories_staged') }} receipt_part_inventories_cte
    {{ schema_join(
    join_expr="source_cte.id = receipt_part_inventories_cte.purchase_order_line_id",
    join_table="source_cte",
    source_table="receipt_part_inventories_cte"
) }}
    where receipt_part_inventories_cte.is_deleted = false
    group by source_cte.ion_uid
),
issues_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        issues_cte.*
      )
    ) as issues
    from {{ ref('issues_staged') }} issues_cte
    {{ schema_join(
    join_expr="source_cte.id = issues_cte.purchase_order_line_id",
    join_table="source_cte",
    source_table="issues_cte"
) }}
    where issues_cte.is_deleted = false
    group by source_cte.ion_uid
),
comments_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        comments.*
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
        labels.*
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
requirements_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        requirements.*
      )
    ) as requirements
    from {{ ref('requirements_staged') }} requirements
    {{ schema_join(
    join_expr="entities_requirements.requirement_id = requirements.id",
    join_table=ref('entities_requirements_staged'),
    source_table="requirements",
    join_alias="entities_requirements",
    filter_deleted=true
    ) }}
    {{ schema_join(
    join_expr="source_cte.entity_id = entities_requirements.entity_id",
    join_table="source_cte",
    source_table="entities_requirements"
    ) }}
    where requirements.is_deleted = false
    group by source_cte.ion_uid
),
extensible_attributes_cte as (
    select
    collect_list(
        struct(
            pas.type,
            pas.key,
            {{ polymorphic_value_to_string() }} as value
        )
    ) as attributes,
    source_cte.ion_uid
    from {{ ref('purchase_order_lines_attributes_staged') }} pas
    {{ schema_join(
    join_expr="source_cte.id = pas.purchase_order_line_id",
    join_table="source_cte",
    source_table="pas"
    ) }}
    where pas.is_deleted = false
    group by source_cte.ion_uid
),
source_cte_with_users as (
    {{ users_select('source_cte') }}
)
select source_cte_with_users.*,
purchase_order_cte.purchase_order,
part_cte.part,
part_inventories_cte.part_inventories,
receipts_cte.receipts,
received_part_inventories_cte.received_part_inventories,
receipt_part_inventories_cte.receipt_part_inventories,
issues_cte.issues,
comments_cte.comments as comments,
labels_cte.labels as labels,
requirements_cte.requirements,
extensible_attributes_cte.attributes as attributes,
part_inventories_cte.inventory_quantity,
receipt_part_inventories_cte.received_quantity,
CASE
    WHEN (source_cte_with_users.status = 'draft') THEN true
    WHEN (source_cte_with_users.status = 'approved') THEN true
    WHEN (source_cte_with_users.status = 'requested') THEN TRUE
    WHEN (source_cte_with_users.status = 'ordered') THEN TRUE
    ELSE false
END as is_open,
CASE
    WHEN (source_cte_with_users.status = 'draft') THEN true
    ELSE false
END as editable,
case
when source_cte_with_users.cost is null
then 0.0
else round(source_cte_with_users.cost * source_cte_with_users.quantity, 3)
end as estimated_cost
FROM source_cte_with_users
left outer join purchase_order_cte on purchase_order_cte.ion_uid = source_cte_with_users.ion_uid
left outer join part_cte on part_cte.ion_uid = source_cte_with_users.ion_uid
left outer join part_inventories_cte on part_inventories_cte.ion_uid = source_cte_with_users.ion_uid
left outer join receipts_cte on receipts_cte.ion_uid = source_cte_with_users.ion_uid
left outer join received_part_inventories_cte on received_part_inventories_cte.ion_uid = source_cte_with_users.ion_uid
left outer join receipt_part_inventories_cte on receipt_part_inventories_cte.ion_uid = source_cte_with_users.ion_uid
left outer join issues_cte on issues_cte.ion_uid = source_cte_with_users.ion_uid
left outer join comments_cte on
source_cte_with_users.ion_uid = comments_cte.ion_uid
left outer join labels_cte on
source_cte_with_users.ion_uid = labels_cte.ion_uid
left outer join requirements_cte on
source_cte_with_users.ion_uid = requirements_cte.ion_uid
left outer join extensible_attributes_cte on
source_cte_with_users.ion_uid = extensible_attributes_cte.ion_uid
