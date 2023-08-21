with source_cte as (
    select
    {{ select_star_exclude_ab_cols(ref('purchase_orders_staged')) }}
    from {{ ref('purchase_orders_staged') }}
    {{ common_incremental() }}
),
supplier_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('supplier_cte')}}
    ) as supplier
    from {{ ref('suppliers_staged') }} supplier_cte
    {{ schema_join(
    join_expr="source_cte.supplier_id = supplier_cte.id",
    join_table="source_cte",
    source_table="supplier_cte"
) }}
    where supplier_cte.is_deleted = false
),
currency_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('currency_cte')}}
    ) as currency
    from {{ ref('currencies_staged') }} currency_cte
    {{ schema_join(
    join_expr="source_cte.currency_id = currency_cte.id",
    join_table="source_cte",
    source_table="currency_cte"
) }}
    where currency_cte.is_deleted = false
),
approvals_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('approvals_cte')}}
      )
    ) as approvals
    from {{ ref('purchase_order_approvals_staged') }} approvals_cte
    {{ schema_join(
    join_expr="source_cte.id = approvals_cte.purchase_order_id",
    join_table="source_cte",
    source_table="approvals_cte"
) }}
    where approvals_cte.is_deleted = false
    group by source_cte.ion_uid
),
approval_requests_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('approval_requests_cte')}}
      )
    ) as approval_requests
    from {{ ref('purchase_order_approval_requests_staged') }} approval_requests_cte
    {{ schema_join(
    join_expr="source_cte.id = approval_requests_cte.purchase_order_id",
    join_table="source_cte",
    source_table="approval_requests_cte"
) }}
    where approval_requests_cte.is_deleted = false
    group by source_cte.ion_uid
),
assigned_to_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('assigned_to_cte')}}
    ) as assigned_to
    from {{ ref('users_staged') }} assigned_to_cte
    {{ schema_join(
    join_expr="source_cte.assigned_to_id = assigned_to_cte.id",
    join_table="source_cte",
    source_table="assigned_to_cte"
) }}
    where assigned_to_cte.is_deleted = false
),
ship_to_location_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('ship_to_location_cte')}}
    ) as ship_to_location
    from {{ ref('locations_staged') }} ship_to_location_cte
    {{ schema_join(
    join_expr="source_cte.ship_to_location_id = ship_to_location_cte.id",
    join_table="source_cte",
    source_table="ship_to_location_cte"
) }}
    where ship_to_location_cte.is_deleted = false
),
bill_to_location_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('bill_to_location_cte')}}
    ) as bill_to_location
    from {{ ref('locations_staged') }} bill_to_location_cte
    {{ schema_join(
    join_expr="source_cte.bill_to_location_id = bill_to_location_cte.id",
    join_table="source_cte",
    source_table="bill_to_location_cte"
) }}
    where bill_to_location_cte.is_deleted = false
),
fees_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('fees_cte')}}
      )
    ) as fees
    from {{ ref('purchase_order_fees_staged') }} fees_cte
    {{ schema_join(
    join_expr="source_cte.id = fees_cte.purchase_order_id",
    join_table="source_cte",
    source_table="fees_cte"
) }}
    where fees_cte.is_deleted = false
    group by source_cte.ion_uid
),
purchase_order_lines_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
            {{ select_star_exclude_ab_cols(
                'purchase_order_lines_cte',
                [
                    'purchase_order',
                    'part',
                    'part_inventories',
                    'receipts',
                    'received_part_inventories',
                    'receipt_part_inventories',
                    'issues',
                    'comments',
                    'requirements'
                ]
            )}}
      )
    ) as purchase_order_lines
    from {{ ref('purchase_order_lines_denormalized') }} purchase_order_lines_cte
    {{ schema_join(
    join_expr="source_cte.id = purchase_order_lines_cte.purchase_order_id",
    join_table="source_cte",
    source_table="purchase_order_lines_cte"
) }}
    where purchase_order_lines_cte.is_deleted = false
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
    from {{ ref('purchase_orders_attributes_staged') }} attrs
    {{ schema_join(
    join_expr="source_cte.id = attrs.purchase_order_id",
    join_table="source_cte",
    source_table="attrs"
    ) }}
    where attrs.is_deleted = false
    group by source_cte.ion_uid
),
requirements_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('requirements')}}
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
source_cte_with_calc_props as (
    select source_cte.*,
    purchase_order_lines_cte.purchase_order_lines,
    CASE
        WHEN forall(purchase_order_lines_cte.purchase_order_lines.status, status -> status == 'canceled')
        THEN 'canceled'
        WHEN forall(purchase_order_lines_cte.purchase_order_lines.status, status -> status == 'received')
        THEN 'received'
        WHEN array_contains(purchase_order_lines_cte.purchase_order_lines.status, 'draft')
        THEN 'draft'
        WHEN array_contains(purchase_order_lines_cte.purchase_order_lines.status, 'requested')
        THEN 'requested'
        WHEN array_contains(purchase_order_lines_cte.purchase_order_lines.status, 'approved')
        THEN 'approved'
        WHEN array_contains(purchase_order_lines_cte.purchase_order_lines.status, 'ordered')
        THEN 'ordered'
        ELSE 'draft'
    END AS status,
    forall(purchase_order_lines_cte.purchase_order_lines.paid, paid -> paid == true) as paid,
    aggregate(
        purchase_order_lines_cte.purchase_order_lines.estimated_cost,
        cast(0 as double),
        (acc, estimated_cost) -> acc + estimated_cost
    ) as estimated_cost,
    array_max(purchase_order_lines_cte.purchase_order_lines.estimated_arrival_date) as estimated_arrival_date,
    array_min(purchase_order_lines_cte.purchase_order_lines.need_date) as need_date
    FROM source_cte
    left outer join purchase_order_lines_cte on
    purchase_order_lines_cte.ion_uid = source_cte.ion_uid
),
source_cte_with_users as (
    {{ users_select('source_cte_with_calc_props') }}
)
select source_cte_with_users.*,
source_cte_with_users.status = 'draft' as editable,
source_cte_with_users.status in ('draft', 'requested', 'approved', 'ordered') as is_open,
aggregate(
    fees_cte.fees,
    source_cte_with_users.estimated_cost,
    (acc, fee) -> acc + if(
        fee.type == 'percentage',
        (fee.value / 100.0) * source_cte_with_users.estimated_cost,
        fee.value
    )
) as estimated_total_cost,
supplier_cte.supplier,
currency_cte.currency,
approvals_cte.approvals,
approval_requests_cte.approval_requests,
assigned_to_cte.assigned_to,
ship_to_location_cte.ship_to_location,
bill_to_location_cte.bill_to_location,
fees_cte.fees,
labels_cte.labels,
attributes_cte.attributes,
requirements_cte.requirements
FROM source_cte_with_users
left outer join supplier_cte on supplier_cte.ion_uid = source_cte_with_users.ion_uid
left outer join currency_cte on currency_cte.ion_uid = source_cte_with_users.ion_uid
left outer join approvals_cte on approvals_cte.ion_uid = source_cte_with_users.ion_uid
left outer join approval_requests_cte on approval_requests_cte.ion_uid = source_cte_with_users.ion_uid
left outer join assigned_to_cte on assigned_to_cte.ion_uid = source_cte_with_users.ion_uid
left outer join ship_to_location_cte on ship_to_location_cte.ion_uid = source_cte_with_users.ion_uid
left outer join bill_to_location_cte on bill_to_location_cte.ion_uid = source_cte_with_users.ion_uid
left outer join fees_cte on fees_cte.ion_uid = source_cte_with_users.ion_uid
left outer join labels_cte on labels_cte.ion_uid = source_cte_with_users.ion_uid
left outer join attributes_cte on attributes_cte.ion_uid = source_cte_with_users.ion_uid
left outer join requirements_cte on requirements_cte.ion_uid = source_cte_with_users.ion_uid
left outer join file_attachments_cte on file_attachments_cte.ion_uid = source_cte_with_users.ion_uid
