with source_cte as (
    select
    {{ select_star_exclude_ab_cols(ref('parts_inventory_staged')) }}
    from {{ ref('parts_inventory_staged') }}
    {{ common_incremental() }}
),
runs_cte as (
    select
   collect_list(
       struct(
        {{ 
          select_star_exclude_ab_cols(
            'runs',
            ['procedure',
            'part',
            'part_inventory',
            'part_kits',
            'redlines',
            'issues',
            'sessions',
            'steps',
            'comments']
          )
        }},
       case
         when runs.status = 'failed' then true
         else runs.is_open
        END as open_or_failed
      )
    ) as runs,
    source_cte.ion_uid
    from {{ ref('runs_denormalized') }} runs
    {{ schema_join(
    join_expr="source_cte.id = runs.part_inventory_id",
    join_table="source_cte",
    source_table="runs"
    ) }}
    where runs.is_deleted = false
    group by source_cte.ion_uid
),
purchase_order_lines_cte as (
  select
  source_cte.ion_uid,
   collect_list(
       struct(
        {{ 
          select_star_exclude_ab_cols(
            'pol',
            ['purchase_order',
            'part',
            'part_inventories',
            'receipts',
            'received_part_inventories',
            'receipt_part_inventories',
            'receipt_items',
            'issues',
            'comments',
            'requirements']
          )
        }}
      )
    ) as purchase_order_lines
    from {{ ref('purchase_order_lines_denormalized') }} pol
    {{ schema_join(
    join_expr="poli.purchase_order_line_id = pol.id",
    join_table=ref('purchase_order_line_inventories_denormalized'),
    source_table="pol",
    join_alias="poli",
    filter_deleted=true
    ) }}
    {{ schema_join(
    join_expr="source_cte.id = poli.part_inventory_id",
    join_table="source_cte",
    source_table="poli"
    ) }}
    where pol.is_deleted = false
    group by source_cte.ion_uid
),
location_cte as (
    select
    struct(
        {{ select_star_exclude_ab_cols('locations') }}
    ) as location,
    source_cte.ion_uid
    from {{ ref('locations_staged') }} locations
    {{ schema_join(
    join_expr="source_cte.location_id = locations.id",
    join_table="source_cte",
    source_table="locations"
    ) }}
    where locations.is_deleted = false
),
part_kits_cte as (
  select
  source_cte.ion_uid,
   collect_list(
       struct(
         pki.part_id as part_kit_item_part_id,
         pki.id as part_kit_item_id,
         pk_inv.quantity as inventory_quantity,
         {{ select_star_exclude_ab_cols('pk') }}
      )
    ) as part_kits,
   collect_list(
       struct(
         pk_inv.quantity as inventory_quantity,
         {{ select_star_exclude_ab_cols('pki') }}
      )
    ) as part_kit_items,
    sum(
      case
        when pk.status != 'completed' then coalesce(pk_inv.quantity, 0)
        else 0
      END
    ) as kitted_qty
    from {{ ref('parts_kits_staged') }} pk
    {{ schema_join(
    join_expr="pk.id = pki.part_kit_id",
    join_table=ref('part_kit_items_staged'),
    source_table="pk",
    join_alias="pki",
    filter_deleted=true
    ) }}
    {{ schema_join(
    join_expr="pk_inv.part_kit_item_id = pki.id",
    join_table=ref('part_kit_inventories_staged'),
    source_table="pki",
    join_alias="pk_inv",
    filter_deleted=true
    ) }}
    {{ schema_join(
    join_expr="source_cte.id = pk_inv.part_inventory_id",
    join_table="source_cte",
    source_table="pk_inv"
    ) }}
    where pk.is_deleted = false
    group by source_cte.ion_uid
),
abom_items_cte as (
  select
  source_cte.ion_uid,
   collect_set(
       struct(
         {{ select_star_exclude_ab_cols('ai') }}
      )
    ) as abom_items,
    coalesce(
        aggregate(
          collect_set(
                  named_struct(
                    "abom_quantity", ai.quantity,
                    "abom_id", ai.id,
                    "parent", ae.parent_abom_item_id is null
                )
              ),
          cast(0 as double),
          (acc, x) -> acc + if(x.parent, cast(0 as double), x.abom_quantity)
      ), cast(0 as double)
    ) as quantity_installed
    from {{ ref('abom_items_staged') }} ai
    {{ schema_join(
    join_expr="ai.id = ae.child_abom_item_id",
    join_table=ref('abom_edges_staged'),
    source_table="ai",
    join_alias="ae",
    join_type="left outer join",
    filter_deleted=true
    ) }}
    {{ schema_join(
    join_expr="source_cte.id = ai.part_inventory_id",
    join_table="source_cte",
    source_table="ai"
    ) }}
    where ai.is_deleted = false
    group by source_cte.ion_uid
),
abom_parents_cte as (
  select
  source_cte.ion_uid,
   collect_list(
       struct(
         {{ select_star_exclude_ab_cols('abom_parents_cte') }}
      )
    ) as abom_parents
    from {{ ref('abom_items_staged') }} abom_parents_cte
    {{ schema_join(
    join_expr="abom_parents_cte.id = ae.parent_abom_item_id",
    join_table=ref('abom_edges_staged'),
    source_table="abom_parents_cte",
    join_alias="ae",
    filter_deleted=true
    ) }}
    {{ schema_join(
    join_expr="abom_items.id = ae.child_abom_item_id",
    join_table=ref('abom_items_staged'),
    source_table="ae",
    join_alias="abom_items",
    filter_deleted=true
    ) }}
    {{ schema_join(
    join_expr="source_cte.id = abom_items.part_inventory_id",
    join_table="source_cte",
    source_table="abom_items"
    ) }}
    where abom_parents_cte.is_deleted = false
    group by source_cte.ion_uid
),
abom_children_cte as (
  select
  source_cte.ion_uid,
   collect_list(
       struct(
         {{ select_star_exclude_ab_cols('abom_children_cte') }}
      )
    ) as abom_children
    from {{ ref('abom_items_staged') }} abom_children_cte
    {{ schema_join(
    join_expr="abom_children_cte.id = ae.child_abom_item_id",
    join_table=ref('abom_edges_staged'),
    source_table="abom_children_cte",
    join_alias="ae",
    filter_deleted=true
    ) }}
    {{ schema_join(
    join_expr="abom_items.id = ae.parent_abom_item_id",
    join_table=ref('abom_items_staged'),
    source_table="ae",
    join_alias="abom_items",
    filter_deleted=true
    ) }}
    {{ schema_join(
    join_expr="source_cte.id = abom_items.part_inventory_id",
    join_table="source_cte",
    source_table="abom_items"
    ) }}
    where abom_children_cte.is_deleted = false
    group by source_cte.ion_uid
),
receipts_cte as (
  select
  source_cte.ion_uid,
   collect_list(
     struct(
        {{ select_star_exclude_ab_cols('receipts') }},
         rpi.purchase_order_line_id,
         rpi.quantity,
         case
           when pol.id is null then false
           when pol.status = 'canceled' then false
           else true
         end as received
      )
    ) as receipts
    from {{ ref('receipts_staged') }} receipts
    {{ schema_join(
    join_expr="rpi.receipt_id = receipts.id",
    join_table=ref('receipt_items_staged'),
    source_table="receipts",
    join_alias="rpi",
    filter_deleted=true
    ) }}
    {{ schema_join(
    join_expr="source_cte.id = rpi.part_inventory_id",
    join_table="source_cte",
    source_table="rpi"
    ) }}
    {{ schema_join(
    join_expr="rpi.purchase_order_line_id = pol.id",
    join_table=ref('purchase_order_lines_staged'),
    source_table="rpi",
    join_alias="pol",
    join_type="left outer join"
    ) }}
    where receipts.is_deleted = false
    group by source_cte.ion_uid
),
comments_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('comments') }}
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
        {{ select_star_exclude_ab_cols('labels') }}
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
part_cte as (
    select
    struct(
        {{ select_star_exclude_ab_cols('parts') }}
    ) as part,
    source_cte.ion_uid
    from {{ ref('parts_staged') }} parts
    {{ schema_join(
    join_expr="source_cte.part_id = parts.id",
    join_table="source_cte",
    source_table="parts"
    ) }}
    where parts.is_deleted = false
),
origin_part_inventory_cte as (
    select
    struct(
        {{ select_star_exclude_ab_cols('origin_inventory') }}
    ) as origin_inventory,
    source_cte.ion_uid
    from {{ ref('parts_inventory_staged') }} origin_inventory
    {{ schema_join(
    join_expr="source_cte.origin_part_inventory_id = origin_inventory.id",
    join_table="source_cte",
    source_table="origin_inventory"
    ) }}
    where origin_inventory.is_deleted = false
),
issues_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('issues_cte')}}
      )
    ) as issues
    from {{ ref('issues_staged') }} issues_cte
    {{ schema_join(
    join_expr="issue_part_inventories.issue_id = issues_cte.id",
    join_table=ref('issue_part_inventories_staged'),
    source_table="issues_cte",
    join_alias="issue_part_inventories",
    filter_deleted=true
    ) }}
    {{ schema_join(
    join_expr="source_cte.id = issue_part_inventories.part_inventory_id",
    join_table="source_cte",
    source_table="issue_part_inventories"
    ) }}
    where issues_cte.is_deleted = false
    group by source_cte.ion_uid
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
    join_expr="source_cte.id = part_inventories_cte.origin_part_inventory_id",
    join_table="source_cte",
    source_table="part_inventories_cte"
) }}
    where part_inventories_cte.is_deleted = false
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
    from {{ ref('parts_inventories_attributes_staged') }} pas
    {{ schema_join(
    join_expr="source_cte.id = pas.part_inventory_id",
    join_table="source_cte",
    source_table="pas"
    ) }}
    where pas.is_deleted = false
    group by source_cte.ion_uid
),
inventory_properties as (
    select
        source_cte.*,
        case
        when source_cte.quantity = 0
        then "unavailable"
        when source_cte.quantity_scrapped = source_cte.quantity
        then "scrapped"
        when source_cte.quantity <= abom_items_cte.quantity_installed
        then "installed"
        when source_cte.quantity <= part_kits_cte.kitted_qty
        then "kitted"
        when array_contains(runs_cte.runs.open_or_failed, true)
        then "wip"
        when forall(receipts_cte.receipts.received, received -> received == true) and
        location_cte.location.available = true
        then "available"
        when forall(receipts_cte.receipts.received, received -> received == true) and
        location_cte.location.available = false
        then "unavailable"
        when array_contains(purchase_order_lines_cte.purchase_order_lines.is_open, true)
        then "on_order"
        when forall(purchase_order_lines_cte.purchase_order_lines.status, status -> status == "canceled")
        then "unavailable"
        when location_cte.location.available = false
        then "unavailable"
        else "available"
        end as status,
        source_cte.quantity <= part_kits_cte.kitted_qty as kitted,
        source_cte.quantity <= abom_items_cte.quantity_installed as installed,
        round(nvl(part_kits_cte.kitted_qty,0),3) as quantity_kitted,
        round(nvl(abom_items_cte.quantity_installed,0),3) as quantity_installed,
        forall(receipts_cte.receipts.received, received -> received == true) as received,
        (
            source_cte.lot_number is not null or
            source_cte.serial_number is not null or
            source_cte.location_id is not null
        ) as identifiable,
        case
        when source_cte.serial_number is not null
        then "serial"
        when source_cte.lot_number is not null
        then "lot"
        end as tracking_type,
        nvl(source_cte.`_cost`, 0.0) as cost,
        round(nvl(source_cte.`_cost`, 0.0) * source_cte.quantity, 3) as estimated_total_cost,
        case
        when source_cte.last_maintained_date is null
        then "available"
        when part_cte.part is null
        then "available"
        when part_cte.part.maintenance_interval_seconds is null
        then "available"
        when cast(concat(part_cte.part.maintenance_interval_seconds, " seconds") as interval) + source_cte.last_maintained_date < now()
        then "unavailable"
        else "available"
        end as maintenance_status,
        case
        when part_cte.part is null
        then null
        when part_cte.part.sourcing_strategy = "make"
        then "made_to_stock"
        when part_cte.part.sourcing_strategy = "purchase_to_stock"
        then "purchase_to_stock"
        end as usage_type,
        comments_cte.comments as comments,
        labels_cte.labels as labels,
        part_cte.part as part,
        runs_cte.runs as runs,
        purchase_order_lines_cte.purchase_order_lines as purchase_order_lines,
        location_cte.location as location,
        part_kits_cte.part_kits as part_kits,
        part_kits_cte.part_kit_items,
        receipts_cte.receipts as receipts,
        abom_items_cte.abom_items as abom_items,
        extensible_attributes_cte.attributes as attributes,
        origin_part_inventory_cte.origin_inventory as origin_inventory,
        issues_cte.issues,
        supplier_cte.supplier,
        part_inventories_cte.part_inventories,
        file_attachments_cte.file_attachments,
        abom_children_cte.abom_children,
        abom_parents_cte.abom_parents
    from source_cte
        left outer join runs_cte on
        source_cte.ion_uid = runs_cte.ion_uid
        left outer join purchase_order_lines_cte on
        source_cte.ion_uid = purchase_order_lines_cte.ion_uid
        left outer join location_cte on
        source_cte.ion_uid = location_cte.ion_uid
        left outer join part_kits_cte on
        source_cte.ion_uid = part_kits_cte.ion_uid
        left outer join abom_items_cte on
        source_cte.ion_uid = abom_items_cte.ion_uid
        left outer join receipts_cte on
        source_cte.ion_uid = receipts_cte.ion_uid
        left outer join comments_cte on
        source_cte.ion_uid = comments_cte.ion_uid
        left outer join labels_cte on
        source_cte.ion_uid = labels_cte.ion_uid
        left outer join part_cte on
        source_cte.ion_uid = part_cte.ion_uid
        left outer join origin_part_inventory_cte on
        source_cte.ion_uid = origin_part_inventory_cte.ion_uid
        left outer join extensible_attributes_cte on
        source_cte.ion_uid = extensible_attributes_cte.ion_uid
        left outer join issues_cte on
        source_cte.ion_uid = issues_cte.ion_uid
        left outer join supplier_cte on
        source_cte.ion_uid = supplier_cte.ion_uid
        left outer join file_attachments_cte on
        source_cte.ion_uid = file_attachments_cte.ion_uid
        left outer join part_inventories_cte on
        source_cte.ion_uid = part_inventories_cte.ion_uid
        left outer join abom_children_cte on
        abom_children_cte.ion_uid = source_cte.ion_uid
        left outer join abom_parents_cte on
        abom_parents_cte.ion_uid = source_cte.ion_uid
),
final_cte as (
    {{ users_select('inventory_properties') }}
)
select
*,
case
when final_cte.status in ("wip", "on_order", "unavailable")
then 0.0
else round(nvl(final_cte.quantity - greatest(final_cte.quantity_kitted, final_cte.quantity_installed) - final_cte.quantity_scrapped,0),3)
end as quantity_available
FROM final_cte