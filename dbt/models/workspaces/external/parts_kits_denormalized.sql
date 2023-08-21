with source_cte as (
    select
    {{ select_star_exclude_ab_cols(ref('parts_kits_staged')) }}
    from {{ ref('parts_kits_staged') }}
    {{ common_incremental() }}
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
location_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('location_cte')}}
    ) as location
    from {{ ref('locations_staged') }} location_cte
    {{ schema_join(
    join_expr="source_cte.location_id = location_cte.id",
    join_table="source_cte",
    source_table="location_cte"
) }}
    where location_cte.is_deleted = false
),
delivery_location_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('delivery_location_cte')}}
    ) as delivery_location
    from {{ ref('locations_staged') }} delivery_location_cte
    {{ schema_join(
    join_expr="source_cte.delivery_location_id = delivery_location_cte.id",
    join_table="source_cte",
    source_table="delivery_location_cte"
) }}
    where delivery_location_cte.is_deleted = false
),
run_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('run_cte')}}
    ) as run
    from {{ ref('runs_staged') }} run_cte
    {{ schema_join(
    join_expr="source_cte.run_id = run_cte.id",
    join_table="source_cte",
    source_table="run_cte"
) }}
    where run_cte.is_deleted = false
),
part_kit_inventories_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('part_kit_inventories_cte')}}
      )
    ) as part_kit_inventories
    from {{ ref('part_kit_inventories_staged') }} part_kit_inventories_cte
    {{ schema_join(
    join_expr="part_kit_items.id = part_kit_inventories_cte.part_kit_item_id",
    join_table=ref('part_kit_items_staged'),
    source_table="part_kit_inventories_cte",
    join_alias="part_kit_items",
    filter_deleted=true
) }}
    {{ schema_join(
    join_expr="source_cte.id = part_kit_items.part_kit_id",
    join_table="source_cte",
    source_table="part_kit_items"
) }}
    where part_kit_inventories_cte.is_deleted = false
    group by source_cte.ion_uid
),
part_kit_items_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        part_kit_items_cte.*
      )
    ) as part_kit_items,
    sum(
        case
        when part_kit_items_cte.fulfilled
        then 1.0
        else 0.0
        end
    ) as total_items_fulfilled,
    count(part_kit_items_cte.id) as total_items_count
    from {{ ref('part_kit_items_denormalized') }} part_kit_items_cte
    {{ schema_join(
    join_expr="source_cte.id = part_kit_items_cte.part_kit_id",
    join_table="source_cte",
    source_table="part_kit_items_cte"
) }}
    where part_kit_items_cte.is_deleted = false
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
    from {{ ref('part_kit_attributes_staged') }} attrs
    {{ schema_join(
    join_expr="source_cte.id = attrs.part_kit_id",
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
assigned_to_cte.assigned_to,
location_cte.location,
delivery_location_cte.delivery_location,
run_cte.run,
part_kit_inventories_cte.part_kit_inventories,
flatten(part_kit_items_cte.part_kit_items.part_inventories) as part_inventories,
ceil(
               case
                   when part_kit_items_cte.total_items_count is null
                       then 100.0
                   else cast(part_kit_items_cte.total_items_count as double) /
                        part_kit_items_cte.total_items_fulfilled * 100.0
                   end
           ) as percent_fulfilled,
forall(part_kit_items_cte.part_kit_items.fulfilled, fulfilled -> fulfilled == true) as fulfilled,
part_kit_items_cte.part_kit_items,
comments_cte.comments,
labels_cte.labels,
attributes_cte.attributes,
file_attachments_cte.file_attachments
FROM source_cte_with_users
left outer join assigned_to_cte on assigned_to_cte.ion_uid = source_cte_with_users.ion_uid
left outer join location_cte on location_cte.ion_uid = source_cte_with_users.ion_uid
left outer join delivery_location_cte on delivery_location_cte.ion_uid = source_cte_with_users.ion_uid
left outer join run_cte on run_cte.ion_uid = source_cte_with_users.ion_uid
left outer join part_kit_inventories_cte on part_kit_inventories_cte.ion_uid = source_cte_with_users.ion_uid
left outer join part_kit_items_cte on part_kit_items_cte.ion_uid = source_cte_with_users.ion_uid
left outer join comments_cte on comments_cte.ion_uid = source_cte_with_users.ion_uid
left outer join labels_cte on labels_cte.ion_uid = source_cte_with_users.ion_uid
left outer join attributes_cte on attributes_cte.ion_uid = source_cte_with_users.ion_uid
left outer join file_attachments_cte on source_cte_with_users.ion_uid = file_attachments_cte.ion_uid