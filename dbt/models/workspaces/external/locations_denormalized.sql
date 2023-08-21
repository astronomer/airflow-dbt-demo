with source_cte as (
    select
    {{ select_star_exclude_ab_cols(ref('locations_staged')) }}
    from {{ ref('locations_staged') }}
    {{ common_incremental() }}
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
    join_expr="source_cte.id = part_inventories_cte.location_id",
    join_table="source_cte",
    source_table="part_inventories_cte"
) }}
    where part_inventories_cte.is_deleted = false
    group by source_cte.ion_uid
),
location_subtype_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('location_subtype_cte')}}
    ) as location_subtype
    from {{ ref('location_subtypes_staged') }} location_subtype_cte
    {{ schema_join(
    join_expr="source_cte.location_subtype_id = location_subtype_cte.id",
    join_table="source_cte",
    source_table="location_subtype_cte"
) }}
    where location_subtype_cte.is_deleted = false
),
supervisor_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('supervisor_cte')}}
    ) as supervisor
    from {{ ref('users_staged') }} supervisor_cte
    {{ schema_join(
    join_expr="source_cte.supervisor_id = supervisor_cte.id",
    join_table="source_cte",
    source_table="supervisor_cte"
) }}
    where supervisor_cte.is_deleted = false
),
locations_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('locations_cte')}}
      )
    ) as locations
    from {{ ref('locations_staged') }} locations_cte
    {{ schema_join(
    join_expr="source_cte.id = locations_cte.parent_id",
    join_table="source_cte",
    source_table="locations_cte"
) }}
    where locations_cte.is_deleted = false
    group by source_cte.ion_uid
),
image_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('image_cte')}}
    ) as image
    from {{ ref('file_attachments_staged') }} image_cte
    {{ schema_join(
    join_expr="source_cte.image_id = image_cte.id",
    join_table="source_cte",
    source_table="image_cte"
) }}
    where image_cte.is_deleted = false
),
contact_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('contact_cte')}}
    ) as contact
    from {{ ref('contacts_staged') }} contact_cte
    {{ schema_join(
    join_expr="source_cte.contact_id = contact_cte.id",
    join_table="source_cte",
    source_table="contact_cte"
) }}
    where contact_cte.is_deleted = false
),
part_kits_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('part_kits_cte')}}
      )
    ) as part_kits
    from {{ ref('parts_kits_staged') }} part_kits_cte
    {{ schema_join(
    join_expr="source_cte.id = part_kits_cte.location_id",
    join_table="source_cte",
    source_table="part_kits_cte"
) }}
    where part_kits_cte.is_deleted = false
    group by source_cte.ion_uid
),
expected_part_kits_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('expected_part_kits_cte')}}
      )
    ) as expected_part_kits
    from {{ ref('parts_kits_staged') }} expected_part_kits_cte
    {{ schema_join(
    join_expr="source_cte.id = expected_part_kits_cte.delivery_location_id",
    join_table="source_cte",
    source_table="expected_part_kits_cte"
) }}
    where expected_part_kits_cte.is_deleted = false
    group by source_cte.ion_uid
),
parent_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('parent_cte')}}
    ) as parent
    from {{ ref('locations_staged') }} parent_cte
    {{ schema_join(
    join_expr="source_cte.parent_id = parent_cte.id",
    join_table="source_cte",
    source_table="parent_cte"
) }}
    where parent_cte.is_deleted = false
),
run_steps_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('run_steps_cte')}}
      )
    ) as run_steps
    from {{ ref('run_steps_staged') }} run_steps_cte
    {{ schema_join(
    join_expr="source_cte.id = run_steps_cte.location_id",
    join_table="source_cte",
    source_table="run_steps_cte"
) }}
    where run_steps_cte.is_deleted = false
    group by source_cte.ion_uid
),
steps_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('steps_cte')}}
      )
    ) as steps
    from {{ ref('steps_staged') }} steps_cte
    {{ schema_join(
    join_expr="source_cte.id = steps_cte.location_id",
    join_table="source_cte",
    source_table="steps_cte"
) }}
    where steps_cte.is_deleted = false
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
    from {{ ref('locations_attributes_staged') }} attrs
    {{ schema_join(
    join_expr="source_cte.id = attrs.location_id",
    join_table="source_cte",
    source_table="attrs"
    ) }}
    where attrs.is_deleted = false
    group by source_cte.ion_uid
),
source_cte_with_users as (
    {{ users_select('source_cte') }}
)
select source_cte_with_users.*,
part_inventories_cte.part_inventories,
location_subtype_cte.location_subtype,
supervisor_cte.supervisor,
locations_cte.locations,
image_cte.image,
contact_cte.contact,
part_kits_cte.part_kits,
expected_part_kits_cte.expected_part_kits,
parent_cte.parent,
run_steps_cte.run_steps,
steps_cte.steps,
attributes_cte.attributes
FROM source_cte_with_users
left outer join part_inventories_cte on part_inventories_cte.ion_uid = source_cte_with_users.ion_uid
left outer join location_subtype_cte on location_subtype_cte.ion_uid = source_cte_with_users.ion_uid
left outer join supervisor_cte on supervisor_cte.ion_uid = source_cte_with_users.ion_uid
left outer join locations_cte on locations_cte.ion_uid = source_cte_with_users.ion_uid
left outer join image_cte on image_cte.ion_uid = source_cte_with_users.ion_uid
left outer join contact_cte on contact_cte.ion_uid = source_cte_with_users.ion_uid
left outer join part_kits_cte on part_kits_cte.ion_uid = source_cte_with_users.ion_uid
left outer join expected_part_kits_cte on expected_part_kits_cte.ion_uid = source_cte_with_users.ion_uid
left outer join parent_cte on parent_cte.ion_uid = source_cte_with_users.ion_uid
left outer join attributes_cte on attributes_cte.ion_uid = source_cte_with_users.ion_uid
left outer join run_steps_cte on run_steps_cte.ion_uid = source_cte_with_users.ion_uid
left outer join steps_cte on steps_cte.ion_uid = source_cte_with_users.ion_uid
