with source_cte as (
    select
    {{ select_star_exclude_ab_cols(ref('parts_staged')) }}
    from {{ ref('parts_staged') }}
    {{ common_incremental() }}
),
abom_items_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        abom_items_cte.*
      )
    ) as abom_items
    from {{ ref('abom_items_staged') }} abom_items_cte
    {{ schema_join(
    join_expr="source_cte.id = abom_items_cte.part_id",
    join_table="source_cte",
    source_table="abom_items_cte"
) }}
    where abom_items_cte.is_deleted = false
    group by source_cte.ion_uid
),
inputs_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        inputs_cte.*
      )
    ) as inputs
    from {{ ref('plan_inputs_staged') }} inputs_cte
    {{ schema_join(
    join_expr="source_cte.id = inputs_cte.part_id",
    join_table="source_cte",
    source_table="inputs_cte"
) }}
    where inputs_cte.is_deleted = false
    group by source_cte.ion_uid
),
mbom_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('mbom_cte', ['part'])}}
    ) as mbom
    from {{ ref('mboms_denormalized') }} mbom_cte
    {{ schema_join(
    join_expr="source_cte.id = mbom_cte.part_id",
    join_table="source_cte",
    source_table="mbom_cte"
) }}
    where mbom_cte.is_deleted = false
),
part_kit_items_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        part_kit_items_cte.*
      )
    ) as part_kit_items
    from {{ ref('part_kit_items_staged') }} part_kit_items_cte
    {{ schema_join(
    join_expr="source_cte.id = part_kit_items_cte.part_id",
    join_table="source_cte",
    source_table="part_kit_items_cte"
) }}
    where part_kit_items_cte.is_deleted = false
    group by source_cte.ion_uid
),
part_procedures_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        part_procedures_cte.*
      )
    ) as part_procedures
    from {{ ref('part_procedures_staged') }} part_procedures_cte
    {{ schema_join(
    join_expr="source_cte.id = part_procedures_cte.part_id",
    join_table="source_cte",
    source_table="part_procedures_cte"
) }}
    where part_procedures_cte.is_deleted = false
    group by source_cte.ion_uid
),
part_subtypes_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        part_subtypes_cte.*
      )
    ) as part_subtypes
    from {{ ref('part_subtypes_staged') }} part_subtypes_cte
    {{ schema_join(
    join_expr="part_part_subtypes.part_subtype_id = part_subtypes_cte.id",
    join_table=ref('part_part_subtypes_staged'),
    source_table="part_subtypes_cte",
    join_alias="part_part_subtypes",
    filter_deleted=true,
) }}
    {{ schema_join(
    join_expr="source_cte.id = part_part_subtypes.part_id",
    join_table="source_cte",
    source_table="part_part_subtypes"
) }}
    where part_subtypes_cte.is_deleted = false
    group by source_cte.ion_uid
),
parts_inventory_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        parts_inventory_cte.*
      )
    ) as parts_inventory,
    sum(
        coalesce(parts_inventory_cte.quantity, 0)
    ) as inventory_quantity
    from {{ ref('parts_inventory_staged') }} parts_inventory_cte
    {{ schema_join(
    join_expr="source_cte.id = parts_inventory_cte.part_id",
    join_table="source_cte",
    source_table="parts_inventory_cte"
) }}
    where parts_inventory_cte.is_deleted = false
    group by source_cte.ion_uid
),
plan_items_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        plan_items_cte.*
      )
    ) as plan_items
    from {{ ref('plan_items_staged') }} plan_items_cte
    {{ schema_join(
    join_expr="source_cte.id = plan_items_cte.part_id",
    join_table="source_cte",
    source_table="plan_items_cte"
) }}
    where plan_items_cte.is_deleted = false
    group by source_cte.ion_uid
),
procedures_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        procedures_cte.*
      )
    ) as procedures
    from {{ ref('procedures_staged') }} procedures_cte
    {{ schema_join(
    join_expr="part_procedures.procedure_id = procedures_cte.id",
    join_table=ref('part_procedures_staged'),
    source_table="procedures_cte",
    join_alias="part_procedures",
    filter_deleted=true,
) }}
    {{ schema_join(
    join_expr="source_cte.id = part_procedures.part_id",
    join_table="source_cte",
    source_table="part_procedures"
) }}
    where procedures_cte.is_deleted = false
    group by source_cte.ion_uid
),
purchase_order_lines_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        purchase_order_lines_cte.*
      )
    ) as purchase_order_lines
    from {{ ref('purchase_order_lines_staged') }} purchase_order_lines_cte
    {{ schema_join(
    join_expr="source_cte.id = purchase_order_lines_cte.part_id",
    join_table="source_cte",
    source_table="purchase_order_lines_cte"
) }}
    where purchase_order_lines_cte.is_deleted = false
    group by source_cte.ion_uid
),
revisions_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        revisions_cte.*
      )
    ) as revisions
    from {{ ref('parts_staged') }} revisions_cte
    {{ schema_join(
    join_expr="source_cte.id = revisions_cte.revised_from_id",
    join_table="source_cte",
    source_table="revisions_cte"
) }}
    where revisions_cte.is_deleted = false
    group by source_cte.ion_uid
),
runs_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        runs_cte.*
      )
    ) as runs
    from {{ ref('runs_staged') }} runs_cte
    {{ schema_join(
    join_expr="source_cte.id = runs_cte.part_id",
    join_table="source_cte",
    source_table="runs_cte"
) }}
    where runs_cte.is_deleted = false
    group by source_cte.ion_uid
),
thumbnail_cte as (
    select
    source_cte.ion_uid,
    struct(
        thumbnail_cte.*
    ) as thumbnail
    from {{ ref('file_attachments_staged') }} thumbnail_cte
    {{ schema_join(
    join_expr="source_cte.thumbnail_id = thumbnail_cte.id",
    join_table="source_cte",
    source_table="thumbnail_cte"
) }}
    where thumbnail_cte.is_deleted = false
),
unit_of_measure_cte as (
    select
    source_cte.ion_uid,
    struct(
        unit_of_measure_cte.*
    ) as unit_of_measure
    from {{ ref('units_of_measurements_staged') }} unit_of_measure_cte
    {{ schema_join(
    join_expr="source_cte.unit_of_measure_id = unit_of_measure_cte.id",
    join_table="source_cte",
    source_table="unit_of_measure_cte"
) }}
    where unit_of_measure_cte.is_deleted = false
),
revised_from_cte as (
    select
    source_cte.ion_uid,
    struct(
        revised_from_cte.*
    ) as revised_from
    from {{ ref('parts_staged') }} revised_from_cte
    {{ schema_join(
    join_expr="source_cte.revised_from_id = revised_from_cte.id",
    join_table="source_cte",
    source_table="revised_from_cte"
) }}
    where revised_from_cte.is_deleted = false
),
attr_org_settings as (
  select
    explode(
        json_object_keys(
            get_json_object(
                settings, "$.parts.attributes"
            )
        )
    ) as key,
    settings,
    _org_name
  from  {{ ref('organizations_staged') }}
),
attr_model_settings as (
    select
    collect_list(
        struct(
            get_json_object(
                settings,
                concat("$.parts.attributes.", key, ".type")
            ) as type,
            key,
            cast(null as string) as value
        )
    ) as attrs,
    _org_name
from attr_org_settings
group by _org_name
),
attributes_cte as (
    select
    collect_list(
        struct(
            attrs.type,
            attrs.key,
            {{ polymorphic_value_to_string() }} as value
        )
    ) as attributes,
    source_cte.ion_uid
    from {{ ref('part_attributes_staged') }} attrs
    {{ schema_join(
    join_expr="source_cte.id = attrs.part_id",
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
source_cte_with_users as (
    {{ users_select('source_cte') }}
)
select source_cte_with_users.*,
parts_inventory_cte.inventory_quantity as quantity,
abom_items_cte.abom_items,
inputs_cte.inputs,
mbom_cte.mbom,
part_kit_items_cte.part_kit_items,
part_procedures_cte.part_procedures,
part_subtypes_cte.part_subtypes,
parts_inventory_cte.parts_inventory,
plan_items_cte.plan_items,
procedures_cte.procedures,
purchase_order_lines_cte.purchase_order_lines,
revisions_cte.revisions,
runs_cte.runs,
thumbnail_cte.thumbnail,
unit_of_measure_cte.unit_of_measure,
revised_from_cte.revised_from,
case
    when attributes_cte.attributes is null
    then (
        select first(attr_model_settings.attrs)
        from attr_model_settings
        where attr_model_settings._org_name = source_cte_with_users._org_name
    )
    else array_union(
        attributes_cte.attributes,
        (
            select first(attr_model_settings.attrs)
            from attr_model_settings
            where attr_model_settings._org_name = source_cte_with_users._org_name
        )
    )
END as attributes,
requirements_cte.requirements
FROM source_cte_with_users
left outer join abom_items_cte on abom_items_cte.ion_uid = source_cte_with_users.ion_uid
left outer join inputs_cte on inputs_cte.ion_uid = source_cte_with_users.ion_uid
left outer join mbom_cte on mbom_cte.ion_uid = source_cte_with_users.ion_uid
left outer join part_kit_items_cte on part_kit_items_cte.ion_uid = source_cte_with_users.ion_uid
left outer join part_procedures_cte on part_procedures_cte.ion_uid = source_cte_with_users.ion_uid
left outer join part_subtypes_cte on part_subtypes_cte.ion_uid = source_cte_with_users.ion_uid
left outer join parts_inventory_cte on parts_inventory_cte.ion_uid = source_cte_with_users.ion_uid
left outer join plan_items_cte on plan_items_cte.ion_uid = source_cte_with_users.ion_uid
left outer join procedures_cte on procedures_cte.ion_uid = source_cte_with_users.ion_uid
left outer join purchase_order_lines_cte on purchase_order_lines_cte.ion_uid = source_cte_with_users.ion_uid
left outer join revisions_cte on revisions_cte.ion_uid = source_cte_with_users.ion_uid
left outer join runs_cte on runs_cte.ion_uid = source_cte_with_users.ion_uid
left outer join thumbnail_cte on thumbnail_cte.ion_uid = source_cte_with_users.ion_uid
left outer join unit_of_measure_cte on unit_of_measure_cte.ion_uid = source_cte_with_users.ion_uid
left outer join revised_from_cte on revised_from_cte.ion_uid = source_cte_with_users.ion_uid
left outer join attributes_cte on attributes_cte.ion_uid = source_cte_with_users.ion_uid
left outer join requirements_cte on requirements_cte.ion_uid = source_cte_with_users.ion_uid
