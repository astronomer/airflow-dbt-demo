with source_cte as (
    select
    {{ select_star_exclude_ab_cols(ref('run_steps_fields_staged')) }},
    {{ polymorphic_value_to_string() }} as value
    from {{ ref('run_steps_fields_staged') }}
    {{ common_incremental() }}
),
origin_field_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('origin_field_cte')}}
    ) as origin_field
    from {{ ref('steps_fields_staged') }} origin_field_cte
    {{ schema_join(
    join_expr="source_cte.origin_field_id = origin_field_cte.id",
    join_table="source_cte",
    source_table="origin_field_cte"
) }}
    where origin_field_cte.is_deleted = false
),
step_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('step_cte')}}
    ) as step
    from {{ ref('run_steps_staged') }} step_cte
    {{ schema_join(
    join_expr="source_cte.run_step_id = step_cte.id",
    join_table="source_cte",
    source_table="step_cte"
) }}
    where step_cte.is_deleted = false
),
validations_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('validations_cte')}}
      )
    ) as validations
    from {{ ref('run_step_field_validations_staged') }} validations_cte
    {{ schema_join(
    join_expr="source_cte.id = validations_cte.run_step_field_id",
    join_table="source_cte",
    source_table="validations_cte"
) }}
    where validations_cte.is_deleted = false
    group by source_cte.ion_uid
),
part_subtype_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('part_subtype_cte')}}
    ) as part_subtype
    from {{ ref('part_subtypes_staged') }} part_subtype_cte
    {{ schema_join(
    join_expr="source_cte.part_subtype_id = part_subtype_cte.id",
    join_table="source_cte",
    source_table="part_subtype_cte"
) }}
    where part_subtype_cte.is_deleted = false
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
signoff_role_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('signoff_role_cte')}}
    ) as signoff_role
    from {{ ref('roles_staged') }} signoff_role_cte
    {{ schema_join(
    join_expr="source_cte.signoff_role_id = signoff_role_cte.id",
    join_table="source_cte",
    source_table="signoff_role_cte"
) }}
    where signoff_role_cte.is_deleted = false
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
origin_field_cte.origin_field,
step_cte.step,
validations_cte.validations,
part_subtype_cte.part_subtype,
part_cte.part,
signoff_role_cte.signoff_role,
part_inventory_cte.part_inventory,
file_attachment_cte.file_attachment
FROM source_cte_with_users
left outer join origin_field_cte on origin_field_cte.ion_uid = source_cte_with_users.ion_uid
left outer join step_cte on step_cte.ion_uid = source_cte_with_users.ion_uid
left outer join validations_cte on validations_cte.ion_uid = source_cte_with_users.ion_uid
left outer join part_subtype_cte on part_subtype_cte.ion_uid = source_cte_with_users.ion_uid
left outer join part_cte on part_cte.ion_uid = source_cte_with_users.ion_uid
left outer join signoff_role_cte on signoff_role_cte.ion_uid = source_cte_with_users.ion_uid
left outer join part_inventory_cte on part_inventory_cte.ion_uid = source_cte_with_users.ion_uid
left outer join file_attachment_cte on file_attachment_cte.ion_uid = source_cte_with_users.ion_uid