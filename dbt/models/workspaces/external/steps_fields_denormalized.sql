with source_cte as (
    select
    {{ select_star_exclude_ab_cols(ref('steps_fields_staged')) }}
    from {{ ref('steps_fields_staged') }}
    {{ common_incremental() }}
),
step_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('step_cte')}}
    ) as step
    from {{ ref('steps_denormalized') }} step_cte
    {{ schema_join(
    join_expr="source_cte.step_id = step_cte.id",
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
    from {{ ref('step_field_validations_staged') }} validations_cte
    {{ schema_join(
    join_expr="source_cte.id = validations_cte.field_id",
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
roles_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('roles_cte')}}
    ) as signoff_role
    from {{ ref('roles_staged') }} roles_cte
    {{ schema_join(
    join_expr="source_cte.signoff_role_id = roles_cte.id",
    join_table="source_cte",
    source_table="roles_cte"
) }}
    where roles_cte.is_deleted = false
),
source_cte_with_users as (
    {{ users_select('source_cte') }}
)
select source_cte_with_users.*,
step_cte.step,
step_cte.step.editable as editable,
validations_cte.validations,
part_subtype_cte.part_subtype,
part_cte.part,
roles_cte.signoff_role
FROM source_cte_with_users
left outer join step_cte on step_cte.ion_uid = source_cte_with_users.ion_uid
left outer join validations_cte on validations_cte.ion_uid = source_cte_with_users.ion_uid
left outer join part_subtype_cte on part_subtype_cte.ion_uid = source_cte_with_users.ion_uid
left outer join part_cte on part_cte.ion_uid = source_cte_with_users.ion_uid
left outer join roles_cte on roles_cte.ion_uid = source_cte_with_users.ion_uid