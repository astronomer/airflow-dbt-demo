with source_cte as (
    select
    {{ select_star_exclude_ab_cols(ref('plan_items_staged')) }}
    from {{ ref('plan_items_staged') }}
    {{ common_incremental() }}
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
mrp_job_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('mrp_job_cte')}}
    ) as mrp_job
    from {{ ref('mrp_jobs_staged') }} mrp_job_cte
    {{ schema_join(
    join_expr="source_cte.mrp_job_id = mrp_job_cte.id",
    join_table="source_cte",
    source_table="mrp_job_cte"
) }}
    where mrp_job_cte.is_deleted = false
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
procedure_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('procedure_cte')}}
    ) as procedure
    from {{ ref('procedures_staged') }} procedure_cte
    {{ schema_join(
    join_expr="source_cte.procedure_id = procedure_cte.id",
    join_table="source_cte",
    source_table="procedure_cte"
) }}
    where procedure_cte.is_deleted = false
),
plans_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('plans_cte')}}
      )
    ) as plans
    from {{ ref('plans_staged') }} plans_cte
    {{ schema_join(
    join_expr="plan_plan_items.plan_id = plans_cte.id",
    join_table=ref('plan_plan_items_staged'),
    source_table="plans_cte",
    join_alias="plan_plan_items",
    filter_deleted=true
) }}
    {{ schema_join(
    join_expr="source_cte.id = plan_plan_items.plan_item_id",
    join_table="source_cte",
    source_table="plan_plan_items"
) }}
    where plans_cte.is_deleted = false
    group by source_cte.ion_uid
),
inputs_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('inputs_cte')}}
      )
    ) as inputs
    from {{ ref('plan_inputs_staged') }} inputs_cte
    {{ schema_join(
    join_expr="plan_plan_items.plan_input_id = inputs_cte.id",
    join_table=ref('plan_plan_items_staged'),
    source_table="inputs_cte",
    join_alias="plan_plan_items",
    filter_deleted=true
) }}
    {{ schema_join(
    join_expr="source_cte.id = plan_plan_items.plan_item_id",
    join_table="source_cte",
    source_table="plan_plan_items"
) }}
    where inputs_cte.is_deleted = false
    group by source_cte.ion_uid
),
result_entities_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('result_entities_cte')}}
      )
    ) as result_entities
    from {{ ref('entities_staged') }} result_entities_cte
    {{ schema_join(
    join_expr="plan_item_results.entity_id = result_entities_cte.id",
    join_table=ref('plan_item_results_staged'),
    source_table="result_entities_cte",
    join_alias="plan_item_results",
    filter_deleted=true
) }}
    {{ schema_join(
    join_expr="source_cte.id = plan_item_results.plan_item_id",
    join_table="source_cte",
    source_table="plan_item_results"
) }}
    where result_entities_cte.is_deleted = false
    group by source_cte.ion_uid
),
child_allocations_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('child_allocations_cte')}}
      )
    ) as child_allocations
    from {{ ref('plan_item_allocations_staged') }} child_allocations_cte
    {{ schema_join(
    join_expr="source_cte.id = child_allocations_cte.parent_plan_item_id",
    join_table="source_cte",
    source_table="child_allocations_cte"
) }}
    where child_allocations_cte.is_deleted = false
    group by source_cte.ion_uid
),
parent_allocations_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('parent_allocations_cte')}}
      )
    ) as parent_allocations
    from {{ ref('plan_item_allocations_staged') }} parent_allocations_cte
    {{ schema_join(
    join_expr="source_cte.id = parent_allocations_cte.child_plan_item_id",
    join_table="source_cte",
    source_table="parent_allocations_cte"
) }}
    where parent_allocations_cte.is_deleted = false
    group by source_cte.ion_uid
),
source_cte_with_users as (
    {{ users_select('source_cte') }}
)
select source_cte_with_users.*,
part_cte.part,
mrp_job_cte.mrp_job,
assigned_to_cte.assigned_to,
supplier_cte.supplier,
procedure_cte.procedure,
plans_cte.plans,
inputs_cte.inputs,
result_entities_cte.result_entities,
child_allocations_cte.child_allocations,
parent_allocations_cte.parent_allocations
FROM source_cte_with_users
left outer join part_cte on part_cte.ion_uid = source_cte_with_users.ion_uid
left outer join mrp_job_cte on mrp_job_cte.ion_uid = source_cte_with_users.ion_uid
left outer join assigned_to_cte on assigned_to_cte.ion_uid = source_cte_with_users.ion_uid
left outer join supplier_cte on supplier_cte.ion_uid = source_cte_with_users.ion_uid
left outer join procedure_cte on procedure_cte.ion_uid = source_cte_with_users.ion_uid
left outer join plans_cte on plans_cte.ion_uid = source_cte_with_users.ion_uid
left outer join inputs_cte on inputs_cte.ion_uid = source_cte_with_users.ion_uid
left outer join result_entities_cte on result_entities_cte.ion_uid = source_cte_with_users.ion_uid
left outer join child_allocations_cte on child_allocations_cte.ion_uid = source_cte_with_users.ion_uid
left outer join parent_allocations_cte on parent_allocations_cte.ion_uid = source_cte_with_users.ion_uid