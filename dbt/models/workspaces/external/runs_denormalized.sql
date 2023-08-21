with source_cte as (
    select
    {{ select_star_exclude_ab_cols(ref('runs_staged'))}}
    from {{ ref('runs_staged') }}
    {{ common_incremental() }}
),
procedure_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('procedure_cte') }}
    ) as procedure
    from {{ ref('procedures_staged') }} procedure_cte
    {{ schema_join(
    join_expr="source_cte.procedure_id = procedure_cte.id",
    join_table="source_cte",
    source_table="procedure_cte"
) }}
    where procedure_cte.is_deleted = false
),
assigned_to_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('assigned_to_cte') }}
    ) as assigned_to
    from {{ ref('users_staged') }} assigned_to_cte
    {{ schema_join(
    join_expr="source_cte.assigned_to_id = assigned_to_cte.id",
    join_table="source_cte",
    source_table="assigned_to_cte"
) }}
    where assigned_to_cte.is_deleted = false
),
run_steps_cte as (
    select
        source_cte.ion_uid,
        collect_list(
            struct(
                {{ 
                    select_star_exclude_ab_cols(
                        'rs',
                        [
                            'run',
                            'fields',
                            'assigned_to',
                            'upstream',
                            'downstream',
                            'started_by',
                            'completed_by',
                            'redlines',
                            'issues',
                            'part_inventories',
                            'datagrid_columns',
                            'datagrid_rows',
                            'location',
                            'steps',
                            'origin_step',
                            'parent',
                            'comments',
                            'attributes'
                        ]
                    )
                }}
            )
        ) as steps,
        min(rs.start_time) as start_time,
        max(rs.end_time) as end_time,
        min(rs.scheduled_start) as scheduled_start,
        max(rs.scheduled_end) as scheduled_end,
        collect_list(
            rs.location
        ) as locations,
        collect_list(
            rs.redlines
        ) as redlines,
        collect_list(
            rs.issues
        ) as issues,
        collect_list(
            rs.sessions
        ) as sessions
    from {{ ref('run_steps_denormalized') }} rs
    {{ schema_join(
    join_expr="source_cte.id = rs.run_id",
    join_table="source_cte",
    source_table="rs"
    ) }}
    group by source_cte.ion_uid
),
part_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('part_cte') }}
    ) as part
    from {{ ref('parts_staged') }} part_cte
    {{ schema_join(
    join_expr="source_cte.part_id = part_cte.id",
    join_table="source_cte",
    source_table="part_cte"
) }}
    where part_cte.is_deleted = false
),
part_inventory_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('part_inventory_cte') }}
    ) as part_inventory
    from {{ ref('parts_inventory_staged') }} part_inventory_cte
    {{ schema_join(
    join_expr="source_cte.part_inventory_id = part_inventory_cte.id",
    join_table="source_cte",
    source_table="part_inventory_cte"
) }}
    where part_inventory_cte.is_deleted = false
),
part_kits_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('part_kits_cte') }}
      )
    ) as part_kits
    from {{ ref('parts_kits_staged') }} part_kits_cte
    {{ schema_join(
    join_expr="source_cte.id = part_kits_cte.run_id",
    join_table="source_cte",
    source_table="part_kits_cte"
) }}
    where part_kits_cte.is_deleted = false
    group by source_cte.ion_uid
),
run_batch_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('run_batch_cte') }}
    ) as run_batch
    from {{ ref('run_batches_staged') }} run_batch_cte
    {{ schema_join(
    join_expr="source_cte.run_batch_id = run_batch_cte.id",
    join_table="source_cte",
    source_table="run_batch_cte"
) }}
    where run_batch_cte.is_deleted = false
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
    from {{ ref('runs_attributes_staged') }} attrs
    {{ schema_join(
    join_expr="source_cte.id = attrs.run_id",
    join_table="source_cte",
    source_table="attrs"
    ) }}
    where attrs.is_deleted = false
    group by source_cte.ion_uid
),
run_calc_properties as (
    select
    run.*,
    run.run_batch_id is not NULL as batched,
    run_steps_cte.start_time,
    run_steps_cte.end_time,
    run_steps_cte.scheduled_start,
    run_steps_cte.scheduled_end,
    CASE
        WHEN (run_steps_cte.steps.status is null) THEN 'complete'
        WHEN (array_contains(run_steps_cte.steps.status, 'redline')) THEN 'redline'
        WHEN (array_contains(run_steps_cte.steps.status, 'hold')) THEN 'hold'
        WHEN (array_contains(run_steps_cte.steps.status, 'failed')) THEN 'failed'
        WHEN (forall(run_steps_cte.steps.status, status -> status == 'complete')) THEN 'complete'
        WHEN (array_contains(run_steps_cte.steps.status, 'in_progress')) THEN 'in_progress'
        WHEN (forall(run_steps_cte.steps.status, status -> status == 'todo')) THEN 'todo'
        WHEN (forall(run_steps_cte.steps.status, status -> status == 'canceled')) THEN 'canceled'
        WHEN (forall(run_steps_cte.steps.status, status -> status == 'canceled' or status == 'complete')) THEN 'partial_complete'
        WHEN (forall(run_steps_cte.steps.status, status -> status == 'canceled' or status == 'todo')) THEN 'todo'
        WHEN (array_contains(run_steps_cte.steps.status, 'complete')) THEN 'in_progress'
        ELSE 'complete'
    END AS status,
    flatten(run_steps_cte.redlines) as redlines,
    flatten(run_steps_cte.issues) as issues,
    flatten(run_steps_cte.sessions) as sessions,
    array_distinct(filter(run_steps_cte.locations, location -> location.id is not null)) as locations,
    filter(run_steps_cte.steps, step -> step.parent_id is null) as steps
    FROM source_cte as run
    left outer join run_steps_cte
    on run_steps_cte.ion_uid = run.ion_uid
),
source_cte_with_users as (
    {{ users_select('run_calc_properties') }}
)
select source_cte_with_users.*,
source_cte_with_users.status in ('todo', 'hold', 'redline', 'in_progress') as is_open,
procedure_cte.procedure,
assigned_to_cte.assigned_to,
part_cte.part,
part_inventory_cte.part_inventory,
part_kits_cte.part_kits,
run_batch_cte.run_batch,
comments_cte.comments,
attributes_cte.attributes
FROM source_cte_with_users
left outer join procedure_cte on procedure_cte.ion_uid = source_cte_with_users.ion_uid
left outer join assigned_to_cte on assigned_to_cte.ion_uid = source_cte_with_users.ion_uid
left outer join part_cte on part_cte.ion_uid = source_cte_with_users.ion_uid
left outer join part_inventory_cte on part_inventory_cte.ion_uid = source_cte_with_users.ion_uid
left outer join part_kits_cte on part_kits_cte.ion_uid = source_cte_with_users.ion_uid
left outer join run_batch_cte on run_batch_cte.ion_uid = source_cte_with_users.ion_uid
left outer join comments_cte on comments_cte.ion_uid = source_cte_with_users.ion_uid
left outer join attributes_cte on attributes_cte.ion_uid = source_cte_with_users.ion_uid