with source_cte as (
    select
    {{ select_star_exclude_ab_cols(ref('run_steps_staged'))}}
    from {{ ref('run_steps_staged') }}
    {{ common_incremental() }}
),
run_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('run_cte') }}
    ) as run
    from {{ ref('runs_staged') }} run_cte
    {{ schema_join(
    join_expr="source_cte.run_id = run_cte.id",
    join_table="source_cte",
    source_table="run_cte"
) }}
    where run_cte.is_deleted = false
),
fields_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('fields_cte') }}
      )
    ) as fields
    from {{ ref('run_steps_fields_staged') }} fields_cte
    {{ schema_join(
    join_expr="source_cte.id = fields_cte.run_step_id",
    join_table="source_cte",
    source_table="fields_cte"
) }}
    where fields_cte.is_deleted = false
    group by source_cte.ion_uid
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
upstream_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('upstream_steps') }}
      )
    ) as upstream
    from {{ ref('run_steps_staged') }} upstream_steps
    {{ schema_join(
    join_expr="upstream_steps.id = upstream_cte.upstream_step_id",
    join_table=ref('run_steps_dags_staged'),
    source_table="upstream_steps",
    join_alias="upstream_cte",
    filter_deleted=true
    ) }}
    {{ schema_join(
    join_expr="source_cte.id = upstream_cte.step_id",
    join_table="source_cte",
    source_table="upstream_cte"
) }}
    where upstream_steps.is_deleted = false
    group by source_cte.ion_uid
),
downstream_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('downstream_steps') }}
      )
    ) as downstream
    from {{ ref('run_steps_staged') }} downstream_steps
    {{ schema_join(
    join_expr="downstream_steps.id = downstream_cte.step_id",
    join_table=ref('run_steps_dags_staged'),
    source_table="downstream_steps",
    join_alias="downstream_cte",
    filter_deleted=true
    ) }}
    {{ schema_join(
    join_expr="source_cte.id = downstream_cte.upstream_step_id",
    join_table="source_cte",
    source_table="downstream_cte"
    ) }}
    where downstream_steps.is_deleted = false
    group by source_cte.ion_uid
),
started_by_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('started_by_cte') }}
    ) as started_by
    from {{ ref('users_staged') }} started_by_cte
    {{ schema_join(
    join_expr="source_cte.started_by_id = started_by_cte.id",
    join_table="source_cte",
    source_table="started_by_cte"
) }}
    where started_by_cte.is_deleted = false
),
completed_by_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('completed_by_cte') }}
    ) as completed_by
    from {{ ref('users_staged') }} completed_by_cte
    {{ schema_join(
    join_expr="source_cte.completed_by_id = completed_by_cte.id",
    join_table="source_cte",
    source_table="completed_by_cte"
) }}
    where completed_by_cte.is_deleted = false
),
redlines_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('redlines_cte') }}
      )
    ) as redlines
    from {{ ref('redlines_staged') }} redlines_cte
    {{ schema_join(
    join_expr="source_cte.id = redlines_cte.step_id",
    join_table="source_cte",
    source_table="redlines_cte"
) }}
    where redlines_cte.is_deleted = false
    group by source_cte.ion_uid
),
issues_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('issues_cte') }}
      )
    ) as issues
    from {{ ref('issues_staged') }} issues_cte
    {{ schema_join(
    join_expr="source_cte.id = issues_cte.run_step_id",
    join_table="source_cte",
    source_table="issues_cte"
) }}
    where issues_cte.is_deleted = false
    group by source_cte.ion_uid
),
part_inventories_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('part_inventories_cte') }}
      )
    ) as part_inventories
    from {{ ref('parts_inventory_staged') }} part_inventories_cte
    {{ schema_join(
    join_expr="run_step_part_inventories.part_inventory_id = part_inventories_cte.id",
    join_table=ref('run_step_part_inventories_staged'),
    source_table="part_inventories_cte",
    join_alias="run_step_part_inventories",
    filter_deleted=true,
) }}
    {{ schema_join(
    join_expr="source_cte.id = run_step_part_inventories.run_step_id",
    join_table="source_cte",
    source_table="run_step_part_inventories"
) }}
    where part_inventories_cte.is_deleted = false
    group by source_cte.ion_uid
),
datagrid_columns_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('datagrid_columns_cte') }}
      )
    ) as datagrid_columns
    from {{ ref('datagrid_columns_staged') }} datagrid_columns_cte
    {{ schema_join(
    join_expr="source_cte.id = datagrid_columns_cte.run_step_id",
    join_table="source_cte",
    source_table="datagrid_columns_cte"
) }}
    where datagrid_columns_cte.is_deleted = false
    group by source_cte.ion_uid
),
datagrid_rows_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('datagrid_rows_cte') }}
      )
    ) as datagrid_rows
    from {{ ref('datagrid_rows_staged') }} datagrid_rows_cte
    {{ schema_join(
    join_expr="source_cte.id = datagrid_rows_cte.run_step_id",
    join_table="source_cte",
    source_table="datagrid_rows_cte"
) }}
    where datagrid_rows_cte.is_deleted = false
    group by source_cte.ion_uid
),
pdf_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('pdf_cte') }}
    ) as pdf
    from {{ ref('file_attachments_staged') }} pdf_cte
    {{ schema_join(
    join_expr="source_cte.pdf_asset_id = pdf_cte.id",
    join_table="source_cte",
    source_table="pdf_cte"
) }}
    where pdf_cte.is_deleted = false
),
location_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('location_cte') }}
    ) as location
    from {{ ref('locations_staged') }} location_cte
    {{ schema_join(
    join_expr="source_cte.location_id = location_cte.id",
    join_table="source_cte",
    source_table="location_cte"
) }}
    where location_cte.is_deleted = false
),
steps_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('steps_cte') }}
      )
    ) as steps
    from {{ ref('run_steps_staged') }} steps_cte
    {{ schema_join(
    join_expr="source_cte.id = steps_cte.parent_id",
    join_table="source_cte",
    source_table="steps_cte"
) }}
    where steps_cte.is_deleted = false
    group by source_cte.ion_uid
),
origin_step_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('origin_step_cte') }}
    ) as origin_step
    from {{ ref('steps_staged') }} origin_step_cte
    {{ schema_join(
    join_expr="source_cte.origin_step_id = origin_step_cte.id",
    join_table="source_cte",
    source_table="origin_step_cte"
) }}
    where origin_step_cte.is_deleted = false
),
parent_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('parent_cte') }}
    ) as parent
    from {{ ref('run_steps_staged') }} parent_cte
    {{ schema_join(
    join_expr="source_cte.parent_id = parent_cte.id",
    join_table="source_cte",
    source_table="parent_cte"
) }}
    where parent_cte.is_deleted = false
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
sessions_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        sessions.*
      )
    ) as sessions
    from {{ ref('sessions_denormalized') }} sessions
    {{ schema_join(
    join_expr="source_cte.id = sessions.run_step_id",
    join_table="source_cte",
    source_table="sessions"
    ) }}
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
    from {{ ref('run_steps_attributes_staged') }} attrs
    {{ schema_join(
    join_expr="source_cte.id = attrs.run_step_id",
    join_table="source_cte",
    source_table="attrs"
    ) }}
    where attrs.is_deleted = false
    group by source_cte.ion_uid
),
source_cte_with_users as (
    {{ users_select('source_cte') }}
)
select
source_cte_with_users.*,
source_cte_with_users.status = 'redline' as editable,
source_cte_with_users.status = 'in_progress' as settable,
case
    when (
        source_cte_with_users.status IN ('todo', 'in_progress') and
        forall(
            upstream_cte.upstream,
            upstream_step -> upstream_step.status == 'complete'
        ) and
        (parent_cte.parent.id IS NULL OR parent_cte.parent.status = 'complete')
    ) THEN true
    else false
END as available_for_work,
CASE
when source_cte_with_users.scheduled_end is not null and source_cte_with_users.scheduled_start is not null
then unix_timestamp(source_cte_with_users.scheduled_end) - unix_timestamp(source_cte_with_users.scheduled_start)
when source_cte_with_users.lead_time is not null
then source_cte_with_users.lead_time
else 0.0
END as calculated_lead_time,
run_cte.run.run_batch_id,
run_cte.run.run_batch_id is not null as batched,
run_cte.run,
fields_cte.fields,
assigned_to_cte.assigned_to,
upstream_cte.upstream,
downstream_cte.downstream,
started_by_cte.started_by,
completed_by_cte.completed_by,
redlines_cte.redlines,
issues_cte.issues,
part_inventories_cte.part_inventories,
datagrid_columns_cte.datagrid_columns,
datagrid_rows_cte.datagrid_rows,
pdf_cte.pdf,
location_cte.location,
steps_cte.steps,
origin_step_cte.origin_step,
parent_cte.parent,
comments_cte.comments,
attributes_cte.attributes,
sessions_cte.sessions
FROM source_cte_with_users
left outer join run_cte on run_cte.ion_uid = source_cte_with_users.ion_uid
left outer join fields_cte on fields_cte.ion_uid = source_cte_with_users.ion_uid
left outer join assigned_to_cte on assigned_to_cte.ion_uid = source_cte_with_users.ion_uid
left outer join upstream_cte on upstream_cte.ion_uid = source_cte_with_users.ion_uid
left outer join downstream_cte on downstream_cte.ion_uid = source_cte_with_users.ion_uid
left outer join started_by_cte on started_by_cte.ion_uid = source_cte_with_users.ion_uid
left outer join completed_by_cte on completed_by_cte.ion_uid = source_cte_with_users.ion_uid
left outer join redlines_cte on redlines_cte.ion_uid = source_cte_with_users.ion_uid
left outer join issues_cte on issues_cte.ion_uid = source_cte_with_users.ion_uid
left outer join part_inventories_cte on part_inventories_cte.ion_uid = source_cte_with_users.ion_uid
left outer join datagrid_columns_cte on datagrid_columns_cte.ion_uid = source_cte_with_users.ion_uid
left outer join datagrid_rows_cte on datagrid_rows_cte.ion_uid = source_cte_with_users.ion_uid
left outer join pdf_cte on pdf_cte.ion_uid = source_cte_with_users.ion_uid
left outer join location_cte on location_cte.ion_uid = source_cte_with_users.ion_uid
left outer join steps_cte on steps_cte.ion_uid = source_cte_with_users.ion_uid
left outer join origin_step_cte on origin_step_cte.ion_uid = source_cte_with_users.ion_uid
left outer join parent_cte on parent_cte.ion_uid = source_cte_with_users.ion_uid
left outer join comments_cte on comments_cte.ion_uid = source_cte_with_users.ion_uid
left outer join attributes_cte on attributes_cte.ion_uid = source_cte_with_users.ion_uid
left outer join sessions_cte on sessions_cte.ion_uid = source_cte_with_users.ion_uid
