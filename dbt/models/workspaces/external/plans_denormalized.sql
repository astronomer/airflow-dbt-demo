with source_cte as (
    select
    {{ select_star_exclude_ab_cols(ref('plans_staged')) }}
    from {{ ref('plans_staged') }}
    {{ common_incremental() }}
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
    join_expr="source_cte.id = inputs_cte.plan_id",
    join_table="source_cte",
    source_table="inputs_cte"
) }}
    where inputs_cte.is_deleted = false
    group by source_cte.ion_uid
),
plan_items_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('plan_items_cte')}}
      )
    ) as plan_items
    from {{ ref('plan_items_staged') }} plan_items_cte
    {{ schema_join(
    join_expr="plan_plan_items.plan_item_id = plan_items_cte.id",
    join_table=ref('plan_plan_items_staged'),
    source_table="plan_items_cte",
    join_alias="plan_plan_items",
    filter_deleted=true
) }}
    {{ schema_join(
    join_expr="source_cte.id = plan_plan_items.plan_id",
    join_table="source_cte",
    source_table="plan_plan_items"
) }}
    where plan_items_cte.is_deleted = false
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
inputs_cte.inputs,
plan_items_cte.plan_items,
comments_cte.comments,
labels_cte.labels,
file_attachments_cte.file_attachments
FROM source_cte_with_users
left outer join inputs_cte on inputs_cte.ion_uid = source_cte_with_users.ion_uid
left outer join plan_items_cte on plan_items_cte.ion_uid = source_cte_with_users.ion_uid
left outer join comments_cte on comments_cte.ion_uid = source_cte_with_users.ion_uid
left outer join labels_cte on labels_cte.ion_uid = source_cte_with_users.ion_uid
left outer join file_attachments_cte on file_attachments_cte.ion_uid = source_cte_with_users.ion_uid