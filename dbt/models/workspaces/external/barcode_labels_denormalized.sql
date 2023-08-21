with source_cte as (
    select
    {{ select_star_exclude_ab_cols(ref('barcode_labels_staged')) }}
    from {{ ref('barcode_labels_staged') }}
    {{ common_incremental() }}
),
template_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('template_cte')}}
    ) as template
    from {{ ref('barcode_templates_staged') }} template_cte
    {{ schema_join(
    join_expr="source_cte.template_id = template_cte.id",
    join_table="source_cte",
    source_table="template_cte"
) }}
    where template_cte.is_deleted = false
),
label_preview_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('label_preview_cte')}}
    ) as label_preview
    from {{ ref('file_attachments_staged') }} label_preview_cte
    {{ schema_join(
    join_expr="source_cte.label_preview_id = label_preview_cte.id",
    join_table="source_cte",
    source_table="label_preview_cte"
) }}
    where label_preview_cte.is_deleted = false
),
source_cte_with_users as (
    {{ users_select('source_cte') }}
)
select source_cte_with_users.*,
template_cte.template,
label_preview_cte.label_preview
FROM source_cte_with_users
left outer join template_cte on template_cte.ion_uid = source_cte_with_users.ion_uid
left outer join label_preview_cte on label_preview_cte.ion_uid = source_cte_with_users.ion_uid