with source_cte as (
    select
    {{ select_star_exclude_ab_cols(ref('steps_staged')) }}
    from {{ ref('steps_staged') }}
    {{ common_incremental() }}
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
upstream_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('upstream_steps')}}
      )
    ) as upstream
    from {{ ref('steps_staged') }} upstream_steps
    {{ schema_join(
    join_expr="upstream_steps.id = upstream_cte.upstream_step_id",
    join_table=ref('steps_dags_staged'),
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
    from {{ ref('steps_staged') }} downstream_steps
    {{ schema_join(
    join_expr="downstream_steps.id = downstream_cte.step_id",
    join_table=ref('steps_dags_staged'),
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
standard_step_version as (
    select
    source_cte.ion_uid,
    max(standard_step_version.version) as max_std_step_version
    from {{ ref('steps_staged') }} standard_step_version
    {{ schema_join(
    join_expr="source_cte.standard_step_family_id = standard_step_version.family_id",
    join_table="source_cte",
    source_table="standard_step_version"
    ) }}
    where standard_step_version.is_deleted = false and
        standard_step_version.standard_step_status = 'released'
    group by source_cte.ion_uid
),
standard_step_cte as (
    select
    source_cte.ion_uid,
    {{ select_star_exclude_ab_cols('standard_step_cte', additional_cols=['ion_uid']) }},
    struct(
        {{ select_star_exclude_ab_cols('standard_step_cte')}}
    ) as standard_step
    from {{ ref('steps_staged') }} standard_step_cte
    {{ schema_join(
    join_expr="source_cte.standard_step_family_id = standard_step_cte.family_id",
    join_table="source_cte",
    source_table="standard_step_cte"
    ) }}
    join standard_step_version on
    standard_step_version.ion_uid = source_cte.ion_uid and
        standard_step_version.max_std_step_version = standard_step_cte.version
    where standard_step_cte.is_deleted = false and
    standard_step_cte.standard_step_status = 'released'
),
source_or_standard_step_cte as (
    select
    source_cte.ion_uid,
    source_cte.id,
    source_cte.procedure_id,
    source_cte.updated_by_id,
    source_cte.entity_id,
    source_cte._last_session_id,
    source_cte.parent_id,
    source_cte.is_deleted,
    source_cte.position,
    source_cte._etag,
    source_cte._org_name,
    source_cte.standard_step_family_id,
    source_cte.family_id,
    case
    when standard_step_cte.id is not null
    then struct(
            {{ select_star_exclude_ab_cols(
                'standard_step_cte',
                [
                    'ion_uid',
                    'id',
                    'procedure_id',
                    'updated_by_id',
                    'entity_id',
                    'position',
                    '_etag',
                    '_last_session_id',
                    'parent_id',
                    'is_deleted',
                    'position',
                    '_org_name',
                    '_etag',
                    'standard_step_family_id',
                    'family_id',
                    'standard_step'   
                ]
            )}}
        )
    else struct(
        {{ select_star_exclude_ab_cols(
            'source_cte',
            [
                'ion_uid',
                'id',
                'procedure_id',
                'updated_by_id',
                'entity_id',
                'position',
                '_etag',
                '_last_session_id',
                'parent_id',
                'is_deleted',
                'position',
                '_org_name',
                '_etag',
                'standard_step_family_id',
                'family_id'
            ])}}
        )
    END as step_values,
    case
    when standard_step_cte.id is not null
    then standard_step_cte.id
    else source_cte.id
    END as calculated_join_id,
    standard_step_cte.standard_step
    from source_cte
    left outer join standard_step_cte on
    standard_step_cte.ion_uid = source_cte.ion_uid
),
final_step_values as (
    select
    source_or_standard_step_cte.step_values.*,
    {{ select_star_exclude_ab_cols(
        'source_or_standard_step_cte',
        [
            'step_values',
        ]
    )}}
    from source_or_standard_step_cte
),
fields_cte as (
    select
    final_step_values.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('fields_cte')}}
      )
    ) as fields
    from {{ ref('steps_fields_staged') }} fields_cte
    {{ schema_join(
    join_expr="final_step_values.calculated_join_id = fields_cte.step_id",
    join_table="final_step_values",
    source_table="fields_cte"
) }}
    where fields_cte.is_deleted = false
    group by final_step_values.ion_uid
),
reviews_cte as (
    select
    final_step_values.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('reviews_cte')}}
      )
    ) as reviews
    from {{ ref('reviews_staged') }} reviews_cte
    {{ schema_join(
    join_expr="final_step_values.id = reviews_cte.step_id",
    join_table="final_step_values",
    source_table="reviews_cte"
) }}
    where reviews_cte.is_deleted = false
    group by final_step_values.ion_uid
),
location_subtype_cte as (
    select
    final_step_values.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('location_subtype_cte')}}
    ) as location_subtype
    from {{ ref('location_subtypes_staged') }} location_subtype_cte
    {{ schema_join(
    join_expr="final_step_values.location_subtype_id = location_subtype_cte.id",
    join_table="final_step_values",
    source_table="location_subtype_cte"
) }}
    where location_subtype_cte.is_deleted = false
),
derived_steps_cte as (
    select
    final_step_values.ion_uid,
    collect_list(
        struct(
                {{ select_star_exclude_ab_cols('derived_steps_cte')}}
            )
    ) as derived_steps
    from {{ ref('steps_staged') }} derived_steps_cte
    {{ schema_join(
    join_expr="final_step_values.family_id = derived_steps_cte.standard_step_family_id",
    join_table="final_step_values",
    source_table="derived_steps_cte"
) }}
    where derived_steps_cte.is_deleted = false
    group by final_step_values.ion_uid
),
pdf_cte as (
    select
    final_step_values.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('pdf_cte')}}
    ) as pdf
    from {{ ref('file_attachments_staged') }} pdf_cte
    {{ schema_join(
    join_expr="final_step_values.pdf_asset_id = pdf_cte.id",
    join_table="final_step_values",
    source_table="pdf_cte"
) }}
    where pdf_cte.is_deleted = false
),
location_cte as (
    select
    final_step_values.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('location_cte')}}
    ) as location
    from {{ ref('locations_staged') }} location_cte
    {{ schema_join(
    join_expr="final_step_values.location_id = location_cte.id",
    join_table="final_step_values",
    source_table="location_cte"
) }}
    where location_cte.is_deleted = false
),
steps_cte as (
    select
    final_step_values.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('steps_cte')}}
      )
    ) as steps
    from {{ ref('steps_staged') }} steps_cte
    {{ schema_join(
    join_expr="final_step_values.calculated_join_id = steps_cte.parent_id",
    join_table="final_step_values",
    source_table="steps_cte"
) }}
    where steps_cte.is_deleted = false
    group by final_step_values.ion_uid
),
origin_step_cte as (
    select
    final_step_values.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('origin_step_cte')}}
    ) as origin_step
    from {{ ref('steps_staged') }} origin_step_cte
    {{ schema_join(
    join_expr="final_step_values.origin_step_id = origin_step_cte.id",
    join_table="final_step_values",
    source_table="origin_step_cte"
) }}
    where origin_step_cte.is_deleted = false
),
approvals_cte as (
    select
    final_step_values.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('approvals_cte')}}
      )
    ) as approvals
    from {{ ref('step_approvals_staged') }} approvals_cte
    {{ schema_join(
    join_expr="final_step_values.id = approvals_cte.step_id",
    join_table="final_step_values",
    source_table="approvals_cte"
) }}
    where approvals_cte.is_deleted = false
    group by final_step_values.ion_uid
),
approval_requests_cte as (
    select
    final_step_values.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('approval_requests_cte')}}
      )
    ) as approval_requests
    from {{ ref('step_approval_requests_staged') }} approval_requests_cte
    {{ schema_join(
    join_expr="final_step_values.id = approval_requests_cte.step_id",
    join_table="final_step_values",
    source_table="approval_requests_cte"
) }}
    where approval_requests_cte.is_deleted = false
    group by final_step_values.ion_uid
),
datagrid_columns_cte as (
    select
    final_step_values.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('datagrid_columns_cte')}}
      )
    ) as datagrid_columns
    from {{ ref('datagrid_columns_staged') }} datagrid_columns_cte
    {{ schema_join(
    join_expr="final_step_values.calculated_join_id = datagrid_columns_cte.step_id",
    join_table="final_step_values",
    source_table="datagrid_columns_cte"
) }}
    where datagrid_columns_cte.is_deleted = false
    group by final_step_values.ion_uid
),
datagrid_rows_cte as (
    select
    final_step_values.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('datagrid_rows_cte')}}
      )
    ) as datagrid_rows
    from {{ ref('datagrid_rows_staged') }} datagrid_rows_cte
    {{ schema_join(
    join_expr="final_step_values.calculated_join_id = datagrid_rows_cte.step_id",
    join_table="final_step_values",
    source_table="datagrid_rows_cte"
) }}
    where datagrid_rows_cte.is_deleted = false
    group by final_step_values.ion_uid
),
parent_cte as (
    select
    final_step_values.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('parent_cte')}}
    ) as parent
    from {{ ref('steps_staged') }} parent_cte
    {{ schema_join(
    join_expr="final_step_values.parent_id = parent_cte.id",
    join_table="final_step_values",
    source_table="parent_cte"
) }}
    where parent_cte.is_deleted = false
),
labels_cte as (
    select
    final_step_values.ion_uid,
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
    join_expr="final_step_values.entity_id = entities_labels.entity_id",
    join_table="final_step_values",
    source_table="entities_labels"
    ) }}
    where labels.is_deleted = false
    group by final_step_values.ion_uid
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
    final_step_values.ion_uid
    from {{ ref('steps_attributes_staged') }} attrs
    {{ schema_join(
    join_expr="final_step_values.calculated_join_id = attrs.step_id",
    join_table="final_step_values",
    source_table="attrs"
    ) }}
    where attrs.is_deleted = false
    group by final_step_values.ion_uid
),
assets_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        file_attachments_cte.*
      )
    ) as assets
    from {{ ref('file_attachments_denormalized') }} file_attachments_cte
    {{ schema_join(
    join_expr="entities_assets.file_attachment_id = file_attachments_cte.id",
    join_table=ref('entities_assets_staged'),
    source_table="file_attachments_cte",
    join_alias="entities_assets",
    filter_deleted=true
    ) }}
    {{ schema_join(
    join_expr="source_cte.entity_id = entities_assets.entity_id",
    join_table="source_cte",
    source_table="entities_assets"
    ) }}
    where file_attachments_cte.is_deleted = false
    group by source_cte.ion_uid
),
source_cte_with_users as (
    {{ users_select('final_step_values') }}
)
select {{ select_star_exclude_ab_cols('source_cte_with_users', ['calculated_join_id']) }},
source_cte_with_users.parent_id is null and source_cte_with_users.procedure_id is null as is_standard_step,
source_cte_with_users.standard_step_family_id is not null as is_derived_step,
case
when source_cte_with_users.procedure_id is null
then source_cte_with_users.standard_step_status = 'draft'
else procedure_cte.procedure.status = 'draft'
end as editable,
procedure_cte.procedure,
fields_cte.fields,
upstream_cte.upstream,
downstream_cte.downstream,
reviews_cte.reviews,
location_subtype_cte.location_subtype,
derived_steps_cte.derived_steps,
pdf_cte.pdf,
location_cte.location,
steps_cte.steps,
origin_step_cte.origin_step,
approvals_cte.approvals,
approval_requests_cte.approval_requests,
datagrid_columns_cte.datagrid_columns,
datagrid_rows_cte.datagrid_rows,
parent_cte.parent,
labels_cte.labels,
attributes_cte.attributes,
assets_cte.assets
FROM source_cte_with_users
left outer join procedure_cte on procedure_cte.ion_uid = source_cte_with_users.ion_uid
left outer join fields_cte on fields_cte.ion_uid = source_cte_with_users.ion_uid
left outer join upstream_cte on upstream_cte.ion_uid = source_cte_with_users.ion_uid
left outer join downstream_cte on downstream_cte.ion_uid = source_cte_with_users.ion_uid
left outer join reviews_cte on reviews_cte.ion_uid = source_cte_with_users.ion_uid
left outer join location_subtype_cte on location_subtype_cte.ion_uid = source_cte_with_users.ion_uid
left outer join derived_steps_cte on derived_steps_cte.ion_uid = source_cte_with_users.ion_uid
left outer join pdf_cte on pdf_cte.ion_uid = source_cte_with_users.ion_uid
left outer join location_cte on location_cte.ion_uid = source_cte_with_users.ion_uid
left outer join steps_cte on steps_cte.ion_uid = source_cte_with_users.ion_uid
left outer join origin_step_cte on origin_step_cte.ion_uid = source_cte_with_users.ion_uid
left outer join approvals_cte on approvals_cte.ion_uid = source_cte_with_users.ion_uid
left outer join approval_requests_cte on approval_requests_cte.ion_uid = source_cte_with_users.ion_uid
left outer join datagrid_columns_cte on datagrid_columns_cte.ion_uid = source_cte_with_users.ion_uid
left outer join datagrid_rows_cte on datagrid_rows_cte.ion_uid = source_cte_with_users.ion_uid
left outer join parent_cte on parent_cte.ion_uid = source_cte_with_users.ion_uid
left outer join labels_cte on labels_cte.ion_uid = source_cte_with_users.ion_uid
left outer join attributes_cte on attributes_cte.ion_uid = source_cte_with_users.ion_uid
left outer join assets_cte on source_cte_with_users.ion_uid = assets_cte.ion_uid
