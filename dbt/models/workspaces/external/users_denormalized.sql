with source_cte as (
    select
    {{ select_star_exclude_ab_cols(ref('users_staged')) }}
    from {{ ref('users_staged') }}
    {{ common_incremental() }}
),
organization_cte as (
    select
    source_cte.ion_uid,
    struct(
        {{ select_star_exclude_ab_cols('organization_cte')}}
    ) as organization
    from {{ ref('organizations_staged') }} organization_cte
    {{ schema_join(
    join_expr="source_cte.organization_id = organization_cte.id",
    join_table="source_cte",
    source_table="organization_cte"
) }}
    where organization_cte.is_deleted = false
),
teams_cte as (
    select
    source_cte.ion_uid,
    collect_list(
        struct(
            {{ select_star_exclude_ab_cols('teams_cte', ['users']) }}
        )
    ) as teams
    from {{ ref('teams_denormalized') }} teams_cte
    {{ schema_join(
    join_expr="team_users.team_id = teams_cte.id",
    join_table=ref('team_users_staged'),
    source_table="teams_cte",
    join_alias="team_users",
    filter_deleted=true
) }}
    {{ schema_join(
    join_expr="source_cte.id = team_users.user_id",
    join_table="source_cte",
    source_table="team_users"
) }}
    where teams_cte.is_deleted = false
    group by source_cte.ion_uid
),
analytics_credential_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('analytics_credential_cte')}}
      )
    ) as analytics_credential
    from {{ ref('analytics_credentials_staged') }} analytics_credential_cte
    {{ schema_join(
    join_expr="source_cte.id = analytics_credential_cte.user_id",
    join_table="source_cte",
    source_table="analytics_credential_cte"
) }}
    where analytics_credential_cte.is_deleted = false
    group by source_cte.ion_uid
),
api_keys_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('api_keys_cte')}}
      )
    ) as api_keys
    from {{ ref('api_keys_staged') }} api_keys_cte
    {{ schema_join(
    join_expr="source_cte.id = api_keys_cte.user_id",
    join_table="source_cte",
    source_table="api_keys_cte"
) }}
    where api_keys_cte.is_deleted = false
    group by source_cte.ion_uid
),
user_roles_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('roles_cte')}}
      )
    ) as user_roles
    from {{ ref('roles_staged') }} roles_cte
    {{ schema_join(
    join_expr="user_roles.role_id = roles_cte.id",
    join_table=ref('user_roles_staged'),
    source_table="roles_cte",
    join_alias="user_roles",
    filter_deleted=true
) }}
    {{ schema_join(
    join_expr="source_cte.id = user_roles.user_id",
    join_table="source_cte",
    source_table="user_roles"
) }}
    where roles_cte.is_deleted = false
    group by source_cte.ion_uid
)
select source_cte.*,
organization_cte.organization,
teams_cte.teams,
analytics_credential_cte.analytics_credential,
api_keys_cte.api_keys,
array_union(
    user_roles_cte.user_roles,
    flatten(teams_cte.teams.roles)
           ) as roles
FROM source_cte
left outer join organization_cte on organization_cte.ion_uid = source_cte.ion_uid
left outer join teams_cte on teams_cte.ion_uid = source_cte.ion_uid
left outer join analytics_credential_cte on analytics_credential_cte.ion_uid = source_cte.ion_uid
left outer join api_keys_cte on api_keys_cte.ion_uid = source_cte.ion_uid
left outer join user_roles_cte on user_roles_cte.ion_uid = source_cte.ion_uid