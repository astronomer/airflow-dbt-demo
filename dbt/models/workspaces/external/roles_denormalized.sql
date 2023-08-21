with source_cte as (
    select
    {{ select_star_exclude_ab_cols(ref('roles_staged')) }}
    from {{ ref('roles_staged') }}
    {{ common_incremental() }}
),
users_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('users_cte')}}
      )
    ) as users
    from {{ ref('users_staged') }} users_cte
    {{ schema_join(
    join_expr="user_roles.user_id = users_cte.id",
    join_table=ref('user_roles_staged'),
    source_table="users_cte",
    join_alias="user_roles",
    filter_deleted=true
) }}
    {{ schema_join(
    join_expr="source_cte.id = user_roles.role_id",
    join_table="source_cte",
    source_table="user_roles"
) }}
    where users_cte.is_deleted = false
    group by source_cte.ion_uid
),
teams_cte as (
    select
    source_cte.ion_uid,
    collect_list(
       struct(
        {{ select_star_exclude_ab_cols('teams_cte')}}
      )
    ) as teams
    from {{ ref('teams_staged') }} teams_cte
    {{ schema_join(
    join_expr="team_roles.team_id = teams_cte.id",
    join_table=ref('team_roles_staged'),
    source_table="teams_cte",
    join_alias="team_roles",
    filter_deleted=true
) }}
    {{ schema_join(
    join_expr="source_cte.id = team_roles.role_id",
    join_table="source_cte",
    source_table="team_roles"
) }}
    where teams_cte.is_deleted = false
    group by source_cte.ion_uid
)
select source_cte.*,
users_cte.users,
teams_cte.teams
FROM source_cte
left outer join users_cte on users_cte.ion_uid = source_cte.ion_uid
left outer join teams_cte on teams_cte.ion_uid = source_cte.ion_uid