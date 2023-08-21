with run_step_sessions as (
    select run_step_id, _org_name
    from {{ ref('session_events_staged') }}
    {{ common_incremental() }}
    group by run_step_id, _org_name
),
source_cte as (
    select
    {{ select_star_exclude_ab_cols('all_run_step_session_events') }}
    from {{ ref('session_events_staged') }} as all_run_step_session_events
    {{ schema_join(
    join_expr="run_step_sessions.run_step_id = all_run_step_session_events.run_step_id",
    join_table="run_step_sessions",
    source_table="all_run_step_session_events"
    ) }}
),
session_grouping as (
    select
    _created as check_in,
    event_type as check_in_type,
    lead(_created) over (partition by created_by_id, run_step_id, _org_name order by _created) as check_out,
    lead(event_type) over (partition by created_by_id, run_step_id, _org_name order by _created) as check_out_type,
    case
    when lead(_created) over (partition by created_by_id, run_step_id, _org_name order by _created) is null
    then unix_timestamp(now()) - unix_timestamp(_created)
    else unix_timestamp(lead(_created) over (partition by created_by_id, run_step_id, _org_name order by _created)) - unix_timestamp(_created)
    end as duration,
    (
        event_type = 'check_in' and
        (
            lead(event_type) over (partition by created_by_id, run_step_id, _org_name order by _created) is null or
            lead(event_type) over (partition by created_by_id, run_step_id, _org_name order by _created) = 'check_out'
        )
    ) as is_valid_grouping,
    source_cte.*
    from source_cte
    where source_cte.is_deleted = false
    order by _created
)
select *
from session_grouping
where is_valid_grouping = true;