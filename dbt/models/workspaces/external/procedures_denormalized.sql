with source_cte as (
    select
		{{ select_star_exclude_ab_cols(ref('procedures_staged')) }}
    from {{ ref('procedures_staged') }}
    {{ common_incremental() }}
    ),
source_cte_with_users as (
	{{ users_select('source_cte') }}
)
select source_cte_with_users.*
FROM source_cte_with_users