with source_cte as (
    select
    {{ select_star_exclude_ab_cols(ref('organizations_staged')) }}
    from {{ ref('organizations_staged') }}
    {{ common_incremental() }}
)
select source_cte.*
FROM source_cte
