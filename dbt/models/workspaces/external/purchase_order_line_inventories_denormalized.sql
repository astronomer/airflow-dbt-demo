with source_cte as (
    select
		{{ select_star_exclude_ab_cols(ref('purchase_order_line_inventories_staged')) }}
    from {{ ref('purchase_order_line_inventories_staged') }}
    {{ common_incremental() }}
    )
select source_cte.*
FROM source_cte