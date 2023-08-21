with source_cte as (
    select
	cast(receipt_id as int) as receipt_id,
	cast(purchase_order_line_id as int) as purchase_order_line_id,
	cast(part_inventory_id as int) as part_inventory_id,
    {{ dedupe_rank(pk_columns=['receipt_id', 'part_inventory_id']) }},
    {{ dbt_utils.star(
        source("raw_data", "receipt_part_inventories"),
        except=['receipt_id', 'purchase_order_line_id', 'part_inventory_id']
    ) }}
    from {{ source("raw_data", "receipt_part_inventories") }}
    {{ common_incremental() }}
)
select
    {{ generate_ion_id(pk_columns=['receipt_id', 'part_inventory_id']) }},
    {{ obscure_selective_customers('receipt_part_inventories_staged', 'source_cte') }}
from source_cte
where {{ dedupe_filter() }}
