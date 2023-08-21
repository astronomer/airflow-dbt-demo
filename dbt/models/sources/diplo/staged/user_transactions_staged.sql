with source_cte as (
    select
    _cdc_timestamp,
	_cdc_id,
	_org_name,
	_cdc_lsn,
	_cdc_processed_timestamp,
	_cdc_transaction_id,
    {{ dedupe_rank(pk_columns=['transaction_id', 'user_id']) }},
    {{ dbt_utils.star(
        source("raw_data", "user_transactions"),
        except=['_cdc_timestamp', '_cdc_id', '_org_name', '_cdc_lsn', '_cdc_processed_timestamp', '_cdc_transaction_id']
    ) }}
    from {{ source("raw_data", "user_transactions") }}
    {{ common_incremental() }}
)
select
    {{ generate_ion_id(pk_columns=['transaction_id', 'user_id']) }},
    *
from source_cte
where {{ dedupe_filter() }}
