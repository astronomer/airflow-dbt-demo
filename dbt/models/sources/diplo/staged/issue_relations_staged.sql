with source_cte as (
    select
    _cdc_timestamp,
	_cdc_id,
	_org_name,
	_cdc_lsn,
	_cdc_processed_timestamp,
	_cdc_transaction_id,
    {{ dedupe_rank(pk_columns=['related_issue_id', 'origin_issue_id']) }},
    {{ dbt_utils.star(
        source("raw_data", "issue_relations"),
        except=['_cdc_timestamp', '_cdc_id', '_org_name', '_cdc_lsn', '_cdc_processed_timestamp', '_cdc_transaction_id']
    ) }}
    from {{ source("raw_data", "issue_relations") }}
    {{ common_incremental() }}
)
select
    {{ generate_ion_id(pk_columns=['related_issue_id', 'origin_issue_id']) }},
    *
from source_cte
where {{ dedupe_filter() }}
