{% macro common_incremental() %}
    {%- if is_incremental() -%}
        {% set incremental_type = var('incremental_type', none) %}
        {%- if incremental_type == 'transaction' -%}
            where _cdc_transaction_id in ({TRANSACTION_IDS}) and _org_name = '{ORG_NAME}'
        {%- else -%}
            where _cdc_timestamp > (select nvl(max(_cdc_timestamp),"1970-01-02 00:00:00 UTC") from {{ this }})
        {%- endif -%}
    {%- endif -%}
{% endmacro %}