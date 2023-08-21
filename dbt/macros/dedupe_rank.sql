{% macro dedupe_rank(pk_columns=none, rank_col_name="row_number") %}
    {%- if pk_columns is none -%}

        {% set pk_columns = ["id"] %}

    {% endif %}
    {% set incremental_type = var('incremental_type', none) %}
    {%- if incremental_type == 'transaction' -%}
        ROW_NUMBER() OVER(
            PARTITION BY {{ pk_columns|join(", ") }}
            ORDER BY _cdc_timestamp desc, _cdc_lsn desc
        ) as {{ rank_col_name }}
    {%- else -%}
        ROW_NUMBER() OVER(
            PARTITION BY {{ pk_columns|join(", ") }}, _org_name
            ORDER BY _cdc_timestamp desc, _cdc_lsn desc, _cdc_processed_timestamp desc
        ) as {{ rank_col_name }}
    {%- endif -%}
{% endmacro %}