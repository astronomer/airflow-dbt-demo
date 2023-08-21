{% macro generate_ion_id(pk_columns=none) %}
    {%- if pk_columns is none -%}

        {% set pk_columns = ["id"] %}

    {% endif %}

    CONCAT({{ pk_columns|join(", '_' , ") }}, '_' , _org_name) as ion_uid,
    -- TEMP addition for snowflake because _org_name is partitioned upon it isnt in the parquet files
    _org_name as _organization_name

{% endmacro %}
