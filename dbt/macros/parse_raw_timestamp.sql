{% macro parse_raw_timestamp(column) %}

    cast({{column}} as datetime) AS {{column}}

{% endmacro %}
