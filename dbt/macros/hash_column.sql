{% macro hash_column(column) %}

    sha2(cast({{column}} as string), 256) AS {{column}}

{% endmacro %}
