{% macro get_obfuscated_select_of_table(source_table, source_name=none) %}
    SELECT
    {%- if source_name is not none -%}

        {% set table_ref = source(source_name, source_table)  %}
        {% set node_type = "sources"  %}

    {%- else -%}

        {% set table_ref = ref(source_table)  %}
        {% set node_type = "nodes"  %}

    {% endif %}
    {% set meta_columns = get_meta_columns(source_table, "sensitive", source_name) %}

    {%- for column in meta_columns %}

        {{ hash_column(column) }},

    {% endfor %}

    {{ dbt_utils.star(from=table_ref, except=meta_columns) }}
    FROM {{ table_ref }}
{% endmacro %}
