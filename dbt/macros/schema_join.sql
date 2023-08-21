{% macro schema_join(join_expr, join_table, source_table, join_type="join", join_alias=None, filter_deleted=false) %}
    {%- if join_alias is none -%}

        {% set join_alias = "" %}
        {% set join_table_name = join_table %}

    {%- else -%}

        {% set join_table_name = join_alias %}

    {% endif %}
    {{ join_type }} {{ join_table }} {{ join_alias }}
        ON {{ join_expr }} and
        {{ source_table }}._org_name = {{ join_table_name }}._org_name
    {% if filter_deleted is true -%}
        and {{ join_table_name }}.is_deleted = false
    {% endif %}
{% endmacro %}