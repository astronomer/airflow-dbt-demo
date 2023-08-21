{% macro select_star_exclude_ab_cols(table_ref, additional_cols, include_cols) %}
    {% set default_exlcude_cols = [
            'row_number'
        ] + additional_cols|default([])
    %}
    {% set exlcude_cols = [] %}
    {% for column in default_exlcude_cols %}
        {% if column not in include_cols %}
            {% do exlcude_cols.append(column) %}
        {% endif %}
    {% endfor %}
    {% if table_ref is string %}
        {{table_ref}}.`({{ exlcude_cols|join('|') }})?+.+`
    {% else %}
       {{- dbt_utils.star(
            table_ref,
            except=exlcude_cols
       ) -}}
    {% endif %}
{% endmacro %}