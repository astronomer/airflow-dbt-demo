

{% macro polymorphic_value_to_string(polymorphic_table=none) %}

    {% set polymorphic_types =  {
            "'string'": "_string_value",
            "'number'": "_number_value",
            "'boolean'": "_boolean_value",
            "'datetime'": "_datetime_value",
            "'file_attachment'": "file_attachment_id",
            "'select'": "_select_value",
            "'multiselect'": "_multiselect_values"
        }
    %}
    CASE
        {% for type_, column in polymorphic_types.items() -%}
            {% if polymorphic_table is not none %}
                WHEN ({{type_}} = {{ polymorphic_table }}.type) THEN cast({{ polymorphic_table }}.{{column}} as string)
            {% else %}
                WHEN ({{type_}} = type) THEN cast({{column}} as string)
            {% endif %}
        {% endfor %}
        ELSE ''
    END
{% endmacro %}
