{% macro obscure_selective_customers(source_table, source_cte) %}
    {% set sensitive_env = env_var('AWS_REGION', 'us-gov-west-1') == 'us-gov-west-1' %}
    -- If not deploying into gov cloud env, then use standard select * for simplicity and speed
    {%- if sensitive_env not in [true, "True", "true", "t"]  %}
        *
    {% else -%}
        {% if execute %}
            -- Get list of columns for table which should be obfuscated and that columns data type as a tuple
            --  EX: ('title', 'string')
            {% set meta_columns = get_meta_columns(source_table, "sensitive", additional_keys=["data_type", 'exempting']) %}
            {% set exclude_from_star_select = [] %}
            -- Loop through each sensitive column and obfuscate the data unless the org has opted in
            {%- for column in meta_columns %}
                -- Get list of customers who have opted in to have their data flow un-obscured through analytics
                {% set not_to_hash = column[2] %}
{#                {% for item in not_to_hash_env_var %}#}
{#                    {% do not_to_hash.append("'" + item.strip() + "'") %}#}
{#                {% endfor %}#}
                {% if (not_to_hash is none) or (not_to_hash|length == 0) %}
                    {% if column[1] == "array<string>" %}
                        transform({{ source_cte }}.{{column[0]}}, x -> sha2(x, 256)) as {{column[0]}},
                    {% else %}
                        sha2({{ source_cte }}.{{column[0]}}, 256) as {{column[0]}},
                    {% endif %}
                {% else %}
                    {% set not_to_hash_sql_strs = [] %}
                    {% for item in not_to_hash %}
                        {% do not_to_hash_sql_strs.append("'" + item.strip() + "'") %}
                    {% endfor %}
                    -- If data type is an array of strings hash each entry in the array
                    {% if column[1] == "array<string>" %}
                        if(
                            ({{ source_cte }}._org_name not in ({{ not_to_hash_sql_strs|join(", ") }}) and {{ source_cte }}.{{column[0]}} is not null),
                            transform({{ source_cte }}.{{column[0]}}, x -> sha2(x, 256)),
                            {{ source_cte }}.{{column[0]}}
                        ) as {{column[0]}},
                    -- Otherwise just hash the entire column
                    {% else %}
                        if(
                            ({{ source_cte }}._org_name not in ({{ not_to_hash_sql_strs|join(", ") }}) and {{ source_cte }}.{{column[0]}} is not null),
                            sha2({{ source_cte }}.{{column[0]}}, 256),
                            {{ source_cte }}.{{column[0]}}
                        ) as {{column[0]}},
                    {% endif %}
                {% endif %}
                {% do exclude_from_star_select.append(column[0]) %}
            {% endfor %}
            -- select everything excluding the columns that were explicitly selected above in obfuscation logic
            {{ select_star_exclude_ab_cols(source_cte, exclude_from_star_select) }}
        {% endif %}
    {% endif %}
{% endmacro %}
