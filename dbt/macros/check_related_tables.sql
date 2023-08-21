-- use: dbt run-operation check_related_tables --args '{model_name: my_model}'

{% macro check_related_tables(model_name) %}
    {% if is_incremental() %}
    {% if execute %}
    
        {% set model = fetch_model_config(model_name) %}
        {% set relas = model['meta']['relationships'] %}

        {% for rela in relas %}
            {% if rela == 'id' %}
                OR ion_uid IN (
                {% for j in range(relas[rela]|length) %}
                    {% set val = relas[rela][j] %}

                    {%- if '|' in val -%}

                        {% for i in range(val.split('|')|length) %}

                            {% set i_plus = i+1 %}
                            {% set alias_1 = 'A'~j~i_plus %}
                            {% set alias_2 = 'A'~j~i %}
                            {% set i_val = val.split('|')[i] %}
                            
                            {% if i == 0 %}
                                SELECT CONCAT ({{alias_1}}.{{i_val.split('.')[1]}}, "_", {{alias_1}}._org_name)
                                FROM {{ ref( i_val.split('.')[0] ) }} {{alias_1}}

                            {% elif i == val.split('|')|length -1 %}
                                {% set l_val = i_val.split(':')[0] %}
                                {% set r_val = i_val.split(':')[1] %}

                                JOIN {{ ref( r_val.split('.')[0] ) }} {{ alias_1 }} ON
                                    {{ alias_2 }}.{{ l_val.split('.')[1] }} = {{ alias_1 }}.{{ r_val.split('.')[1] }}
                                WHERE {{ alias_1 }}._cdc_timestamp > (
                                    SELECT nvl(max(_cdc_timestamp),"1970-01-01 00:00:00 UTC") 
                                    FROM {{ this }})

                            {% else %}
                                {% set l_val = i_val.split(':')[0] %}
                                {% set r_val = i_val.split(':')[1] %}

                                JOIN {{ ref( r_val.split('.')[0] ) }} {{ alias_1 }} ON
                                    {{ alias_2 }}.{{ l_val.split('.')[1] }} = {{ alias_1 }}.{{ r_val.split('.')[1] }}

                            {% endif %}

                        {% endfor %}
                    {%- else -%}

                        SELECT 
                            CONCAT ({{val.split('.')[1]}}, "_", _org_name)
                        FROM 
                            {{ ref( val.split('.')[0] ) }} 
                        WHERE {{ ref( val.split('.')[0] ) }}._cdc_timestamp > (
                            SELECT nvl(max(_cdc_timestamp),"1970-01-01 00:00:00 UTC") 
                            FROM {{ this }})

                    {% endif %}
                    {%- if j != relas[rela]|length -1 %}
                        UNION
                    {%- endif -%}
                {%- endfor -%}
                )

            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}
    {%- endif -%}

{% endmacro %}

