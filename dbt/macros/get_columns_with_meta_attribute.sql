-- https://gitlab.com/gitlab-data/analytics/-/blob/master/transform/snowflake-dbt/macros/sensitive/get_meta_columns.sql
{% macro get_meta_columns(model_name, meta_key=none, source_name=None, project='hush_sound', additional_keys=none) %}
    {% if source_name is not none %}
        {% set graph_context = "sources" %}
        {% set fqname = 'source.' ~ project ~ '.' ~ source_name ~ '.' ~ model_name %}
    {% else %}

        {% set graph_context = "nodes" %}
        {% set fqname = 'model.' ~ project ~ '.' ~ model_name %}

    {% endif %}

	{% if execute %}
        {% set meta_columns = [] %}

	    {% set columns = graph[graph_context][fqname]['columns']  %}

        {% for column in columns %}
            {% if meta_key is not none %}

                {% if graph[graph_context][fqname]['columns'][column]['meta'][meta_key] == true %}
                    {% if additional_keys is not none %}
                        {% set current_meta_column = [column] %}
                        {% for additional_key in additional_keys %}
                            {% do current_meta_column.append(graph[graph_context][fqname]['columns'][column]['meta'].get(additional_key)) %}
                        {% endfor %}
                        {% do meta_columns.append(current_meta_column) %}
                    {% else %}
                        {% do meta_columns.append(column) %}
                    {% endif %}
                {% endif %}
            {% else %}
                {% do meta_columns.append(column) %}
            {% endif %}
        {% endfor %}

        {{ return(meta_columns) }}

	{% endif %}

{% endmacro %}

