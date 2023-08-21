
-- https://stackoverflow.com/questions/70866939/extracting-ref-and-source-tables-from-a-dbt-model
-- use: dbt run-operation get_dependencies --args '{model_name: my_model}'
{% macro get_dependencies(model_name) %}
    {% set models = graph.nodes.values() %}

    {% set model = (models | selectattr('name', 'equalto', model_name) | list).pop() %}

    {% do log("sources: ", info=true) %}
    {% set sources = model['sources'] %}
    {% for source in sources %}
        {% do log(source[0] ~ '.' ~ source[1], info=true) %}
    {% endfor %}


    {% do log("refs: ", info=true) %}
    {% set refs = model['refs'] %}
    {% for ref in refs %}
        {% do log(ref[0], info=true) %}

    {% endfor %}

{% endmacro %}