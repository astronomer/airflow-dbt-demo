-- use: dbt run-operation model_info --args '{model_name: my_model}'
{% macro model_info(model_name) %}

    {% set models = graph.nodes.values() %}

    {% set model = (models | selectattr('name', 'equalto', model_name) | list).pop() %}
    
    {% do log(" ============= ", info=true) %}
    {% do log("sources: ", info=true) %}
    {% set sources = model['sources'] %}
    {% for source in sources %}
        {% do log(source[0] ~ '.' ~ source[1], info=true) %}
    {% endfor %}

    {% do log(" ------- ", info=true) %}
    {% do log("relationships: ", info=true) %}
    {% set relas = model['meta']['relationships'] %}
    {% for rela in relas %}
        {% do log(rela, info=true) %}
    {% endfor %}

    {% do log(" ------- ", info=true) %}
    {% do log("refs: ", info=true) %}
    {% set refs = model['refs'] %}
    {% for ref in refs %}
        {% do log(ref[0], info=true) %}
    {% endfor %}

    {% set fqname = 'model.hush_sound.' ~ model_name %}

    {% do log(" ------- ", info=true) %}
    {% set columns = model['columns']  %}
    {% do log("columns: ", info=true) %}
    {% for column in columns %}
        {% do log(column, info=true) %}
    {% endfor %}

{% endmacro %}
