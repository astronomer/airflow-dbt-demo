{% macro fetch_model_config(model_name) %}

    {% set fqname = 'model.hush_sound.' ~ model_name %}
    {% set model = graph['nodes'][fqname] %}

    {{ return(model) }}

{% endmacro %}