{% macro array_agg(expr) %}

    collect_list({{ expr }})

{% endmacro %}