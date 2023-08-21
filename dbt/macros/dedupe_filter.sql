{% macro dedupe_filter(rank_col_name="row_number") %}
    {{ rank_col_name }} = 1
{% endmacro %}