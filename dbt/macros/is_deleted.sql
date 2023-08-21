{% macro is_deleted() %}

    CASE
        WHEN (_ab_cdc_deleted_at is null) THEN false
        ELSE true
    END as is_deleted,
    _ab_cdc_deleted_at as _deleted_at

{% endmacro %}
