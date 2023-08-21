{% test tests_empty_table(model) %}

    with cte as (
        select
            case
                when count(*) < 1 then 'True'
                else 'False'
            end as table_empty
        from {{ model }}
    )
    select *
    FROM cte
    WHERE table_empty = 'True'

{% endtest %}
