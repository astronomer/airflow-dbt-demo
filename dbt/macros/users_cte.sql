{% macro users_select(origin_cte) %}
    select
      creator.email as created_by_email,
      creator.name as created_by_name,
      updator.email as updated_by_email,
      updator.name as updated_by_name,
      {{origin_cte}}.*
    from {{origin_cte}}
    left outer join {{ ref('users_staged') }} as creator on
        {{origin_cte}}.created_by_id = creator.id and
      {{origin_cte}}._org_name = creator._org_name
    left outer join {{ ref('users_staged') }} as updator on
      {{origin_cte}}.updated_by_id = updator.id and
      {{origin_cte}}._org_name = updator._org_name
{% endmacro %}
