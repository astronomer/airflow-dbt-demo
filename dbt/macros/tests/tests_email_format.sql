
{% test tests_email_format(model, column_name) %}

select
    {{ column_name }} 
from {{ model }}
where not ({{ column_name }} LIKE '%@%')


{% endtest %}
