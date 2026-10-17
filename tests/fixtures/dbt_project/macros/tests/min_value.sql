{% test min_value(model, column_name, minimum) %}
select {{ column_name }} from {{ model }} where {{ column_name }} < {{ minimum }}
{% endtest %}
