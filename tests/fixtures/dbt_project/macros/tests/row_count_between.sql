{% test row_count_between(model, low, high) %}
select n from (select count(*) as n from {{ model }}) c where n not between {{ low }} and {{ high }}
{% endtest %}
