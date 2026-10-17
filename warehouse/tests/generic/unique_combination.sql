{#
  Fails for every combination of combination_of_columns that occurs more than
  once. NULLs compare equal (GROUP BY semantics), so a NULL in a key column cannot
  hide a duplicate.
#}
{% test unique_combination(model, combination_of_columns) %}

select
    {{ combination_of_columns | join(', ') }},
    count(*) as occurrences
from {{ model }}
group by {{ combination_of_columns | join(', ') }}
having count(*) > 1

{% endtest %}
