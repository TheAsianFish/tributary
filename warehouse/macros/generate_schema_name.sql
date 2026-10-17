{#
  Use the configured schema verbatim (staging, marts) instead of dbt's default
  "<target schema>_<custom schema>", so the warehouse layout is the same in every
  environment and the agent's schema allowlist can name it.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
