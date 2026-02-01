{% macro generate_schema_name(custom_schema_name, node) -%}
  {#-
    ClickHouse uses "schema" as the database name.
    We want folder/model-level +schema (e.g. dw/dm) WITHOUT prefixing with target.schema.

    Rules:
    - If +schema is set, use it as-is (dw/dm), no prefixes.
    - Otherwise, fall back to target.schema (should be a real default, e.g. "default").
  -#}

  {%- set default_schema = target.schema | trim -%}
  {%- set custom_schema = (custom_schema_name or '') | trim -%}

  {%- if custom_schema != '' -%}
    {{ custom_schema }}
  {%- else -%}
    {{ default_schema }}
  {%- endif -%}
{%- endmacro %}
