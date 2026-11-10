{#-
    Write to the dataset named in +schema, as is (vn_ministop_all, vn_ministop).
    -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set dataset = custom_schema_name | trim if custom_schema_name else target.schema -%}
    {{ dataset ~ var('dataset_suffix', '') }}
{%- endmacro %}