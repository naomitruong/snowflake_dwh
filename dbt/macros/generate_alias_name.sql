{#-
    Table name = model name without the "vn_ministop_" prefix and "_table"/"_view"
    An explicit alias in a model''s config still wins.
-#}
{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
    {%- if custom_alias_name -%}
        {{ custom_alias_name | trim }}
    {%- elif node.name.startswith('vn_ministop_') -%}
        {{ modules.re.sub('_(table|view)$', '', node.name[12:]) }}
    {%- else -%}
        {{ node.name }}
    {%- endif -%}
{%- endmacro %}