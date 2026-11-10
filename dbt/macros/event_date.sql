{#-
    The processing date as a quoted 'YYYYMMDD' string, used where Fez's compiled SQL had '20260924'.
    Set it with --vars '{event_date: "20260924"}' (the scheduler passes the run date).
    Default: today in UTC (run_started_at). The scheduler should always pass event_date explicitly.
-#}
{% macro event_date() -%}
    {%- set value = var('event_date', none) -%}
    {%- if value is none -%}
        {%- set value = run_started_at.strftime('%Y%m%d') -%}
    {%- endif -%}
    {%- set value = value | string -%}
    {%- if not modules.re.fullmatch('[0-9]{8}', value) -%}
        {{ exceptions.raise_compiler_error("event_date must be YYYYMMDD, got: " ~ value) }}
    {%- endif -%}
    '{{ value }}'
{%- endmacro %}
