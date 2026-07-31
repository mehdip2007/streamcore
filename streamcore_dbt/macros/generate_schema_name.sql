{#
    dbt's built-in generate_schema_name macro CONCATENATES the profile's
    base schema with any custom +schema config (e.g. "streamcore_staging"
    would actually become "<base_schema>_streamcore_staging"). Every
    model in this project sets an explicit +schema (see dbt_project.yml)
    and we want that name used exactly as written — streamcore_staging,
    streamcore_intermediate, streamcore_marts — since that's what
    sources.yml, CLAUDE.md, and the mart/staging docs all assume.

    This is dbt's own documented override for exactly this situation:
    https://docs.getdbt.com/docs/build/custom-schemas
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
