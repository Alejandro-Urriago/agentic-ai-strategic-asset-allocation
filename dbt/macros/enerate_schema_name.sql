-- Decides which schema (area) each table is written to.
-- Production (target "prod", used by the Databricks job): the schema set in
-- dbt_project.yml (silver, gold, snapshots).
-- Anywhere else (your laptop, target "dev"): your personal test schema.
-- Uses dbt's built-in rule generate_schema_name_for_env.
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ generate_schema_name_for_env(custom_schema_name, node) }}
{%- endmacro %}