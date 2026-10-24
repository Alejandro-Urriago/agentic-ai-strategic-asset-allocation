-- Gold: superannuation funds compared in the project.
-- Current version of each fund only (one row per fund).
-- A view on the history table fund_snapshot (SCD Type 2), so no data is copied.
{{ config(materialized='view') }}

select
    fund_key,
    rse_abn,
    fund_name,
    short_name,
    fund_type,
    dbt_valid_from as valid_from
from {{ ref('fund_snapshot') }}
where dbt_valid_to is null   -- current version only; full history is in fund_snapshot