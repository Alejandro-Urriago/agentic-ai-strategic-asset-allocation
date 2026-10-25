-- Gold: investment options compared in the project.
-- The largest diversified option (holds shares and cash/bonds) per fund and risk profile. Risk profile comes from the
-- option's asset mix (growth %), not its name.
-- Current version of each option only (one row per option).
-- A view on the history table investment_option_snapshot (SCD Type 2), so no data is copied.
{{ config(materialized='view') }}

select
    investment_option_key,
    investment_option_id,
    investment_option_name,
    investment_option_type,
    investment_option_category,
    fund_key,
    risk_profile,
    growth_assets_pct,
    defensive_assets_pct,
    total_member_assets,
    dbt_valid_from as valid_from
from {{ ref('investment_option_snapshot') }}
where dbt_valid_to is null   -- current version only; full history is in investment_option_snapshot