-- Gold: investment options compared in the project.
-- One row per option: the largest option per fund and risk profile.
-- Risk profile comes from the option's asset mix (growth %), not its name.
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
    total_member_assets
from {{ ref('stg_dim_investment_option') }}