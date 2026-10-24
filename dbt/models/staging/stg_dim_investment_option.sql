-- Silver: one clean row per investment option.
-- Keeps only the latest load of each option (removes duplicates) and sets data types.
select
    cast(investment_option_key as int)             as investment_option_key,
    investment_option_id,
    investment_option_name,
    investment_option_type,
    investment_option_category,
    cast(fund_key as int)                          as fund_key,
    risk_profile,
    cast(growth_assets_pct as decimal(9, 6))       as growth_assets_pct,
    cast(defensive_assets_pct as decimal(9, 6))    as defensive_assets_pct,
    cast(total_member_assets as bigint)            as total_member_assets,
    _ingested_at
from {{ source('bronze', 'bronze_dim_investment_option') }}
qualify row_number() over (partition by investment_option_key order by _ingested_at desc) = 1