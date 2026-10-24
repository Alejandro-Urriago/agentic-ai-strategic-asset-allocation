-- Silver: strategic asset allocation targets (not actual holdings).
-- One row per date, investment option and asset class.
-- Keeps only the latest load of each row (removes duplicates) and sets data types.
select
    cast(date_key as date)                               as date_key,
    cast(fund_key as int)                                as fund_key,
    cast(investment_option_key as int)                   as investment_option_key,
    cast(asset_class_key as int)                         as asset_class_key,
    cast(benchmark_allocation_pct as decimal(9, 6))      as benchmark_allocation_pct,
    cast(currency_hedging_ratio_pct as decimal(9, 6))    as currency_hedging_ratio_pct,
    _ingested_at
from {{ source('bronze', 'bronze_fact_investment_strategic_allocation') }}
qualify row_number() over (
    partition by date_key, investment_option_key, asset_class_key
    order by _ingested_at desc
) = 1