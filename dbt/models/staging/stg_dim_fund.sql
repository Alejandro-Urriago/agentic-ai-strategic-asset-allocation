-- Silver: one clean row per fund.
-- Keeps only the latest load of each fund (removes duplicates) and sets data types.
select
    cast(fund_key as int) as fund_key,
    rse_abn,
    fund_name,
    short_name,
    fund_type,
    _ingested_at
from {{ source('bronze', 'bronze_dim_fund') }}
qualify row_number() over (partition by fund_key order by _ingested_at desc) = 1