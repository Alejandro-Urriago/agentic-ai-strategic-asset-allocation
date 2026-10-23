-- Gold: superannuation funds compared in the project. One row per fund.
select
    fund_key,
    rse_abn,
    fund_name,
    short_name,
    fund_type
from {{ ref('stg_dim_fund') }}