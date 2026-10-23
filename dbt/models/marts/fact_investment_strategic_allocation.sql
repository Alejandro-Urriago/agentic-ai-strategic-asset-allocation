-- Gold: strategic asset allocation targets (board-approved targets, not actual holdings).
-- One row per date, investment option and asset class. Keeps all options;
-- reports compare only the options in dim_investment_option (inner join).
-- Incremental: each run adds only new or changed rows (merge), instead of rebuilding the table.
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['date_key', 'investment_option_key', 'asset_class_key']
) }}

select
    date_key,
    fund_key,
    investment_option_key,
    asset_class_key,
    benchmark_allocation_pct,
    currency_hedging_ratio_pct,
    _ingested_at
from {{ ref('stg_fact_investment_strategic_allocation') }}
{% if is_incremental() %}
where _ingested_at > (select max(_ingested_at) from {{ this }})
{% endif %}