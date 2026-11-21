-- Gold: strategic asset allocation targets (board-approved targets, not actual holdings).
-- Latest value only: one row per date, investment option and asset class.
-- Earlier values (APRA corrections) are kept in strategic_allocation_snapshot.
-- Keeps all options; reports compare only the options in dim_investment_option (inner join).
-- Built as a table from the history table strategic_allocation_snapshot (SCD Type 2), rebuilt on every run,
-- so it allows primary and foreign keys (needed for the star schema diagram in Databricks).

select
    date_key,
    fund_key,
    investment_option_key,
    asset_class_key,
    benchmark_allocation_pct,
    currency_hedging_ratio_pct,
    _ingested_at,
    dbt_valid_from as valid_from
from {{ ref('strategic_allocation_snapshot') }}
where dbt_valid_to is null   -- current version only; full history is in strategic_allocation_snapshot