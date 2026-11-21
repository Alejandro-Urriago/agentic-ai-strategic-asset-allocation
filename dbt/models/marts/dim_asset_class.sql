-- Gold: asset classes (cash, shares, property...).
-- Current version of each asset class only (one row per asset class).
-- Built as a table from the history table asset_class_snapshot (SCD Type 2), rebuilt on every run,
-- so it allows primary and foreign keys (needed for the star schema diagram in Databricks).

select
    asset_class_key,
    asset_class_name,
    sector_l1,
    sub_sector_l2,
    listing_type,
    domicile_type,
    economy_type,
    hedging_status,
    measure_type,
    parent_asset_class,
    is_overlay,
    source_system,
    source_column,
    saa_sector_type,
    dbt_valid_from as valid_from
from {{ ref('asset_class_snapshot') }}
where dbt_valid_to is null   -- current version only; full history is in asset_class_snapshot