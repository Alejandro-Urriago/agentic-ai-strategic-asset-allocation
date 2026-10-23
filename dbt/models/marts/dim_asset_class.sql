-- Gold: asset classes (cash, shares, property...). One row per asset class.
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
    saa_sector_type
from {{ ref('stg_dim_asset_class') }}