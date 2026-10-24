-- Silver: one clean row per asset class.
-- Keeps only the latest load of each asset class (removes duplicates) and sets data types.
select
    cast(asset_class_key as int)       as asset_class_key,
    asset_class_name,
    sector_l1,
    sub_sector_l2,
    listing_type,
    domicile_type,
    economy_type,
    hedging_status,
    measure_type,
    parent_asset_class,
    cast(is_overlay as boolean)        as is_overlay,
    source_system,
    source_column,
    saa_sector_type,
    _ingested_at
from {{ source('bronze', 'bronze_dim_asset_class') }}
qualify row_number() over (partition by asset_class_key order by _ingested_at desc) = 1