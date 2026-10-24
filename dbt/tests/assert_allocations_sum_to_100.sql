-- Business rule: each option's strategic targets add up to 100% on every date.
-- Currency exposure (overlay) is excluded, as it sits on top of the 100%.
-- The test lists the option-dates that break the rule; it passes when the list is empty.
select
    f.date_key,
    f.investment_option_key,
    sum(f.benchmark_allocation_pct) as total_allocation
from {{ ref('fact_investment_strategic_allocation') }} as f
inner join {{ ref('dim_asset_class') }} as a
    on f.asset_class_key = a.asset_class_key
where not coalesce(a.is_overlay, false)
group by f.date_key, f.investment_option_key
having abs(sum(f.benchmark_allocation_pct) - 1) > 0.01   -- tolerance for rounding. Examples: 99.5% or 100.4% pass (rounding); 98% or 102% are shown as failures.