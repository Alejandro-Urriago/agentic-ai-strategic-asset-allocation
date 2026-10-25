{#
  Gold: metric view (semantic layer). Business definitions written once, used by Genie, Claude and dashboards.
  Built-in rules:
  - Only the 40 compared options (inner join to dim_investment_option); currency overlay excluded.
  - Every % measure = average target per option and date, so options are never added together by mistake:
    one fund + one date gives that option's exact target; a risk profile + a year gives the simple average across funds.
  - Percentages are 0-100 (already multiplied by 100).
  - 1-year and 5-year changes compare each date with the same quarter 12 and 60 months earlier.
  Jinja comments are used here because the model body must be pure YAML.
#}
{{ config(materialized='metric_view') }}

version: 1.1
comment: >
  Strategic asset allocation (SAA) targets of the 40 compared investment options (largest option per fund and risk profile).
  Board-approved target allocations reported to APRA, not actual holdings. Percentages are 0-100.
  Compare funds within one risk profile. For one point per financial year, filter is_year_end_point = true.
source: |
  SELECT
    s.date_key,
    s.investment_option_key,
    s.benchmark_allocation_pct,
    s.currency_hedging_ratio_pct,
    f.short_name AS fund,
    f.fund_type,
    o.risk_profile,
    o.investment_option_name,
    d.financial_year,
    s.date_key = MAX(s.date_key) OVER (PARTITION BY d.financial_year) AS is_year_end_point,
    a.sector_l1,
    a.sub_sector_l2,
    a.listing_type,
    a.domicile_type,
    a.economy_type
  FROM {{ ref('fact_investment_strategic_allocation') }} AS s
  JOIN {{ ref('dim_investment_option') }} AS o ON s.investment_option_key = o.investment_option_key
  JOIN {{ ref('dim_fund') }} AS f ON o.fund_key = f.fund_key
  JOIN {{ ref('dim_date') }} AS d ON s.date_key = d.date_key
  JOIN {{ ref('dim_asset_class') }} AS a ON s.asset_class_key = a.asset_class_key
  WHERE NOT COALESCE(a.is_overlay, false)

fields:
  - name: fund
    expr: fund
    display_name: Fund
    comment: Short name of the superannuation fund.
    synonyms: [super fund, pension fund, fund name]
  - name: fund_type
    expr: fund_type
    display_name: Fund type
    comment: "Industry, Retail or Public_Sector."
  - name: risk_profile
    expr: risk_profile
    display_name: Risk profile
    comment: Risk group by share of growth assets (Conservative, Balanced, Growth, High growth). Compare funds within one risk profile, never across.
    synonyms: [risk level, risk band, risk category]
  - name: investment_option_name
    expr: investment_option_name
    display_name: Investment option
    comment: Name the fund gives the option. Not a reliable guide to risk; use risk_profile to compare.
  - name: date
    expr: date_key
    display_name: Date
    comment: Quarter-end date of the target.
    synonyms: [quarter, as at date]
  - name: financial_year
    expr: financial_year
    display_name: Financial year
    comment: Australian financial year, named by the year it ends (July 2025 to June 2026 = 2026).
    synonyms: [FY, year]
  - name: is_year_end_point
    expr: is_year_end_point
    display_name: Year-end point
    comment: True on the last quarter-end available in each financial year (the current year may end before June). Filter on it for one point per year.
    synonyms: [year end, end of year, annual point]
  - name: sector
    expr: sector_l1
    display_name: Sector
    comment: Main asset sector used for comparisons, e.g. cash, fixed_income, equity, property, infrastructure, alternatives.
    synonyms: [asset class, asset sector]
  - name: sub_sector
    expr: sub_sector_l2
    display_name: Sub-sector
    comment: More detailed asset class within the sector.
  - name: listing_type
    expr: listing_type
    display_name: Listing
    comment: listed, unlisted (private) or unspecified (fund did not say).
  - name: domicile_type
    expr: domicile_type
    display_name: Domicile
    comment: australian, international or unspecified (no split reported). Keep unspecified as its own group.
    synonyms: [region, geography, domestic or international]
  - name: economy_type
    expr: economy_type
    display_name: Market type
    comment: developed, emerging or unspecified (no split reported). Never treat unspecified as developed.
    synonyms: [developed or emerging]

measures:
  - name: allocation_pct
    expr: SUM(benchmark_allocation_pct) * 100 / COUNT(DISTINCT investment_option_key, date_key)
    display_name: Target allocation %
    comment: Target allocation in %, average per option. Group by sector (or sub_sector, domicile...) to see the mix; all sectors add up to 100 per option.
    synonyms: [weight, target weight, SAA, allocation]
    format:
      type: number
      decimal_places:
        type: max
        places: 1
  - name: equity_pct
    expr: SUM(CASE WHEN sector_l1 = 'equity' THEN benchmark_allocation_pct END) * 100 / COUNT(DISTINCT investment_option_key, date_key)
    display_name: Equity %
    comment: Equity target in %, average per option (listed and unlisted).
    synonyms: [shares, equities, stocks]
    format:
      type: number
      decimal_places:
        type: max
        places: 1
  - name: listed_equity_pct
    expr: SUM(CASE WHEN sector_l1 = 'equity' AND listing_type <> 'unlisted' THEN benchmark_allocation_pct END) * 100 / COUNT(DISTINCT investment_option_key, date_key)
    display_name: Listed equity %
    comment: Listed equity target in %, average per option. Includes listing unspecified; excludes only unlisted (private) equity.
    synonyms: [listed shares, public equity, liquid equity]
    format:
      type: number
      decimal_places:
        type: max
        places: 1
  - name: australian_listed_equity_pct
    expr: SUM(CASE WHEN sector_l1 = 'equity' AND listing_type <> 'unlisted' AND domicile_type = 'australian' THEN benchmark_allocation_pct END) * 100 / COUNT(DISTINCT investment_option_key, date_key)
    display_name: Australian listed equity %
    comment: Australian listed equity target in %, average per option.
    synonyms: [domestic equity, australian shares, home bias]
    format:
      type: number
      decimal_places:
        type: max
        places: 1
  - name: international_listed_equity_pct
    expr: SUM(CASE WHEN sector_l1 = 'equity' AND listing_type <> 'unlisted' AND domicile_type = 'international' THEN benchmark_allocation_pct END) * 100 / COUNT(DISTINCT investment_option_key, date_key)
    display_name: International listed equity %
    comment: International listed equity target in %, average per option.
    synonyms: [global equity, overseas shares, international shares]
    format:
      type: number
      decimal_places:
        type: max
        places: 1
  - name: unspecified_domicile_listed_equity_pct
    expr: SUM(CASE WHEN sector_l1 = 'equity' AND listing_type <> 'unlisted' AND domicile_type = 'unspecified' THEN benchmark_allocation_pct END) * 100 / COUNT(DISTINCT investment_option_key, date_key)
    display_name: Listed equity, region not reported %
    comment: Listed equity with no Australian/international split reported, in %. Show it separately; never merge it.
    format:
      type: number
      decimal_places:
        type: max
        places: 1
  - name: international_share_of_listed_equity_pct
    expr: MEASURE(international_listed_equity_pct) * 100 / NULLIF(MEASURE(listed_equity_pct), 0)
    display_name: International share of listed equity %
    comment: International listed equity as a share of all listed equity, in %. Unreliable where the region is not reported.
    synonyms: [international share, home bias ratio]
    format:
      type: number
      decimal_places:
        type: max
        places: 1
  - name: developed_listed_equity_pct
    expr: SUM(CASE WHEN sector_l1 = 'equity' AND listing_type <> 'unlisted' AND domicile_type = 'international' AND economy_type = 'developed' THEN benchmark_allocation_pct END) * 100 / COUNT(DISTINCT investment_option_key, date_key)
    display_name: Developed markets listed equity %
    comment: International developed markets listed equity target in %, average per option.
    synonyms: [developed markets, DM equity]
    format:
      type: number
      decimal_places:
        type: max
        places: 1
  - name: emerging_listed_equity_pct
    expr: SUM(CASE WHEN sector_l1 = 'equity' AND listing_type <> 'unlisted' AND domicile_type = 'international' AND economy_type = 'emerging' THEN benchmark_allocation_pct END) * 100 / COUNT(DISTINCT investment_option_key, date_key)
    display_name: Emerging markets listed equity %
    comment: International emerging markets listed equity target in %, average per option.
    synonyms: [emerging markets, EM equity]
    format:
      type: number
      decimal_places:
        type: max
        places: 1
  - name: unspecified_economy_listed_equity_pct
    expr: SUM(CASE WHEN sector_l1 = 'equity' AND listing_type <> 'unlisted' AND domicile_type = 'international' AND economy_type = 'unspecified' THEN benchmark_allocation_pct END) * 100 / COUNT(DISTINCT investment_option_key, date_key)
    display_name: International listed equity, market type not reported %
    comment: International listed equity with no developed/emerging split reported, in %. Show it separately; never treat it as developed.
    format:
      type: number
      decimal_places:
        type: max
        places: 1
  - name: cash_pct
    expr: SUM(CASE WHEN sector_l1 = 'cash' THEN benchmark_allocation_pct END) * 100 / COUNT(DISTINCT investment_option_key, date_key)
    display_name: Cash %
    comment: Cash target in %, average per option.
    format:
      type: number
      decimal_places:
        type: max
        places: 1
  - name: fixed_income_pct
    expr: SUM(CASE WHEN sector_l1 = 'fixed_income' THEN benchmark_allocation_pct END) * 100 / COUNT(DISTINCT investment_option_key, date_key)
    display_name: Fixed income %
    comment: Fixed income (bonds and credit) target in %, average per option.
    synonyms: [bonds, fixed interest]
    format:
      type: number
      decimal_places:
        type: max
        places: 1

  - name: listed_equity_pct_1y_ago
    expr: SUM(CASE WHEN sector_l1 = 'equity' AND listing_type <> 'unlisted' THEN benchmark_allocation_pct END) * 100 / COUNT(DISTINCT investment_option_key, date_key)
    display_name: Listed equity % 1 year earlier
    comment: Listed equity target in % at the same quarter 12 months earlier.
    window:
      - order: date
        range: current
        semiadditive: last
        offset: -12 month
  - name: listed_equity_pct_5y_ago
    expr: SUM(CASE WHEN sector_l1 = 'equity' AND listing_type <> 'unlisted' THEN benchmark_allocation_pct END) * 100 / COUNT(DISTINCT investment_option_key, date_key)
    display_name: Listed equity % 5 years earlier
    comment: Listed equity target in % at the same quarter 60 months earlier.
    window:
      - order: date
        range: current
        semiadditive: last
        offset: -60 month
  - name: international_listed_equity_pct_1y_ago
    expr: SUM(CASE WHEN sector_l1 = 'equity' AND listing_type <> 'unlisted' AND domicile_type = 'international' THEN benchmark_allocation_pct END) * 100 / COUNT(DISTINCT investment_option_key, date_key)
    display_name: International listed equity % 1 year earlier
    comment: International listed equity target in % at the same quarter 12 months earlier.
    window:
      - order: date
        range: current
        semiadditive: last
        offset: -12 month
  - name: international_listed_equity_pct_5y_ago
    expr: SUM(CASE WHEN sector_l1 = 'equity' AND listing_type <> 'unlisted' AND domicile_type = 'international' THEN benchmark_allocation_pct END) * 100 / COUNT(DISTINCT investment_option_key, date_key)
    display_name: International listed equity % 5 years earlier
    comment: International listed equity target in % at the same quarter 60 months earlier.
    window:
      - order: date
        range: current
        semiadditive: last
        offset: -60 month
  - name: listed_equity_change_1y_points
    expr: MEASURE(listed_equity_pct) - MEASURE(listed_equity_pct_1y_ago)
    display_name: Listed equity change over 1 year (points)
    comment: Change in listed equity target versus the same quarter 1 year earlier, in percentage points.
    format:
      type: number
      decimal_places:
        type: max
        places: 1
  - name: listed_equity_change_5y_points
    expr: MEASURE(listed_equity_pct) - MEASURE(listed_equity_pct_5y_ago)
    display_name: Listed equity change over 5 years (points)
    comment: Change in listed equity target versus the same quarter 5 years earlier, in percentage points.
    format:
      type: number
      decimal_places:
        type: max
        places: 1
  - name: international_listed_equity_change_1y_points
    expr: MEASURE(international_listed_equity_pct) - MEASURE(international_listed_equity_pct_1y_ago)
    display_name: International listed equity change over 1 year (points)
    comment: Change in international listed equity target versus the same quarter 1 year earlier, in percentage points.
    synonyms: [biggest movers, 1 year change]
    format:
      type: number
      decimal_places:
        type: max
        places: 1
  - name: international_listed_equity_change_5y_points
    expr: MEASURE(international_listed_equity_pct) - MEASURE(international_listed_equity_pct_5y_ago)
    display_name: International listed equity change over 5 years (points)
    comment: Change in international listed equity target versus the same quarter 5 years earlier, in percentage points.
    synonyms: [5 year change]
    format:
      type: number
      decimal_places:
        type: max
        places: 1

  - name: funds_compared
    expr: COUNT(DISTINCT fund)
    display_name: Funds compared
    comment: Number of funds in the result. Use this instead of counting rows.
  - name: options_compared
    expr: COUNT(DISTINCT investment_option_key)
    display_name: Options compared
    comment: Number of investment options in the result.
