-- Gold: calendar table, one row per day (generated, not loaded from a file).
-- Covers every full Australian financial year (1 July to 30 June) that has data
-- in the allocation table, so it grows automatically when new quarters arrive.
with date_range as (
    select
        min(date_key) as first_date,
        max(date_key) as last_date
    from {{ ref('stg_fact_investment_strategic_allocation') }}
),

days as (
    select explode(sequence(
        -- 1 July at the start of the first financial year
        make_date(year(first_date) - case when month(first_date) < 7 then 1 else 0 end, 7, 1),
        -- 30 June at the end of the last financial year
        make_date(year(last_date) + case when month(last_date) >= 7 then 1 else 0 end, 6, 30),
        interval 1 day
    )) as date_key
    from date_range
)

select
    date_key,
    year(date_key)                                            as calendar_year,
    quarter(date_key)                                         as calendar_quarter,
    month(date_key)                                           as month_number,
    date_format(date_key, 'MMMM')                             as month_name,
    case when month(date_key) >= 7 then year(date_key) + 1
         else year(date_key) end                              as financial_year,
    case when month(date_key) >= 7 then quarter(date_key) - 2
         else quarter(date_key) + 2 end                       as financial_quarter,
    date_key = last_day(date_key) and month(date_key) in (3, 6, 9, 12)
                                                              as is_quarter_end
from days