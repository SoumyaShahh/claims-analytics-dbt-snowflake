{{ config(materialized='table') }}

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2010-01-01' as date)",
        end_date="cast('2030-12-31' as date)"
    ) }}
)

select
    cast(date_day as date)                      as date_key,
    extract(year from date_day)                 as year_number,
    extract(quarter from date_day)              as quarter_number,
    extract(month from date_day)                as month_number,
    to_char(date_day, 'Month')                  as month_name,
    extract(week from date_day)                 as week_of_year,
    extract(day from date_day)                  as day_of_month,
    extract(dayofweek from date_day)            as day_of_week,
    to_char(date_day, 'Day')                    as day_name,
    case
        when extract(dayofweek from date_day) in (0, 6) then true
        else false
    end                                         as is_weekend,
    to_char(date_day, 'YYYY-MM')                as year_month,
    concat(extract(year from date_day), '-Q', extract(quarter from date_day)) as year_quarter
from date_spine
