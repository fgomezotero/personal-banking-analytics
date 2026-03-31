with bounds as (
    select
        min(transaction_date) as min_date,
        max(transaction_date) as max_date
    from {{ ref('stg_movimientos') }}
),
calendar as (
    select day as date_day
    from bounds,
    unnest(generate_date_array(min_date, max_date)) as day
)
select
    cast(format_date('%Y%m%d', date_day) as int64) as date_key,
    date_day as date_value,
    extract(year from date_day) as year,
    extract(month from date_day) as month,
    extract(day from date_day) as day,
    format_date('%Y-%m', date_day) as year_month,
    extract(quarter from date_day) as quarter,
    extract(month from date_day) as month_of_semester,
    case when extract(month from date_day) <= 6 then 1 else 2 end as semester,
    concat(extract(year from date_day), '-S', case when extract(month from date_day) <= 6 then 1 else 2 end) as year_semester,
    extract(dayofweek from date_day) in (1, 7) as is_weekend
from calendar
