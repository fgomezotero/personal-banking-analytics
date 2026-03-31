select
    format_date('%Y', transaction_date) as year,
    extract(quarter from transaction_date) as quarter,
    concat(format_date('%Y', transaction_date), '-Q', extract(quarter from transaction_date)) as year_quarter,
    bank_code,
    sum(income_amount) as income_quarter,
    sum(expense_amount) as expense_quarter,
    count(*) as movements_count,
    countif(movement_type = 'internal_transfer') as internal_transfer_count
from {{ ref('fact_transactions') }}
group by 1, 2, 3, 4
order by 1, 2, 4
