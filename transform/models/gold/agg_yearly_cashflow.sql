select
    format_date('%Y', transaction_date) as year,
    bank_code,
    sum(income_amount) as income_year,
    sum(expense_amount) as expense_year,
    count(*) as movements_count,
    countif(movement_type = 'internal_transfer') as internal_transfer_count
from {{ ref('fact_transactions') }}
group by 1, 2
order by 1, 2
