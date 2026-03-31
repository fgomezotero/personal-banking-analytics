select
    format_date('%Y-%m', transaction_date) as year_month,
    bank_code,
    sum(income_amount) as income_month,
    sum(expense_amount) as expense_month,
    count(*) as movements_count,
    countif(movement_type = 'internal_transfer') as internal_transfer_count
from {{ ref('fact_transactions') }}
group by 1, 2
