select
    format_date('%Y', transaction_date) as year,
    case when extract(month from transaction_date) <= 6 then 1 else 2 end as semester,
    concat(format_date('%Y', transaction_date), '-S', case when extract(month from transaction_date) <= 6 then 1 else 2 end) as year_semester,
    bank_code,
    sum(income_amount) as income_semester,
    sum(expense_amount) as expense_semester,
    count(*) as movements_count,
    countif(movement_type = 'internal_transfer') as internal_transfer_count
from {{ ref('fact_transactions') }}
group by 1, 2, 3, 4
order by 1, 2, 4
