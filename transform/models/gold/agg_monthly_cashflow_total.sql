select
    year_month,
    'all_banks' as bank_scope,
    sum(income_month) as income_month,
    sum(expense_month) as expense_month,
    sum(movements_count) as movements_count,
    sum(internal_transfer_count) as internal_transfer_count
from {{ ref('agg_monthly_cashflow') }}
group by 1
order by year_month desc
