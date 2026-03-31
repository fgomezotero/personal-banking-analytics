select
    to_hex(md5(concat(
        coalesce(bank_code, ''), '|',
        coalesce(cast(transaction_date as string), ''), '|',
        coalesce(reference_number, ''), '|',
        coalesce(description, ''), '|',
        coalesce(cast(debit_amount as string), ''), '|',
        coalesce(cast(credit_amount as string), '')
    ))) as transaction_id,
    bank_code,
    cast(format_date('%Y%m%d', transaction_date) as int64) as transaction_date_key,
    transaction_date,
    value_date,
    description,
    reference_number,
    debit_amount,
    credit_amount,
    movement_type,
    case when movement_type = 'income' then coalesce(credit_amount, 0) else 0 end as income_amount,
    case when movement_type = 'expense' then coalesce(debit_amount, 0) else 0 end as expense_amount,
    case when movement_type = 'internal_transfer' then true else false end as is_internal_transfer,
    currency,
    source_file,
    source_stream,
    ingested_at,
    row_hash
from {{ ref('stg_movimientos') }}
