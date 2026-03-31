with itau as (
    select
        'itau' as bank_code,
        cast(null as string) as account_id_raw,
        {{ parse_mixed_date('fecha') }} as transaction_date,
        cast(null as date) as value_date,
        cast(concepto as string) as description,
        cast(referencia as string) as reference_number,
        abs(safe_cast(debito as float64)) as debit_amount,
        abs(safe_cast(credito as float64)) as credit_amount,
        safe_cast(saldo as float64) as balance_amount,
        'UYU' as currency,
        cast(_sdc_filename as string) as source_file,
        cast(_sdc_stream as string) as source_stream,
        _sdc_batched_at as ingested_at
    from {{ source('bronze', 'itau_debito') }}
),
scotia as (
    select
        'scotia' as bank_code,
        cast(suc as string) as account_id_raw,
        {{ parse_mixed_date('fecha') }} as transaction_date,
        {{ parse_mixed_date('fecha_valor') }} as value_date,
        cast(descripcion as string) as description,
        cast(comprobante as string) as reference_number,
        abs({{ parse_amount('debito') }}) as debit_amount,
        abs({{ parse_amount('credito') }}) as credit_amount,
        cast(null as float64) as balance_amount,
        'UYU' as currency,
        cast(_sdc_filename as string) as source_file,
        cast(_sdc_stream as string) as source_stream,
        _sdc_batched_at as ingested_at
    from {{ source('bronze', 'scotia_debito') }}
),
bbva as (
    select
        'bbva' as bank_code,
        cast(null as string) as account_id_raw,
        {{ parse_mixed_date('fecha') }} as transaction_date,
        cast(null as date) as value_date,
        cast(concepto as string) as description,
        cast(referencia as string) as reference_number,
        abs({{ parse_amount('debito') }}) as debit_amount,
        abs({{ parse_amount('credito') }}) as credit_amount,
        {{ parse_amount('saldo') }} as balance_amount,
        'UYU' as currency,
        cast(_sdc_filename as string) as source_file,
        cast(_sdc_stream as string) as source_stream,
        _sdc_batched_at as ingested_at
    from {{ source('bronze', 'bbva_debito') }}
),
unified as (
    select * from itau
    union all
    select * from scotia
    union all
    select * from bbva
),
prepared as (
    select
        u.*,
        to_hex(md5(concat(
            coalesce(u.bank_code, ''), '|',
            coalesce(cast(u.transaction_date as string), ''), '|',
            coalesce(u.reference_number, ''), '|',
            coalesce(u.description, ''), '|',
            coalesce(cast(u.debit_amount as string), ''), '|',
            coalesce(cast(u.credit_amount as string), '')
        ))) as row_hash
    from unified u
    where u.transaction_date is not null
),
credit_transfer_matches as (
    select distinct c.row_hash
    from prepared c
    inner join prepared d
        on d.bank_code <> c.bank_code
        and round(coalesce(d.debit_amount, 0), 2) = round(coalesce(c.credit_amount, 0), 2)
        and d.transaction_date between date_sub(c.transaction_date, interval 7 day) and c.transaction_date
    where coalesce(c.credit_amount, 0) > 0
        and coalesce(d.debit_amount, 0) > 0
),
debit_transfer_matches as (
    select distinct d.row_hash
    from prepared d
    inner join prepared c
        on c.bank_code <> d.bank_code
        and round(coalesce(c.credit_amount, 0), 2) = round(coalesce(d.debit_amount, 0), 2)
        and c.transaction_date between d.transaction_date and date_add(d.transaction_date, interval 7 day)
    where coalesce(d.debit_amount, 0) > 0
        and coalesce(c.credit_amount, 0) > 0
),
classified as (
    select
        p.bank_code,
        p.account_id_raw,
        p.transaction_date,
        p.value_date,
        p.description,
        p.reference_number,
        p.debit_amount,
        p.credit_amount,
        p.balance_amount,
        p.currency,
        p.source_file,
        p.source_stream,
        p.ingested_at,
        case
            when (
                coalesce(p.credit_amount, 0) > 0
                and ctm.row_hash is not null
            )
                or (
                    coalesce(p.debit_amount, 0) > 0
                    and dtm.row_hash is not null
                )
                then 'internal_transfer'
            when coalesce(p.credit_amount, 0) > 0 and coalesce(p.debit_amount, 0) = 0 then 'income'
            else 'expense'
        end as movement_type,
        p.row_hash
    from prepared p
    left join credit_transfer_matches ctm
        on ctm.row_hash = p.row_hash
    left join debit_transfer_matches dtm
        on dtm.row_hash = p.row_hash
),
dedup as (
    select *
    from classified
    qualify row_number() over (
        partition by row_hash
        order by ingested_at desc
    ) = 1
)
select * from dedup
