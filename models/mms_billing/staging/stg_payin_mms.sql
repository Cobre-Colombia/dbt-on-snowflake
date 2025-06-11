with data as (
    select *, ID AS MM_ID, TOTAL_AMOUNT as AMOUNT,
        CONVERT_TIMEZONE(
                'UTC',
                CASE
                    WHEN COUNTRY = 'COLOMBIA' THEN 'America/Bogota'
                    WHEN COUNTRY = 'MEXICO' THEN 'America/Mexico_City'
                    ELSE 'UTC'
                END,
                TO_TIMESTAMP_NTZ(eventTimestamp)
            ) as utc_created_at
    from {{ source('SALES_OPS', 'PAYIN_TRANSACTIONS') }}
    where  UPDATED_AT::DATE between '{{ var('billing_period_start') }}' and '{{ var('billing_period_end') }}'
    and CLIENT_ID = '{{ var('client_id') }}'

)
select *
from data
