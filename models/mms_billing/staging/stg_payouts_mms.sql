with data as (
    select *, ID AS MM_ID, TOTAL_AMOUNT as AMOUNT, UTC_CREATED_AT as LOCAL_CREATED_AT
    from {{ source('SALES_OPS', 'PAYOUT_TRANSACTIONS') }}
    where EVENTTIMESTAMP::DATE >= '{{ var('billing_period_start') }}'
    and CLIENT_ID in (
                        {% for cid in var('client_id') %}
                            '{{ cid }}'{% if not loop.last %}, {% endif %}
                        {% endfor %}
                    )

)
select *
from data
