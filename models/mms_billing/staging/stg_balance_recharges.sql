with data as (
    select *, ID AS MM_ID, TOTAL_AMOUNT as AMOUNT
    from {{ source('SALES_OPS', 'BALANCE_RECHARGES') }}
    where  EVENTTIMESTAMP::DATE between '{{ var('billing_period_start') }}' and '{{ var('billing_period_end') }}'
    and CLIENT_ID in (
                        {% for cid in var('client_id') %}
                            '{{ cid }}'{% if not loop.last %}, {% endif %}
                        {% endfor %}
                    )
)
select *
from data
