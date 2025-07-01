select
    sequence_customer_id,
    transaction_month,
    count(distinct hash_match) as saas_trx_receiving_remaining_min
from {{ ref('int_revenue_mms') }}
where consumes_saas and should_be_charged
    and upper(matched_product_name) NOT IN ('PLATFORM FEE', 'DISCOUNT', 'TRUE UP CHARGE')
group by sequence_customer_id, transaction_month
