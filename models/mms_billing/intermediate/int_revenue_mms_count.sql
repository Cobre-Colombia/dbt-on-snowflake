{{ config(materialized='ephemeral') }}

select
    sequence_customer_id,
    transaction_month,
    count(distinct hash_match) as trx_receiving_platform_fee
from {{ ref('int_revenue_mms') }}
where upper(matched_product_name) NOT IN ('PLATFORM FEE', 'DISCOUNT', 'TRUE UP CHARGE')
group by sequence_customer_id, transaction_month
