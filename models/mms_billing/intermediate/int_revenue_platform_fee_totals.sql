{{ config(materialized='table') }}

select
    sequence_customer_id,
    transaction_month,
    sum(revenue) as total_platform_fee
from {{ ref('int_revenue_platform_fee_base') }}
group by sequence_customer_id, transaction_month
