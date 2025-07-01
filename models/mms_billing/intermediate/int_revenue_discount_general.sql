{{ config(materialized='table') }}

with discount_totals as (
    select
        sequence_customer_id,
        transaction_month,
        split_part(matched_product_name, '||', 2) as discount_applies_to,
        sum(revenue) as total_discount
    from {{ ref('int_revenue_discount_base') }}
    group by sequence_customer_id, transaction_month, matched_product_name
)

select *
from discount_totals
where UPPER(discount_applies_to) like 'DISCOUNT'
