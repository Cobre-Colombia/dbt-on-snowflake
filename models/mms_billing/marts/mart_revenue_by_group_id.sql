{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['mm_id', 'client_id', 'group_id', 'matched_product_name', 'utc_created_at', 'product_name'],
    post_hook=[
        "grant select on view {{ this }} to role DATA_DEV_L1",
        "grant select on view {{ this }} to role SALES_OPS_DEV_L0"
    ]
) }}

select 
    mm_id as money_movement_id
    , client_id
    , sequence_customer_id
    , group_id
    , matched_product_name as product_name
    , flow as transaction_flow
    , transaction_type as transaction_type
    , utc_created_at
    , transaction_month
    , transaction_count
    , amount
    , price_minimum_revenue as minimum_revenue
    , pricing_type
    , revenue
    , cumulative_revenue as accumulated_revenue
    , saas_revenue as saas_revenue_amount
    , not_saas_revenue as non_saas_revenue_amount
    , revenue_type as revenue_type
    , utc_updated_at
from {{ ref('int_revenue_cumulative_amount') }}
where 1=1
{% if is_incremental() %}
    and utc_updated_at > (select max(utc_updated_at) from {{ this }})
{% endif %}
