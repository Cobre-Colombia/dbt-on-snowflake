{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['money_movement_id', 'client_id', 'group_id', 'product_name', 'utc_created_at'],
    post_hook=[
        "grant select on table {{ this }} to role DATA_DEV_L1",
        "grant select on table {{ this }} to role SALES_OPS_DEV_L0"
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
    , cumulative_amount as accumulated_amount
    , price_minimum_revenue as minimum_revenue
    , pricing_type
    , consumes_saas as is_saas_transaction
    , should_be_charged as is_charged
    , revenue
    , cumulative_revenue as accumulated_revenue
    , saas_revenue as saas_revenue_amount
    , not_saas_revenue as non_saas_revenue_amount
    , revenue_type as revenue_type
    , tier_application_basis 
    , utc_updated_at
from {{ ref('int_revenue_cumulative_amount_by_month') }}
where 1=1
{% if is_incremental() %}
    and utc_updated_at > (select max(utc_updated_at) from {{ this }})
{% endif %}

