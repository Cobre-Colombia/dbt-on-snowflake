{{ config(materialized='view') }}

with base as (
select *
from {{ ref('mart_revenue_mms') }}

union all

select *
from {{ ref('mart_revenue_platform_fee') }}

union all

select *
from {{ ref('mart_revenue_discount') }}

union all

select *
from {{ ref('mart_revenue_true_up_charge') }}

)

select 

    mm_id as money_movement_id
    , client_id
    , sequence_customer_id
    , group_id
    , matched_product_name as product_name
    , flow as transaction_flow
    , transaction_type as transaction_type
    , local_created_at
    , transaction_month
    , transaction_count
    , amount
    , price_minimum_revenue as minimum_revenue
    , pricing_type
    , consumes_saas as is_saas_transaction
    , should_be_charged as is_charged
    , revenue
    , cumulative_revenue as accumulated_revenue
    , saas_revenue as saas_revenue_amount
    , not_saas_revenue as non_saas_revenue_amount
    , revenue_type as revenue_type
    , local_updated_at
    , currency
    , remaining_minimum as remaining_minimum_amount
    , tier_application_basis
from base