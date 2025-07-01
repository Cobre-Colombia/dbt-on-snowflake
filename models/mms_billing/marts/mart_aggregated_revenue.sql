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
    , cumulative_amount as accumulated_amount
    , price_minimum_revenue as minimum_revenue
    , pricing_type
    , consumes_saas as is_saas_transaction
    , should_be_charged as is_charged
    , currency
    , revenue
    , cumulative_revenue as accumulated_revenue
    , remaining_minimum as remaining_minimum_amount
    , saas_revenue as saas_revenue_amount
    , platform_fee_share
    , remaining_minimum_saas_share
    , revenue_total_adjusted as revenue_total_adjusted_amount
    , not_saas_revenue as non_saas_revenue_amount
    , revenue_type as revenue_type
    , tier_application_basis 
    , local_updated_at
from {{ ref('mart_revenue') }}
where 1=1
{% if is_incremental() %}
    and local_updated_at > (select max(local_updated_at) from {{ this }})
{% endif %}

