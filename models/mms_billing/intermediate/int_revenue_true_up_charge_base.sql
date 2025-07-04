{{ config(
    materialized='incremental',
    unique_key=['SEQUENCE_CUSTOMER_ID', 'MATCHED_PRODUCT_NAME', 'TRANSACTION_MONTH', 'AMOUNT'],
    incremental_strategy='merge'
) }}

select distinct
    null as mm_id,
    null as client_id,
    r.sequence_customer_id,
    r.group_id,
    r.matched_product_name,
    local_created_at,
    date_trunc('month', local_created_at) as transaction_month,
    1 as transaction_count,
    null as amount,
    null as cumulative_amount,
    null as cumulative_amount_before,
    r.price_structure_json,
    cast(r.price_minimum_amount as number(10, 2)) as price_minimum_revenue,
    'TRUE_UP_CHARGE' as pricing_type,
    r.consumes_saas,
    false as should_be_charged,
    r.currency,
    null as is_percentage,
    cast(r.price_structure_json:price::number as number(10, 2)) as price_per_unit,
    null as tier_application_basis,
    cast(r.price_structure_json:price::number as number(10, 2)) as revenue,
    cast(r.price_structure_json:price::number as number(10, 2)) as cumulative_revenue,
    0 as cumulative_revenue_before,
    case when r.consumes_saas then cast(r.price_structure_json:price::number as number(10, 2)) else 0 end as saas_revenue,
    case when not r.consumes_saas then cast(r.price_structure_json:price::number as number(10, 2)) else 0 end as not_saas_revenue,
    'TRUE_UP_CHARGE' as revenue_type,
    0 as remaining_minimum,
    null as flow,
    null as transaction_type,
    null as origination_system,
    null as source_account_type,
    null as country,
    null as origin_bank,
    null as destination_bank,
    null as status,
    r.property_filters_json,
    r.properties_to_negate,
    local_updated_at
from {{ ref('mart_rules') }} r
where upper(matched_product_name) like '%TRUE UP CHARGE%'
{% if is_incremental() %}
    and r.transaction_month >= dateadd(month, -3, current_date())
{% endif %}
