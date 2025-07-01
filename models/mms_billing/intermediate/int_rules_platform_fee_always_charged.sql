{{ config(materialized='view') }}

select
    null as mm_id,
    null as amount,
    r.client_id,
    r.sequence_customer_id,
    r.group_id,
    month as local_created_at,
    r.product_name as matched_product_name,
    r.price_structure_json,
    r.price_minimum_amount,
    r.consumes_saas,
    true as should_be_charged,
    r.currency,
    null as flow,
    null as transaction_type,
    null as origination_system,
    null as source_account_type,
    null as country,
    null as origin_bank,
    null as destination_bank,
    null as status,
    r.property_filters_json,
    null as properties_to_negate,
    date_trunc('month', local_created_at) as transaction_month,
    month as local_updated_at,
    hash(null, sequence_customer_id, matched_product_name, local_created_at, null) as hash_match
from {{ ref('stg_invoice_pricing') }} r
where upper(product_name) = 'PLATFORM FEE'
    and try_parse_json(price_structure_json):pricingType::string = 'FIXED'
