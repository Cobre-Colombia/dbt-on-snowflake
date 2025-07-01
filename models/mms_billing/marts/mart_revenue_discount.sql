select
    mm_id, client_id, db.sequence_customer_id, group_id, matched_product_name,
    local_created_at, db.transaction_month, transaction_count, amount, cumulative_amount, cumulative_amount_before,
    price_structure_json,
    price_minimum_revenue,
    pricing_type, consumes_saas, should_be_charged,
    is_percentage,
    price_per_unit,
    tier_application_basis,
    currency,
    revenue,
    cumulative_revenue,
    cumulative_revenue_before,
    saas_revenue,
    not_saas_revenue,
    revenue_type,
    remaining_minimum,
    flow, transaction_type, origination_system, source_account_type,
    country, origin_bank, destination_bank, status,
    property_filters_json, properties_to_negate,
    local_updated_at,
    null as platform_fee_share,
    null as remaining_minimum_saas_share,
    case 
        when trx_receiving_platform_fee > 0 then 0
        else revenue 
    end as revenue_total_adjusted
from {{ ref('int_revenue_discount_base') }} db
left join {{ ref('int_revenue_mms_count') }} t
    on db.sequence_customer_id = t.sequence_customer_id
    and db.transaction_month = t.transaction_month