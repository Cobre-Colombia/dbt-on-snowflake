with platform_fee_totals as (
    select
        sequence_customer_id,
        transaction_month,
        sum(revenue) as total_platform_fee
    from {{ ref('int_revenue_platform_fee_base') }}
    group by sequence_customer_id, transaction_month
)

select
    mm_id, client_id, pfb.sequence_customer_id, group_id, matched_product_name,
    local_created_at, pfb.transaction_month, transaction_count, amount, cumulative_amount, cumulative_amount_before,
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
        else revenue + ed.total_discount 
    end as revenue_total_adjusted
from {{ ref('int_revenue_platform_fee_base') }} pfb
left join {{ ref('int_revenue_mms_count') }} t
    on pfb.sequence_customer_id = t.sequence_customer_id
    and pfb.transaction_month = t.transaction_month
left join {{ ref('int_revenue_discount_specific') }} ed
    on pfb.sequence_customer_id = ed.sequence_customer_id
    and pfb.transaction_month = ed.transaction_month
