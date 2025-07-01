with with_allocations as (
    select
        r.*,
        p.total_platform_fee,
        gd.total_discount,
        t.trx_receiving_platform_fee,
        s.saas_trx_receiving_remaining_min
    from {{ ref('int_revenue_mms') }} r
    left join {{ ref('int_revenue_platform_fee_totals') }} p
      on r.sequence_customer_id = p.sequence_customer_id
     and r.transaction_month = p.transaction_month
    left join {{ ref('int_revenue_mms_count') }} t
      on r.sequence_customer_id = t.sequence_customer_id
     and r.transaction_month = t.transaction_month
    left join {{ ref('int_revenue_mms_saas_count') }} s
      on r.sequence_customer_id = s.sequence_customer_id
     and r.transaction_month = s.transaction_month
    left join {{ ref('int_revenue_discount_general') }} gd
      on r.sequence_customer_id = gd.sequence_customer_id
      and r.transaction_month = gd.transaction_month
)
,
 final_with_distributions as (
    select r.*,
        MIN(
            CASE 
                WHEN r.saas_trx_receiving_remaining_min > 0 THEN r.remaining_minimum
                ELSE r.price_minimum_revenue
            END
        ) OVER (PARTITION BY r.sequence_customer_id, r.transaction_month) AS REMAINING_MINIMUM_AMOUNT,
        coalesce(case 
            when upper(r.matched_product_name) NOT IN ('PLATFORM FEE', 'DISCOUNT') and r.trx_receiving_platform_fee > 0 
            then r.total_platform_fee / r.trx_receiving_platform_fee 
            else 0 
        end, 0) as platform_fee_share,
        coalesce(case 
            when upper(r.matched_product_name) NOT IN ('PLATFORM FEE', 'DISCOUNT') 
                 and r.consumes_saas and r.should_be_charged 
                 and r.saas_trx_receiving_remaining_min > 0 
            then REMAINING_MINIMUM_AMOUNT / r.saas_trx_receiving_remaining_min 
            else 0 
        end, 0) as remaining_minimum_saas_share,
        
        -- coalesce(case 
        --     when upper(r.matched_product_name) NOT IN ('PLATFORM FEE', 'DISCOUNT') and r.trx_receiving_platform_fee > 0 
        --     then total_discount / r.trx_receiving_platform_fee 
        --     else 0 
        -- end, 0) as discount_share,
        r.revenue 
            + case 
                when upper(r.matched_product_name) NOT IN ('PLATFORM FEE', 'DISCOUNT') and r.trx_receiving_platform_fee > 0 
                then platform_fee_share
                else 0
            end
            + case 
                when upper(r.matched_product_name) NOT IN ('PLATFORM FEE', 'DISCOUNT') 
                 and r.consumes_saas and r.should_be_charged 
                 and r.saas_trx_receiving_remaining_min > 0 
                then remaining_minimum_saas_share
                else 0
            end
            -- + case 
            --     when upper(r.matched_product_name) NOT IN ('PLATFORM FEE', 'DISCOUNT') and r.trx_receiving_platform_fee > 0 
            --     then discount_share
            --     else 0
            -- end
        as revenue_total_adjusted
    from with_allocations r
)

select
    mm_id, client_id, sequence_customer_id, group_id, matched_product_name,
    local_created_at, transaction_month, transaction_count, amount, cumulative_amount, cumulative_amount_before,
    price_structure_json,
    to_number(price_minimum_revenue, 10, 2) as price_minimum_revenue,
    pricing_type, consumes_saas, should_be_charged,
    case 
        when pricing_type = 'LINEAR' then linear_is_percentage
        when pricing_type in ('VOLUME', 'GRADUATED') then eff_tier_is_percentage
        else null
    end as is_percentage,
    case 
        when pricing_type = 'LINEAR' then linear_price_per_unit
        when pricing_type = 'VOLUME' then eff_tier_price
        when pricing_type = 'GRADUATED' then tier_price
        else null
    end as price_per_unit,
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
    platform_fee_share,
    remaining_minimum_saas_share,
    revenue_total_adjusted
from final_with_distributions