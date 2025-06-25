{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['mm_id', 'sequence_customer_id', 'matched_product_name', 'local_created_at'],
    post_hook=[
        "grant select on table {{ this }} to role DATA_DEV_L1"
    ]
) }}

with with_date as (
    select
        mm_id, amount, client_id, sequence_customer_id, group_id, local_created_at,
        matched_product_name, price_structure_json, price_minimum_amount,
        consumes_saas, should_be_charged,
        flow, transaction_type, origination_system, source_account_type,
        country, origin_bank, destination_bank, status,
        property_filters_json, properties_to_negate,
        date_trunc('month', local_created_at) as transaction_month,
        updated_at as local_updated_at, hash(mm_id, sequence_customer_id, matched_product_name, local_created_at, amount) as hash_match
    from {{ ref('int_mms_with_rules_by_month') }}
    where upper(matched_product_name) not in ('DISCOUNT', 'PLATFORM FEE')
),
platform_fee_base as (
    select distinct
        null as mm_id,
        r.client_id,
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
        'FIXED' as pricing_type,
        r.consumes_saas,
        true as should_be_charged,
        null as is_percentage,
        cast(r.price_structure_json:price::number as number(10, 2)) as price_per_unit,
        null as tier_application_basis,
        cast(r.price_structure_json:price::number as number(10, 2)) as revenue,
        cast(r.price_structure_json:price::number as number(10, 2)) as cumulative_revenue,
        0 as cumulative_revenue_before,
        case when r.consumes_saas then cast(r.price_structure_json:price::number as number(10, 2)) else 0 end as saas_revenue,
        case when not r.consumes_saas then cast(r.price_structure_json:price::number as number(10, 2)) else 0 end as not_saas_revenue,
        case 
            when not r.consumes_saas then 'non_consuming'
            else 'saas'
        end as revenue_type,
        greatest(
            cast(r.price_minimum_amount as number(10, 2)) 
            - cast(r.price_structure_json:price::number as number(10, 2)), 0
        ) as remaining_minimum,
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
        updated_at as local_updated_at
    from {{ ref('int_mms_with_rules_by_month') }} r
    where upper(matched_product_name) = 'PLATFORM FEE'
      and r.price_structure_json:pricingType::string = 'FIXED'
),

discount_base as (
    select distinct
        null as mm_id,
        r.client_id,
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
        'DISCOUNT' as pricing_type,
        r.consumes_saas,
        true as should_be_charged,
        null as is_percentage,
        cast(r.price_structure_json:price::number as number(10, 2)) as price_per_unit,
        null as tier_application_basis,
        cast(r.price_structure_json:price::number as number(10, 2)) as revenue,
        cast(r.price_structure_json:price::number as number(10, 2)) as cumulative_revenue,
        0 as cumulative_revenue_before,
        case when r.consumes_saas then cast(r.price_structure_json:price::number as number(10, 2)) else 0 end as saas_revenue,
        case when not r.consumes_saas then cast(r.price_structure_json:price::number as number(10, 2)) else 0 end as not_saas_revenue,
        'discount' as revenue_type,
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
        updated_at as local_updated_at
    from {{ ref('int_mms_with_rules_by_month') }} r
    where upper(matched_product_name) = 'DISCOUNT'
),
ranked as (
    select *,
        row_number() over (
            partition by matched_product_name, sequence_customer_id, transaction_month
            order by local_created_at, mm_id, hash_match
        ) as transaction_count,
        row_number() over (
            partition by sequence_customer_id, transaction_month
            order by local_created_at, mm_id, hash_match
        ) as global_transaction_order
    from with_date
),
amount_accumulated as (
    select *,
        sum(amount) over (
            partition by matched_product_name, sequence_customer_id, transaction_month
            order by local_created_at, mm_id, hash_match
            rows between unbounded preceding and current row
        ) as cumulative_amount,
        sum(amount) over (
            partition by matched_product_name, sequence_customer_id, transaction_month
            order by local_created_at, mm_id, hash_match
            rows between unbounded preceding and 1 preceding
        ) as cumulative_amount_before
    from ranked
),
linear_pricing as (
    select
        *, 
        price_structure_json:pricePerUnit::float as linear_price_per_unit,
        price_structure_json:isPricePercentage::boolean as linear_is_percentage,
        null::float as tier_price,
        null::float as tier_fee,
        null::int as tier_upper_bound,
        null::boolean as tier_is_percentage,
        upper(price_structure_json:pricingType::string) as pricing_type
    from amount_accumulated
    where upper(price_structure_json:pricingType::string) = 'LINEAR'
),
tiered_pricing_raw as (
    select
        r.*,
        null::float as linear_price_per_unit,
        null::boolean as linear_is_percentage,
        t.value:price::float as tier_price,
        t.value:fee::float as tier_fee,
        t.value:upperBound::int as tier_upper_bound,
        t.value:isPricePercentage::boolean as tier_is_percentage,
        upper(r.price_structure_json:pricingType::string) as pricing_type
    from amount_accumulated r
    left join lateral flatten(input => r.price_structure_json:tiers) t
    where upper(r.price_structure_json:pricingType::string) in ('VOLUME', 'GRADUATED')
),
pricing_all as (
    select * from linear_pricing
    union all
    select * from tiered_pricing_raw
),
tier_ranked as (
    select *,
        row_number() over (
            partition by mm_id, hash_match
            order by
                case
                    when pricing_type = 'GRADUATED' and tier_is_percentage and tier_upper_bound is not null and cumulative_amount <= tier_upper_bound then 1
                    when pricing_type = 'GRADUATED' and tier_is_percentage and tier_upper_bound is null then 2
                    when pricing_type = 'GRADUATED' and not tier_is_percentage and tier_upper_bound is not null and transaction_count <= tier_upper_bound then 3
                    when pricing_type = 'GRADUATED' and not tier_is_percentage and tier_upper_bound is null then 4
                    when pricing_type = 'VOLUME' and tier_is_percentage and tier_upper_bound is not null and cumulative_amount <= tier_upper_bound then 5
                    when pricing_type = 'VOLUME' and tier_is_percentage and tier_upper_bound is null then 6
                    when pricing_type = 'VOLUME' and not tier_is_percentage and tier_upper_bound is not null and transaction_count <= tier_upper_bound then 7
                    when pricing_type = 'VOLUME' and not tier_is_percentage and tier_upper_bound is null then 8
                    else 9
                end
        ) as tier_rank
    from pricing_all
),
tier_selected as (
    select * from tier_ranked
    where pricing_type = 'LINEAR' or tier_rank = 1
)

,
latest_volume_tier as (
    select distinct
        transaction_month,
        sequence_customer_id,
        matched_product_name,
        pricing_type,
        max_by(tier_price, transaction_count) over (partition by transaction_month, sequence_customer_id, matched_product_name) as latest_tier_price,
        max_by(tier_fee, transaction_count) over (partition by transaction_month, sequence_customer_id, matched_product_name) as latest_tier_fee,
        max_by(tier_is_percentage, transaction_count) over (partition by transaction_month, sequence_customer_id, matched_product_name) as latest_tier_is_percentage
    from tier_selected
    where pricing_type = 'VOLUME'
),
adjusted as (
    select
        r.*,
        case when r.pricing_type = 'VOLUME' then l.latest_tier_price else r.tier_price end as eff_tier_price,
        case when r.pricing_type = 'VOLUME' then l.latest_tier_fee else r.tier_fee end as eff_tier_fee,
        case when r.pricing_type = 'VOLUME' then l.latest_tier_is_percentage else r.tier_is_percentage end as eff_tier_is_percentage,
        case
            when r.pricing_type = 'VOLUME' and l.latest_tier_is_percentage then 'amount'
            when r.pricing_type = 'VOLUME' and not l.latest_tier_is_percentage then 'count'
            when r.pricing_type = 'GRADUATED' and r.tier_is_percentage then 'amount'
            when r.pricing_type = 'GRADUATED' and not r.tier_is_percentage then 'count'
            when r.pricing_type = 'LINEAR' and r.linear_is_percentage then 'amount'
            when r.pricing_type = 'LINEAR' and not r.linear_is_percentage then 'count'
            else null
        end as tier_application_basis
    from tier_selected r
    left join latest_volume_tier l
      on r.transaction_month = l.transaction_month
     and r.sequence_customer_id = l.sequence_customer_id
     and r.matched_product_name = l.matched_product_name
),
calc as (
    select *,
        case 
            when not should_be_charged then 0
            when pricing_type = 'LINEAR' and linear_is_percentage then amount * (linear_price_per_unit / 100)
            when pricing_type = 'LINEAR' and not linear_is_percentage then linear_price_per_unit
            when pricing_type = 'VOLUME' and eff_tier_is_percentage then (amount * (eff_tier_price / 100)) + coalesce(eff_tier_fee, 0)
            when pricing_type = 'VOLUME' and not eff_tier_is_percentage then eff_tier_price + coalesce(eff_tier_fee, 0)
            when pricing_type = 'GRADUATED' and tier_is_percentage then (amount * (tier_price / 100)) + coalesce(tier_fee, 0)
            when pricing_type = 'GRADUATED' and not tier_is_percentage then tier_price + coalesce(tier_fee, 0)
            else 0
        end as revenue
    from adjusted
),
ranked_revenue as (
    select *,
        sum(revenue) over (
            partition by matched_product_name, sequence_customer_id, transaction_month
            order by local_created_at, mm_id, hash_match
            rows between unbounded preceding and current row
        ) as cumulative_revenue,
        sum(revenue) over (
            partition by matched_product_name, sequence_customer_id, transaction_month
            order by local_created_at, mm_id, hash_match
            rows between unbounded preceding and 1 preceding
        ) as cumulative_revenue_before,
        sum(revenue) over (
            partition by sequence_customer_id, transaction_month
            order by global_transaction_order
            rows between unbounded preceding and current row
        ) as cumulative_revenue_global,
        sum(revenue) over (
            partition by sequence_customer_id, transaction_month
            order by global_transaction_order
            rows between unbounded preceding and 1 preceding
        ) as cumulative_revenue_global_before,
        sum(case when consumes_saas and should_be_charged then revenue else 0 end) over (
            partition by sequence_customer_id, transaction_month
            order by global_transaction_order
            rows between unbounded preceding and current row
        ) as cumulative_saas_revenue_global,
        sum(case when consumes_saas and should_be_charged then revenue else 0 end) over (
            partition by sequence_customer_id, transaction_month
            order by global_transaction_order
            rows between unbounded preceding and 1 preceding
        ) as cumulative_saas_revenue_global_before
    from calc
),
global_minimums as (
    select
        sequence_customer_id,
        transaction_month,
        max(price_minimum_amount) as price_minimum_revenue
    from with_date
    group by sequence_customer_id, transaction_month
),
calc_with_flags as (
    select
        r.*,
        g.price_minimum_revenue,
        case
            when not should_be_charged then 0
            when not consumes_saas then 0
            when cumulative_saas_revenue_global_before >= coalesce(g.price_minimum_revenue, 0) then 0
            when cumulative_saas_revenue_global > coalesce(g.price_minimum_revenue, 0) then
                coalesce(g.price_minimum_revenue, 0) - coalesce(cumulative_saas_revenue_global_before, 0)
            else revenue
        end as saas_revenue,
        case
            when not should_be_charged then 0
            when not consumes_saas then revenue
            when cumulative_saas_revenue_global > coalesce(g.price_minimum_revenue, 0) then
                revenue - greatest(coalesce(g.price_minimum_revenue, 0) - coalesce(cumulative_saas_revenue_global_before, 0), 0)
            else 0
        end as not_saas_revenue,
        greatest(coalesce(g.price_minimum_revenue, 0) - coalesce(cumulative_saas_revenue_global, 0), 0) as remaining_minimum,
        case
            when not should_be_charged then 'excluded'
            when not consumes_saas then 'non_consuming'
            when saas_revenue > 0 and not_saas_revenue = 0 then 'saas'
            when saas_revenue = 0 and not_saas_revenue > 0 then 'post_minimum'
            when saas_revenue > 0 and not_saas_revenue > 0 then 'mixed'
            else null
        end as revenue_type
    from ranked_revenue r
    left join global_minimums g
      on r.sequence_customer_id = g.sequence_customer_id
     and r.transaction_month = g.transaction_month
)

,
platform_fee_totals as (
    select
        sequence_customer_id,
        transaction_month,
        sum(revenue) as total_platform_fee
    from platform_fee_base
    group by sequence_customer_id, transaction_month
),
trx_counts as (
    select
        sequence_customer_id,
        transaction_month,
        count(distinct hash_match) as trx_receiving_platform_fee
    from calc_with_flags
    where upper(matched_product_name) != 'PLATFORM FEE'
    group by sequence_customer_id, transaction_month
),
saas_trx_counts as (
    select
        sequence_customer_id,
        transaction_month,
        count(distinct hash_match) as saas_trx_receiving_remaining_min
    from calc_with_flags
    where consumes_saas and should_be_charged
      and upper(matched_product_name) != 'PLATFORM FEE'
    group by sequence_customer_id, transaction_month
),
with_allocations as (
    select
        r.*,
        p.total_platform_fee,
        t.trx_receiving_platform_fee,
        s.saas_trx_receiving_remaining_min
    from calc_with_flags r
    left join platform_fee_totals p
      on r.sequence_customer_id = p.sequence_customer_id
     and r.transaction_month = p.transaction_month
    left join trx_counts t
      on r.sequence_customer_id = t.sequence_customer_id
     and r.transaction_month = t.transaction_month
    left join saas_trx_counts s
      on r.sequence_customer_id = s.sequence_customer_id
     and r.transaction_month = s.transaction_month
),
final_with_distributions as (
    select *,
        MIN(
            CASE 
                WHEN saas_trx_receiving_remaining_min > 0 THEN REMAINING_MINIMUM
                ELSE PRICE_MINIMUM_REVENUE
            END
        ) OVER (PARTITION BY sequence_customer_id, transaction_month) AS REMAINING_MINIMUM_AMOUNT,
        case 
            when upper(matched_product_name) != 'PLATFORM FEE' and trx_receiving_platform_fee > 0 
            then total_platform_fee / trx_receiving_platform_fee 
            else 0 
        end as platform_fee_share,
        case 
            when upper(matched_product_name) != 'PLATFORM FEE' 
                 and consumes_saas and should_be_charged 
                 and saas_trx_receiving_remaining_min > 0 
            then REMAINING_MINIMUM_AMOUNT / saas_trx_receiving_remaining_min 
            else 0 
        end as remaining_minimum_saas_share,
        revenue 
            + case 
                when upper(matched_product_name) != 'PLATFORM FEE' and trx_receiving_platform_fee > 0 
                then total_platform_fee / trx_receiving_platform_fee 
                else 0 
              end
            + case 
                when upper(matched_product_name) != 'PLATFORM FEE' 
                     and consumes_saas and should_be_charged 
                     and saas_trx_receiving_remaining_min > 0 
                then REMAINING_MINIMUM_AMOUNT / saas_trx_receiving_remaining_min 
                else 0 
              end
        as revenue_total_adjusted
    from with_allocations
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

union all

select
    mm_id, client_id, pfb.sequence_customer_id, group_id, matched_product_name,
    local_created_at, pfb.transaction_month, transaction_count, amount, cumulative_amount, cumulative_amount_before,
    price_structure_json,
    price_minimum_revenue,
    pricing_type, consumes_saas, should_be_charged,
    is_percentage,
    price_per_unit,
    tier_application_basis,
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
from platform_fee_base pfb
left join trx_counts t
    on pfb.sequence_customer_id = t.sequence_customer_id
    and pfb.transaction_month = t.transaction_month

union all

select
    mm_id, client_id, db.sequence_customer_id, group_id, matched_product_name,
    local_created_at, db.transaction_month, transaction_count, amount, cumulative_amount, cumulative_amount_before,
    price_structure_json,
    price_minimum_revenue,
    pricing_type, consumes_saas, should_be_charged,
    is_percentage,
    price_per_unit,
    tier_application_basis,
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
from discount_base db
left join trx_counts t
    on db.sequence_customer_id = t.sequence_customer_id
    and db.transaction_month = t.transaction_month

{% if is_incremental() %}
    where local_updated_at > (select max(local_updated_at) from {{ this }})
{% endif %}



