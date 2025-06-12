with with_date as (
    select
        mm_id,
        amount,
        client_id,
        utc_created_at,
        matched_product_name,
        price_structure_json,
        price_minimum_amount,
        consumes_saas,
        should_be_charged,
        date_trunc('month', utc_created_at) as transaction_month
    from {{ ref('int_mms_with_rules') }}
),

ranked as (
    select *,
        row_number() over (
            partition by matched_product_name, client_id, transaction_month
            order by utc_created_at, mm_id
        ) as transaction_count,

        row_number() over (
            partition by client_id, transaction_month
            order by utc_created_at, mm_id
        ) as global_transaction_order
    from with_date
),

linear_pricing as (
    select
        mm_id, amount, client_id, utc_created_at, matched_product_name,
        price_structure_json, price_minimum_amount, consumes_saas, should_be_charged, transaction_month,
        transaction_count, global_transaction_order,
        price_structure_json:pricePerUnit::float as linear_price_per_unit,
        price_structure_json:isPricePercentage::boolean as linear_is_percentage,
        null::float as tier_price,
        null::float as tier_fee,
        null::int as tier_upper_bound,
        null::boolean as tier_is_percentage,
        upper(price_structure_json:pricingType::string) as pricing_type
    from ranked
    where upper(price_structure_json:pricingType::string) = 'LINEAR'
),

tiered_pricing_raw as (
    select
        r.mm_id, r.amount, r.client_id, r.utc_created_at, r.matched_product_name,
        r.price_structure_json, r.price_minimum_amount, r.consumes_saas, r.should_be_charged, r.transaction_month,
        r.transaction_count, r.global_transaction_order,
        null::float as linear_price_per_unit,
        null::boolean as linear_is_percentage,
        t.value:price::float as tier_price,
        t.value:fee::float as tier_fee,
        t.value:upperBound::int as tier_upper_bound,
        t.value:isPricePercentage::boolean as tier_is_percentage,
        upper(r.price_structure_json:pricingType::string) as pricing_type
    from ranked r
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
            partition by mm_id
            order by
                case
                    when pricing_type = 'GRADUATED' and tier_upper_bound is not null and transaction_count <= tier_upper_bound then 1
                    when pricing_type = 'GRADUATED' and tier_upper_bound is null then 2
                    when pricing_type = 'VOLUME' and tier_upper_bound is not null and transaction_count <= tier_upper_bound then 1
                    when pricing_type = 'VOLUME' and tier_upper_bound is null then 3
                    else 4
                end asc
        ) as tier_rank
    from pricing_all
),

tier_selected as (
    select *
    from tier_ranked
    where pricing_type = 'LINEAR' or tier_rank = 1
),

latest_volume_tier as (
    select distinct
        transaction_month,
        client_id,
        matched_product_name,
        pricing_type,
        max_by(tier_price, transaction_count) over (partition by transaction_month, client_id, matched_product_name) as latest_tier_price,
        max_by(tier_fee, transaction_count) over (partition by transaction_month, client_id, matched_product_name) as latest_tier_fee,
        max_by(tier_is_percentage, transaction_count) over (partition by transaction_month, client_id, matched_product_name) as latest_tier_is_percentage
    from tier_selected
    where pricing_type = 'VOLUME'
),

adjusted as (
    select
        r.*,
        case when r.pricing_type = 'VOLUME' then l.latest_tier_price else r.tier_price end as eff_tier_price,
        case when r.pricing_type = 'VOLUME' then l.latest_tier_fee else r.tier_fee end as eff_tier_fee,
        case when r.pricing_type = 'VOLUME' then l.latest_tier_is_percentage else r.tier_is_percentage end as eff_tier_is_percentage
    from tier_selected r
    left join latest_volume_tier l
      on r.transaction_month = l.transaction_month
     and r.client_id = l.client_id
     and r.matched_product_name = l.matched_product_name
),

calc as (
    select
        *,
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
    select
        *,
        sum(revenue) over (
            partition by matched_product_name, client_id, transaction_month
            order by utc_created_at, mm_id
            rows between unbounded preceding and current row
        ) as cumulative_revenue,

        sum(revenue) over (
            partition by matched_product_name, client_id, transaction_month
            order by utc_created_at, mm_id
            rows between unbounded preceding and 1 preceding
        ) as cumulative_revenue_before,

        sum(revenue) over (
            partition by client_id, transaction_month
            order by global_transaction_order
            rows between unbounded preceding and current row
        ) as cumulative_revenue_global,

        sum(revenue) over (
            partition by client_id, transaction_month
            order by global_transaction_order
            rows between unbounded preceding and 1 preceding
        ) as cumulative_revenue_global_before,

        sum(case when consumes_saas and should_be_charged then revenue else 0 end) over (
            partition by client_id, transaction_month
            order by global_transaction_order
            rows between unbounded preceding and current row
        ) as cumulative_saas_revenue_global,

        sum(case when consumes_saas and should_be_charged then revenue else 0 end) over (
            partition by client_id, transaction_month
            order by global_transaction_order
            rows between unbounded preceding and 1 preceding
        ) as cumulative_saas_revenue_global_before
    from calc
),

global_minimums as (
    select
        client_id,
        transaction_month,
        max(price_minimum_amount) as price_minimum_revenue
    from with_date
    group by client_id, transaction_month
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
      on r.client_id = g.client_id and r.transaction_month = g.transaction_month
)

select
    mm_id,
    client_id,
    matched_product_name,
    utc_created_at,
    transaction_month,
    transaction_count,
    amount,
    price_structure_json,
    to_number(price_minimum_revenue, 10, 2) as price_minimum_revenue,
    pricing_type,
    consumes_saas,
    should_be_charged,
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
    revenue,
    cumulative_revenue,
    cumulative_revenue_before,
    saas_revenue,
    not_saas_revenue,
    revenue_type,
    remaining_minimum
from calc_with_flags
