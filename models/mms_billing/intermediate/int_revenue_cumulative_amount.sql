with with_date as (
    select
        mm_id,
        amount,
        client_id,
        utc_created_at,
        matched_product_name,
        price_structure_json,
        price_minimum_amount,
        date_trunc('month', utc_created_at) as transaction_month
    from {{ ref('int_mms_with_rules') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by matched_product_name, client_id, transaction_month
            order by utc_created_at, mm_id
        ) as transaction_count
    from with_date
),

tiers as (
    select
        r.mm_id,
        r.amount,
        r.client_id,
        r.utc_created_at,
        r.transaction_month,
        r.matched_product_name,
        r.price_structure_json,
        r.price_minimum_amount,
        r.transaction_count,
        t.value:price::float as tier_price,
        t.value:fee::float as tier_fee,
        t.value:upperBound::int as tier_upper_bound,
        t.value:isPricePercentage::boolean as tier_is_percentage
    from ranked r
    left join lateral flatten(input => r.price_structure_json:tiers) t
),

exploded as (
    select
        *,
        upper(price_structure_json:pricingType::string) as pricing_type,
        price_structure_json:isPricePercentage::boolean as linear_is_percentage,
        price_structure_json:pricePerUnit::float as linear_price_per_unit,
        row_number() over (
            partition by mm_id, matched_product_name
            order by
                case
                    when tier_upper_bound is null then 2
                    when tier_upper_bound >= transaction_count then 1
                    else 3
                end,
                tier_upper_bound
        ) as tier_rank
    from tiers
    where pricing_type in ('LINEAR', 'VOLUME', 'GRADUATED')
),

exploded_filtered as (
    select *
    from exploded
    where tier_rank = 1
),

latest_volume_tier as (
    select distinct
        transaction_month,
        client_id,
        matched_product_name,
        pricing_type,
        max_by(tier_price, transaction_count) over (partition by transaction_month, client_id, matched_product_name) as latest_tier_price,
        max_by(tier_fee, transaction_count) over (partition by transaction_month, client_id, matched_product_name) as latest_tier_fee,
        max_by(tier_is_percentage, transaction_count) over (partition by transaction_month, client_id, matched_product_name) as latest_tier_is_percentage,
        max_by(tier_upper_bound, transaction_count) over (partition by transaction_month, client_id, matched_product_name) as latest_tier_upper_bound
    from exploded_filtered
    where pricing_type = 'VOLUME'
),

adjusted as (
    select
        e.*,
        case when e.pricing_type = 'VOLUME' then l.latest_tier_price else e.tier_price end as eff_tier_price,
        case when e.pricing_type = 'VOLUME' then l.latest_tier_fee else e.tier_fee end as eff_tier_fee,
        case when e.pricing_type = 'VOLUME' then l.latest_tier_is_percentage else e.tier_is_percentage end as eff_tier_is_percentage
    from exploded_filtered e
    left join latest_volume_tier l
      on e.transaction_month = l.transaction_month
     and e.client_id = l.client_id
     and e.matched_product_name = l.matched_product_name
),

calc as (
    select
        *,
        case 
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
        ) as cumulative_revenue_before
    from calc
),

calc_with_flags as (
    select
        *,
        case
            when cumulative_revenue_before >= coalesce(price_minimum_amount, 0) then 0
            when cumulative_revenue > coalesce(price_minimum_amount, 0) then
                coalesce(price_minimum_amount, 0) - coalesce(cumulative_revenue_before, 0)
            else revenue
        end as saas_revenue,

        case
            when cumulative_revenue > coalesce(price_minimum_amount, 0) then
                revenue - greatest(coalesce(price_minimum_amount, 0) - coalesce(cumulative_revenue_before, 0), 0)
            else 0
        end as not_saas_revenue
    from ranked_revenue
),

revenue_structured as (
    select
        *,
        case
            when saas_revenue > 0 and not_saas_revenue = 0 then 'saas'
            when saas_revenue = 0 and not_saas_revenue > 0 then 'post_minimum'
            when saas_revenue > 0 and not_saas_revenue > 0 then 'mixed'
            else null
        end as revenue_type,

        to_number(price_minimum_amount, 10, 2) as price_minimum_revenue,

        array_construct(
            object_construct_keep_null(
                'match_name', matched_product_name,
                'pricing_type', pricing_type,
                'is_percentage',
                    case when pricing_type = 'LINEAR' then linear_is_percentage
                         when pricing_type = 'VOLUME' then eff_tier_is_percentage
                         when pricing_type = 'GRADUATED' then tier_is_percentage
                         else null end,
                'price', to_number(
                    case when pricing_type = 'LINEAR' then linear_price_per_unit
                         when pricing_type = 'VOLUME' then eff_tier_price
                         when pricing_type = 'GRADUATED' then tier_price
                         else null end, 10, 2),
                'fee', to_number(coalesce(eff_tier_fee, tier_fee, 0), 10, 2),
                'transaction_amount', to_number(amount, 18, 2),
                'transaction_count', transaction_count,
                'cumulative_revenue', to_number(cumulative_revenue, 18, 2),
                'revenue', to_number(revenue, 18, 2),
                'saas_revenue', to_number(saas_revenue, 18, 2),
                'not_saas_revenue', to_number(not_saas_revenue, 18, 2),
                'revenue_type',
                    case
                        when saas_revenue > 0 and not_saas_revenue = 0 then 'saas'
                        when saas_revenue = 0 and not_saas_revenue > 0 then 'post_minimum'
                        when saas_revenue > 0 and not_saas_revenue > 0 then 'mixed'
                        else null
                    end,
                'formula_applied',
                    case
                        when pricing_type = 'LINEAR' and linear_is_percentage then 'amount * (price_per_unit / 100)'
                        when pricing_type = 'LINEAR' and not linear_is_percentage then 'fixed price_per_unit'
                        when pricing_type = 'VOLUME' and eff_tier_is_percentage then 'amount * (tier_price / 100) + fee'
                        when pricing_type = 'VOLUME' and not eff_tier_is_percentage then 'tier_price + fee'
                        when pricing_type = 'GRADUATED' and tier_is_percentage then 'amount * (tier_price / 100) + fee'
                        when pricing_type = 'GRADUATED' and not tier_is_percentage then 'tier_price + fee'
                        else 'unknown'
                    end
            )
        ) as revenue_calculation_details
    from calc_with_flags
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
    price_minimum_revenue,

    pricing_type,
    case 
        when pricing_type = 'LINEAR' then linear_is_percentage
        when pricing_type in ('VOLUME', 'GRADUATED') then eff_tier_is_percentage
        else null
    end as is_percentage,

    case 
        when pricing_type = 'LINEAR' then linear_price_per_unit
        when pricing_type in ('VOLUME', 'GRADUATED') then eff_tier_price
        else null
    end as price_per_unit,

    revenue,
    cumulative_revenue,
    cumulative_revenue_before,
    saas_revenue,
    not_saas_revenue,
    revenue_type,
    revenue_calculation_details
from revenue_structured
