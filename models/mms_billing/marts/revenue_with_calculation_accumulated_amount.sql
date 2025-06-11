with data as (
    select * from {{ ref('enriched_mms_with_rules') }}
),

with_date as (
    select
        *,
        date_trunc('month', utc_created_at) as transaction_month
    from data
),

ranked as (
    select
        *,
        row_number() over (
            partition by matched_product_name, client_id, date_trunc('month', utc_created_at)
            order by utc_created_at, mm_id
        ) as transaction_count
    from with_date
),

with_cumulative_amount as (
    select
        *,
        sum(amount) over (
            partition by matched_product_name, client_id, transaction_month
            order by utc_created_at, mm_id
            rows between unbounded preceding and current row
        ) as cumulative_amount
    from ranked
),

exploded as (
    select
        r.*,
        upper(r.price_structure_json:pricingType::string) as pricing_type,
        r.price_structure_json:isPricePercentage::boolean as linear_is_percentage,
        r.price_structure_json:pricePerUnit::float as linear_price_per_unit,
        t.value:price::float as tier_price,
        t.value:fee::float as tier_fee,
        t.value:upperBound::int as tier_upper_bound,
        t.value:isPricePercentage::boolean as tier_is_percentage,
        row_number() over (
            partition by r.mm_id, r.matched_product_name
            order by
                case
                    when t.value:upperBound is null then 2
                    when t.value:upperBound::int >= r.transaction_count then 1
                    else 3
                end,
                t.value:upperBound::int
        ) as tier_rank
    from with_cumulative_amount r
    left join lateral flatten(input => r.price_structure_json:tiers) t
),

calc as (
    select
        *,
        row_number() over (partition by mm_id order by matched_product_name) as regla_numero,
        case 
            when pricing_type = 'LINEAR' and linear_is_percentage = true then amount * (linear_price_per_unit / 100)
            when pricing_type = 'LINEAR' and linear_is_percentage = false then linear_price_per_unit
            when pricing_type = 'VOLUME' and cumulative_amount >= coalesce(price_minimum_amount, 0) and tier_is_percentage = true then (amount * (tier_price / 100)) + coalesce(tier_fee, 0)
            when pricing_type = 'VOLUME' and cumulative_amount >= coalesce(price_minimum_amount, 0) and tier_is_percentage = false then tier_price + coalesce(tier_fee, 0)
            else 0
        end as revenue
    from exploded
    where pricing_type = 'LINEAR' or tier_rank = 1
),

revenue_structured as (
    select
        *,
        array_construct(
            object_construct_keep_null(
                'regla_numero', regla_numero,
                'match_name', matched_product_name,
                'pricing_type', pricing_type,
                'tier_selected', 
                    case when pricing_type = 'VOLUME' then 
                        'Tier: hasta ' || coalesce(tier_upper_bound::string, 'Sin límite') || ' transacciones'
                    else null end,
                'is_percentage', 
                    case when pricing_type = 'LINEAR' then linear_is_percentage
                         when pricing_type = 'VOLUME' then tier_is_percentage
                         else null end,
                'price', 
                    to_number(coalesce(tier_price, linear_price_per_unit), 10, 2),
                'fee', 
                    to_number(coalesce(tier_fee, 0), 10, 2),
                'transaction_amount', to_number(amount, 18, 2),
                'transaction_count', transaction_count,
                'cumulative_amount', to_number(cumulative_amount, 18, 2),
                'revenue', to_number(revenue, 18, 2),
                'calculation_method',
                    case
                        when pricing_type = 'LINEAR' and linear_is_percentage = true then 'Porcentaje'
                        when pricing_type = 'LINEAR' and linear_is_percentage = false then 'Carga fija'
                        when pricing_type = 'VOLUME' and tier_is_percentage = true then 'Volumen - Porcentaje'
                        when pricing_type = 'VOLUME' and tier_is_percentage = false then 'Volumen - Carga fija'
                        else 'No aplica'
                    end,
                'calculation_formula',
                    case
                        when pricing_type = 'LINEAR' and linear_is_percentage = true then 
                            to_varchar(amount, 'FM999,999.00') || ' × ' || to_varchar(linear_price_per_unit, 'FM999,999.00') || '% = ' || to_varchar(revenue, 'FM999,999.00')
                        when pricing_type = 'LINEAR' and linear_is_percentage = false then 
                            to_varchar(linear_price_per_unit, 'FM999,999.00')
                        when pricing_type = 'VOLUME' and cumulative_amount >= coalesce(price_minimum_amount, 0) and tier_is_percentage = true then 
                            to_varchar(amount, 'FM999,999.00') || ' × ' || to_varchar(tier_price, 'FM999,999.00') || '%' ||
                            coalesce(' + fee(' || to_varchar(tier_fee, 'FM999,999.00') || ')', '') ||
                            ' = ' || to_varchar(revenue, 'FM999,999.00')
                        when pricing_type = 'VOLUME' and cumulative_amount >= coalesce(price_minimum_amount, 0) and tier_is_percentage = false then 
                            to_varchar(tier_price, 'FM999,999.00') ||
                            coalesce(' + fee(' || to_varchar(tier_fee, 'FM999,999.00') || ')', '') ||
                            ' = ' || to_varchar(revenue, 'FM999,999.00')
                        when pricing_type = 'VOLUME' and cumulative_amount < coalesce(price_minimum_amount, 0) then 
                            'No se aplica: acumulado < price_minimum_amount'
                        else 'No aplica'
                    end
            )
        ) as revenue_calculation_details
    from calc
)

select
    mm_id,
    amount,
    client_id,
    utc_created_at,
    transaction_month,
    matched_product_name,
    price_structure_json,
    price_minimum_amount,
    consumes_saas,
    pricing_type,
    case 
        when pricing_type = 'LINEAR' then linear_is_percentage
        when pricing_type = 'VOLUME' then tier_is_percentage
        else null
    end as is_percentage,
    case 
        when pricing_type = 'LINEAR' then linear_price_per_unit
        when pricing_type = 'VOLUME' then tier_price
        else null
    end as price_per_unit,
    tier_price as volume_price,
    tier_fee as volume_fee,
    tier_upper_bound as volume_upper_bound,
    tier_is_percentage as volume_is_percentage,
    transaction_count,
    cumulative_amount,
    regla_numero,
    revenue,
    revenue_calculation_details
from revenue_structured
