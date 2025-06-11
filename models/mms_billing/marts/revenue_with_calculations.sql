with data as (
    select * from {{ ref('enriched_mms_with_rules') }}
),

match_counts as (
    select
        matched_product_name,
        count(*) as transaction_count
    from data
    group by matched_product_name
),

exploded as (
    select
        d.mm_id,
        d.amount,
        d.matched_product_name,
        d.price_structure_json,
        d.price_minimum_amount,
        d.consumes_saas,
        mc.transaction_count,
        upper(d.price_structure_json:pricingType::string) as pricing_type,
        d.price_structure_json:isPricePercentage::boolean as is_percentage,
        d.price_structure_json:pricePerUnit::float as price_per_unit,
        t.value:price::float as tier_price,
        t.value:fee::float as tier_fee,
        t.value:upperBound::int as tier_upper_bound,
        t.value:isPricePercentage::boolean as tier_is_percentage,
        row_number() over (
            partition by d.mm_id, d.matched_product_name
            order by
                case
                    when t.value:upperBound is null then 2
                    when t.value:upperBound::int >= mc.transaction_count then 1
                    else 3
                end,
                t.value:upperBound::int
        ) as tier_rank
    from data d
    left join match_counts mc
        on d.matched_product_name = mc.matched_product_name
    left join lateral flatten(input => d.price_structure_json:tiers) t
),

calc as (
    select
        *,
        row_number() over (partition by mm_id order by matched_product_name) as regla_numero,

        case 
            when pricing_type = 'LINEAR' and is_percentage = true then amount * (price_per_unit / 100)
            when pricing_type = 'LINEAR' and is_percentage = false then price_per_unit
            when pricing_type = 'VOLUME' and tier_is_percentage = true then (amount * (tier_price / 100)) + coalesce(tier_fee, 0)
            when pricing_type = 'VOLUME' and tier_is_percentage = false then tier_price + coalesce(tier_fee, 0)
            else 0
        end as revenue
    from exploded
    where pricing_type != 'VOLUME' or tier_rank = 1
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
                    case when pricing_type = 'LINEAR' then is_percentage
                         when pricing_type = 'VOLUME' then tier_is_percentage
                         else null end,
                'price', 
                    to_number(coalesce(tier_price, price_per_unit), 10, 2),
                'fee', 
                    to_number(coalesce(tier_fee, 0), 10, 2),
                'transaction_amount', to_number(amount, 18, 2),
                'transaction_count', transaction_count,
                'revenue', to_number(revenue, 18, 2),
                'calculation_method',
                    case
                        when pricing_type = 'LINEAR' and is_percentage = true then 'Porcentaje'
                        when pricing_type = 'LINEAR' and is_percentage = false then 'Carga fija'
                        when pricing_type = 'VOLUME' and tier_is_percentage = true then 'Volumen - Porcentaje'
                        when pricing_type = 'VOLUME' and tier_is_percentage = false then 'Volumen - Carga fija'
                        else 'No aplica'
                    end,
                'calculation_formula',
                    case
                        when pricing_type = 'LINEAR' and is_percentage = true then 
                            to_varchar(amount, 'FM999,999.00') || ' × ' || to_varchar(price_per_unit, 'FM999,999.00') || '% = ' || to_varchar(revenue, 'FM999,999.00')
                        when pricing_type = 'LINEAR' and is_percentage = false then 
                            to_varchar(price_per_unit, 'FM999,999.00')
                        when pricing_type = 'VOLUME' and tier_is_percentage = true then 
                            to_varchar(amount, 'FM999,999.00') || ' × ' || to_varchar(tier_price, 'FM999,999.00') || '%'
                            || coalesce(' + fee(' || to_varchar(tier_fee, 'FM999,999.00') || ')', '')
                            || ' = ' || to_varchar(revenue, 'FM999,999.00')
                        when pricing_type = 'VOLUME' and tier_is_percentage = false then 
                            to_varchar(tier_price, 'FM999,999.00') 
                            || coalesce(' + fee(' || to_varchar(tier_fee, 'FM999,999.00') || ')', '')
                            || ' = ' || to_varchar(revenue, 'FM999,999.00')
                        else 'No aplica'
                    end
            )
        ) as revenue_calculation_details
    from calc
)

select
    mm_id,
    amount,
    matched_product_name,
    price_structure_json,
    price_minimum_amount,
    consumes_saas,
    pricing_type,
    is_percentage,
    price_per_unit,
    tier_price as volume_price,
    tier_fee as volume_fee,
    tier_upper_bound as volume_upper_bound,
    tier_is_percentage as volume_is_percentage,
    transaction_count,
    regla_numero,
    revenue,
    revenue_calculation_details
from revenue_structured
