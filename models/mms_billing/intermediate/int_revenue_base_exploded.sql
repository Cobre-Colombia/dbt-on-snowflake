with data as (
    select * from {{ ref('int_mms_with_rules') }}
),

with_date as (
    select
        *,
        date_trunc('month', utc_created_at) as transaction_month
    from data
),

base_data as (
    select
        *,
        row_number() over (
            partition by matched_product_name, client_id, transaction_month
            order by utc_created_at, mm_id
        ) as transaction_count,
        sum(amount) over (
            partition by matched_product_name, client_id, transaction_month
            order by utc_created_at, mm_id
            rows between unbounded preceding and current row
        ) as cumulative_amount
    from with_date
),

exploded_base as (
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
    from base_data r
    left join lateral flatten(input => r.price_structure_json:tiers) t
)
select * from exploded_base
