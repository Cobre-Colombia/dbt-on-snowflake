with data as (
    select * from {{ ref('enriched_mms_with_rules') }}
),
aggregated as (
    select
        *,
        {{ calculate_revenue(
            'price_structure_json',
            'amount',
            'product_name',
            1
        ) }} as revenue
    from data
)
select * from aggregated
