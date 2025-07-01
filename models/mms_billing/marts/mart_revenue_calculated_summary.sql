{{ config(materialized='table') }}

with base_calculated as (
    select sequence_customer_id
         , group_id
         , matched_product_name as product_name
         , pricing_type
         , tier_application_basis
         , date_trunc('MONTH', transaction_month) as transaction_month
         , currency
         , max(cumulative_revenue)               as calculated_revenue
         , case
               when tier_application_basis = 'amount' then max(accumulated_revenue)
               else max(transaction_count) end    as tier_application_value
    from {{ ref('mart_revenue_calculated') }}
    group by 1, 2, 3, 4, 5, 6, 7
    )
   , customer_groups as (
    select distinct
           sequence_customer_id
         , group_id
         , pricing_type
         , price_minimum_revenue as minimum_revenue
    from {{ ref('mart_revenue_calculated') }}
    where consumes_saas = true
    )

   , distinct_month as (
    select distinct
           date_trunc('MONTH', transaction_month) as transaction_month
    from {{ ref('mart_revenue_calculated') }}
    )

   , trx_count as (
    select cg.sequence_customer_id
         , cg.group_id
         , 'true up charge'         as product_name
         , cg.pricing_type
         , 'amount'                 as tier_application_basis
         , dm.transaction_month
         , cg.minimum_revenue
         , max(m.transaction_count) as transaction_count
    from customer_groups cg
    cross join distinct_month dm
    left join {{ ref('mart_revenue_calculated') }} m
        on m.sequence_customer_id = cg.sequence_customer_id
        and m.group_id = cg.group_id
        and m.pricing_type = cg.pricing_type
        and date_trunc('MONTH', m.transaction_month) = dm.transaction_month
        and m.consumes_saas = true
    group by 1, 2, 3, 4, 5, 6, 7
    )

   , remaining_minimum_amount as (
    select distinct
           r.sequence_customer_id
         , r.group_id
         , 'true up charge'                         as product_name
         , null                                     as pricing_type
         , 'amount'                                 as tier_application_basis
         , date_trunc('MONTH', r.transaction_month) as transaction_month
         , r.currency
         , trx_count.transaction_count
         , min(
                   case
                       when trx_count.transaction_count > 0 then r.remaining_minimum
                       end
           )                                        as calculated_revenue
         , 1                                        as tier_application_value
    from {{ ref('mart_revenue_calculated') }} r
    left join trx_count
        on r.sequence_customer_id = trx_count.sequence_customer_id
        and r.transaction_month::date = trx_count.transaction_month::date
    where trx_count.transaction_count > 0
      and r.matched_product_name not ilike '%discount%'
    group by 1, 2, 3, 4, 5, 6, 7, 8
    )

select sequence_customer_id
        , group_id
        , product_name
        , pricing_type
        , tier_application_basis
        , transaction_month
        , currency
        , calculated_revenue
        , tier_application_value
from base_calculated
union all
select sequence_customer_id
        , group_id
        , product_name
        , pricing_type
        , tier_application_basis
        , transaction_month
        , currency
        , calculated_revenue
        , tier_application_value
from remaining_minimum_amount
