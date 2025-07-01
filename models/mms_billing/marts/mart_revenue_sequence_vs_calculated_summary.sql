{{ config(materialized='table') }}

select s.legal_name                                                         as group_name
     , b.group_id
     , b.sequence_customer_id
     , b.product_name                                                       as calculated_product_name
     , s.product_name                                                       as sequence_product_name
     , s.transaction_month                                                  as billing_period
     , b.currency
     , b.pricing_type
     , case
           when b.tier_application_basis is null
               then b.pricing_type
           else upper(b.tier_application_basis)
    end                                                                     as tier_application_basis
     , round(b.tier_application_value, 0)                                   as calculated_tier_application_value
     , round(s.tier_application_value, 0)                                   as sequence_tier_application_value
     , round(coalesce(calculated_revenue, 0), 0)                            as calculated_revenue_
     , round(coalesce(s.net_total, 0), 0)                                   as sequence_revenue_
     , round(coalesce(calculated_revenue, 0) - coalesce(s.net_total, 0), 0) as _difference
     , coalesce(
        round(
                case
                    when calculated_revenue_ = 0 and sequence_revenue_ = 0 then 0
                    else (calculated_revenue_::float - sequence_revenue_::float)
                             / nullif((calculated_revenue_::float + sequence_revenue_::float) / 2, 0)
                        * 100
                    end,
                2),
        0)                                                                  as relative_percentage_difference

from {{ ref('mart_revenue_calculated_summary') }} b
full outer join {{ ref('mart_revenue_sequence_summary') }} s
    on b.sequence_customer_id = s.customer_id
    and
       case when b.product_name like '%Discount%' then 'discount' else lower(b.product_name) end =
       lower(s.product_name)
    and b.transaction_month = s.transaction_month
order by group_name asc, billing_period asc, calculated_product_name asc, _difference