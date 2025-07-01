with base_calculated as (
    select sequence_customer_id
         , group_id
         , product_name
         , pricing_type
         , tier_application_basis
         , date_trunc('MONTH', transaction_month) as transaction_month
         , currency
         , max(accumulated_revenue)               as calculated_revenue
         , case
               when tier_application_basis = 'amount' then max(accumulated_amount)
               else max(transaction_count) end    as tier_application_value
    from {{ ref('mart_aggregated_revenue') }}
    group by 1, 2, 3, 4, 5, 6, 7
    )
   , customer_groups as (
    select distinct
           sequence_customer_id
         , group_id
         , pricing_type
         , minimum_revenue
    from {{ ref('mart_aggregated_revenue') }}
    where is_saas_transaction = true
    )

   , distinct_month as (
    select distinct
           date_trunc('MONTH', transaction_month) as transaction_month
    from {{ ref('mart_aggregated_revenue') }}
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
    left join {{ ref('mart_aggregated_revenue') }} m
        on m.sequence_customer_id = cg.sequence_customer_id
        and m.group_id = cg.group_id
        and m.pricing_type = cg.pricing_type
        and date_trunc('MONTH', m.transaction_month) = dm.transaction_month
        and m.is_saas_transaction = true
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
                       when trx_count.transaction_count > 0 then r.remaining_minimum_amount
                       end
           )                                        as calculated_revenue
         , 1                                        as tier_application_value
    from {{ ref('mart_aggregated_revenue') }} r
    left join trx_count
        on r.sequence_customer_id = trx_count.sequence_customer_id
        and r.transaction_month::date = trx_count.transaction_month::date
    where trx_count.transaction_count > 0
      and r.product_name not ilike '%discount%'
    group by 1, 2, 3, 4, 5, 6, 7, 8
    )

   , base as (
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
    )

   , sequence_ as (
    select c.id                                                 as customer_id
         , c.legal_name
         , c.customer_aliases
         , il.invoice_id
         , il.billing_schedule_id
         , il.price_id
         , date_trunc('MONTH', to_date(i.billing_period_start)) as billing_period_start
         , last_day(to_date(i.billing_period_end))              as billing_period_end
         , case
               when lower(il.title) = 'new discount' then 'discount'
               else lower(il.title)
        end                                                     as title
         , case
               when lower(p.product_name) = 'new discount' then 'discount'
               else lower(p.product_name)
        end                                                     as product_name
         , round(il.net_total)                                  as net_total
         , il.currency
         , round(sc.price_minimum_amount)                       as price_minimum_amount
         , il.quantity                                          as tier_application_value
    from {{ source('SEQUENCE', 'INVOICES') }} i
    left join {{ source('SEQUENCE', 'INVOICE_LINE_ITEMS') }} il
        on il.invoice_id = i.id
    left join {{ source('SEQUENCE', 'CUSTOMERS') }} c
        on i.customer_id = c.id
    left join {{ source('SEQUENCE', 'PRICES') }} p
        on il.price_id = p.id
    left join {{ source('SEQUENCE', 'BILLING_SCHEDULE_PHASE_PRICES') }} sc
        on il.price_id = sc.price_id
        and il.billing_schedule_id = sc.billing_schedule_id
        and i.billing_period_start >= sc.phase_start_date
        and i.billing_period_start <= coalesce(sc.phase_end_date, '2099-01-01')
        and (sc.status in ('ACTIVE', 'COMPLETED') or sc.status is null)
    where 1 = 1
      and sc.phase_archived_at is null
      and il.deleted_at is null
      and i.status <> 'VOIDED'
    qualify row_number() over (
        partition by c.id, i.billing_period_start, title, product_name order by i.calculated_at desc
        ) = 1
    )

   , sequence_product as (
    select legal_name
         , customer_id
         , iff(title ilike '%discount%', 'discount', coalesce(product_name, title)) as product_name
         , date_trunc('MONTH', billing_period_start)                                as transaction_month
         , sum(net_total)                                                           as net_total
         , sum(tier_application_value)                                              as tier_application_value
    from sequence_
    group by 1, 2, 3, 4
    )

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

from base b
full outer join sequence_product s
    on b.sequence_customer_id = s.customer_id
    and
       case when b.product_name like '%Discount%' then 'discount' else lower(b.product_name) end =
       lower(s.product_name)
    and b.transaction_month = s.transaction_month
order by group_name asc, billing_period asc, calculated_product_name asc, _difference