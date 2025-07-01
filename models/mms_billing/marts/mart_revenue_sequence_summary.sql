with  sequence_base as (
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

select legal_name
    , customer_id
    , iff(title ilike '%discount%', 'discount', coalesce(product_name, title)) as product_name
    , date_trunc('MONTH', billing_period_start)                                as transaction_month
    , sum(net_total)                                                           as net_total
    , sum(tier_application_value)                                              as tier_application_value
from sequence_base
group by 1, 2, 3, 4

