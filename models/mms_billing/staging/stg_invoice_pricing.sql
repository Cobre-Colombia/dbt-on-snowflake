with invoice as (
    select
        c.customer_aliases,
        i.id
    from {{ source('SEQUENCE', 'INVOICES') }} i
    left join {{ source('SEQUENCE', 'CUSTOMERS') }} c
        on i.customer_id = c.id
    where 1 = 1
        and (
            c.customer_aliases = '{{ var('client_id') }}'
            or c.customer_aliases like '{{ var('client_id') }},%'
            or c.customer_aliases like '%,{{ var('client_id') }}'
            or c.customer_aliases like '%,{{ var('client_id') }},%'
        )
        and billing_period_start :: date = '{{ var('billing_period_start') }}'
        and billing_period_end   :: date = '{{ var('billing_period_end') }}'
),

union_all as (
    select
        ili.billing_period_start :: date as month,
        ili.invoice_id,
        ili.title,
        p.product_name,
        um.name as usage_metric,
        bspp.price_minimum_amount,
        iff(bspp.price_minimum_amount is not null, true, false) as consumes_saas,
        ili.quantity,
        ili.rate,
        um.property_filters,
        bspp.price_structure,
        ili.currency,
        ili.net_total,
        ili.gross_total,
        ili.calculated_at
    from {{ source('SEQUENCE', 'INVOICE_LINE_ITEMS') }} ili
    left join {{ source('SEQUENCE', 'PRICES') }} p
        on ili.price_id = p.id
    left join {{ source('SEQUENCE', 'BILLING_SCHEDULE_PHASE_PRICES') }} bspp
        on p.id = bspp.price_id
        and bspp.status = 'ACTIVE'
        and bspp.phase_archived_at is null
    left join {{ source('SEQUENCE', 'USAGE_METRICS') }} um
        on um.id = bspp.price_structure['usageMetricId'] :: string
    inner join invoice i
        on ili.invoice_id = i.id
    qualify
        dense_rank() over (
            partition by ili.invoice_id
            order by ili.calculated_at desc
        ) = 1
),

ranked as (
    select
        ua.*,
        dense_rank() over (
            order by ua.calculated_at desc
        ) as date_numing
    from union_all ua
)

select
    distinct
    *,
    {{ parse_json_column('PROPERTY_FILTERS') }} as property_filters_json,
    {{ parse_json_column('PRICE_STRUCTURE') }} as price_structure_json
from ranked
