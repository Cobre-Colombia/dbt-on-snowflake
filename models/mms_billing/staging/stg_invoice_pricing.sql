{% set end_cutoff_date = modules.datetime.datetime.now().date() - modules.datetime.timedelta(days=0) %}
{% set start_cutoff_date = (end_cutoff_date.replace(day=1) - modules.datetime.timedelta(days=90)).replace(day=1) %}

WITH invoice_pricing_by_month AS (
    SELECT *
    FROM (
        SELECT I.CUSTOMER_ID                                           AS SEQUENCE_CUSTOMER_ID
             , C.CUSTOMER_ALIASES                                      AS RAW_CLIENT_ID
             , C.LEGAL_NAME                                            AS SEQUENCE_CLIENT_NAME
             , ILI.ID                                                  AS INVOICE_LINE_ITEM_ID
             , P.ID                                                    AS PRICE_ID
             , SO.GROUP_ID                                              AS GROUP_ID
             , BSPP.STATUS
             , BSPP.PRICE_MINIMUM_AMOUNT
             , IFF(BSPP.PRICE_MINIMUM_AMOUNT IS NOT NULL, TRUE, FALSE) AS CONSUMES_SAAS
             , BSPP.PRICE_STRUCTURE
             , UM.NAME                                                 AS USAGE_METRIC
             , UM.PROPERTY_FILTERS
             , UM.PROPERTIES_TO_NEGATE
             , DATE_TRUNC(MONTH, I.BILLING_PERIOD_START)               AS BILLING_MONTH
             , ILI.MODIFIED_AT                                         AS MODIFIED_AT
             , IFF(ILI.TITLE = 'Discount', TRUE, FALSE)                AS IS_DISCOUNT
             , IFF(IS_DISCOUNT, ILI.NET_TOTAL, 0)                      AS DISCOUNT_AMOUNT
             , COALESCE(P.PRODUCT_NAME, ILI.TITLE)                     AS PRODUCT_NAME
             , ILI.TITLE
             , ILI.CURRENCY
             , ILI.NET_TOTAL
             , ILI.GROSS_TOTAL
             , ILI.CALCULATED_AT
             , ILI.BILLING_PERIOD_START:: DATE as month
             , DATE_TRUNC('month', i.BILLING_PERIOD_START)           AS PRICE_STRUCTURE_MONTH
             , ROW_NUMBER()
                OVER (
                    PARTITION BY I.CUSTOMER_ID, I.BILLING_PERIOD_START, IS_DISCOUNT, COALESCE(P.PRODUCT_NAME, ILI.TITLE) 
                    ORDER BY I.CALCULATED_AT DESC, ILI.MODIFIED_AT DESC
                    )                                                  AS RN
        FROM {{ source('SEQUENCE', 'INVOICES') }} I
        LEFT JOIN {{ source('SEQUENCE', 'INVOICE_LINE_ITEMS') }} ILI
            ON I.ID = ILI.INVOICE_ID
        LEFT JOIN {{ source('SEQUENCE', 'PRICES') }} P
            ON ILI.PRICE_ID = P.ID
        LEFT JOIN {{ source('SEQUENCE', 'BILLING_SCHEDULE_PHASE_PRICES') }} BSPP
            ON P.ID = BSPP.PRICE_ID 
            AND (BSPP.STATUS IN ('ACTIVE', 'COMPLETED') OR BSPP.STATUS IS NULL)
            AND I.BILLING_PERIOD_START >= BSPP.PHASE_START_DATE
            AND I.BILLING_PERIOD_START <= COALESCE(BSPP.PHASE_END_DATE, '2099-01-01')
        LEFT JOIN {{ source('SEQUENCE', 'USAGE_METRICS') }} UM
            ON UM.ID = BSPP.PRICE_STRUCTURE['usageMetricId']::STRING
        LEFT JOIN {{ source('SEQUENCE', 'CUSTOMERS') }} C
            ON I.CUSTOMER_ID = C.ID
        LEFT JOIN {{ source('SALES_OPS', 'CUSTOMERS') }} SO
            ON C.ID = SO.SEQUENCE_ID
        WHERE 1 = 1
          AND ILI.DELETED_AT IS NULL
          AND BSPP.PHASE_ARCHIVED_AT IS NULL
          AND I.BILLING_PERIOD_START :: DATE between '{{ start_cutoff_date }}' and '{{ end_cutoff_date }}'
        ) LATEST
    WHERE 1 = 1
      AND RN = 1
    )
,
exploded as (
    select
        value::string as client_id,
        r.*
    from invoice_pricing_by_month r,
    lateral flatten(input => split(r.raw_client_id, ','))
)
select
    distinct
    *,
    {{ parse_json_column('PROPERTY_FILTERS') }} as property_filters_json,
    {{ parse_json_column('PRICE_STRUCTURE') }} as price_structure_json
from exploded