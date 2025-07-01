{{ config(
    post_hook=[
        "grant select on table {{ this }} to role DATA_DEV_L1",
        "grant select on table {{ this }} to role SALES_OPS_DEV_L0"
    ]
) }}

WITH BASE_CALCULATED AS (
    SELECT SEQUENCE_CUSTOMER_ID
         , GROUP_ID
         , PRODUCT_NAME
         , PRICING_TYPE
         , TIER_APPLICATION_BASIS
         , DATE_TRUNC('MONTH', TRANSACTION_MONTH) AS TRANSACTION_MONTH
         , CURRENCY
         , MAX(ACCUMULATED_REVENUE)               AS CALCULATED_REVENUE
         , CASE
               WHEN TIER_APPLICATION_BASIS = 'amount' THEN MAX(ACCUMULATED_AMOUNT)
               ELSE MAX(TRANSACTION_COUNT) END    AS TIER_APPLICATION_VALUE
    FROM {{ ref('mart_revenue_by_group_id_by_month') }}
    GROUP BY 1, 2, 3, 4, 5, 6, 7
    )

-- 1. CTE base de clientes-grupo-producto y tipo de pricing
   , CUSTOMER_GROUPS AS (
    SELECT DISTINCT
           SEQUENCE_CUSTOMER_ID
         , GROUP_ID
         , PRICING_TYPE
         , MINIMUM_REVENUE
    FROM {{ ref('mart_revenue_by_group_id_by_month') }}
    WHERE IS_SAAS_TRANSACTION = TRUE
    )

   , DISTINCT_MONTH AS (
    SELECT DISTINCT
           DATE_TRUNC('MONTH', TRANSACTION_MONTH) AS TRANSACTION_MONTH
    FROM {{ ref('mart_revenue_by_group_id_by_month') }}
    )

   , TRX_COUNT AS (
    SELECT CG.SEQUENCE_CUSTOMER_ID
         , CG.GROUP_ID
         , 'true up charge'         AS PRODUCT_NAME
         , CG.PRICING_TYPE
         , 'amount'                 AS TIER_APPLICATION_BASIS
         , DM.TRANSACTION_MONTH
         , CG.MINIMUM_REVENUE
         , MAX(M.TRANSACTION_COUNT) AS TRANSACTION_COUNT
    FROM CUSTOMER_GROUPS CG
    CROSS JOIN DISTINCT_MONTH DM
    LEFT JOIN {{ ref('mart_revenue_by_group_id_by_month') }} M
        ON M.SEQUENCE_CUSTOMER_ID = CG.SEQUENCE_CUSTOMER_ID
        AND M.GROUP_ID = CG.GROUP_ID
        AND M.PRICING_TYPE = CG.PRICING_TYPE
        AND DATE_TRUNC('MONTH', M.TRANSACTION_MONTH) = DM.TRANSACTION_MONTH
        AND M.IS_SAAS_TRANSACTION = TRUE
    GROUP BY 1, 2, 3, 4, 5, 6, 7
    )

   , REMAINING_MINIMUM_AMOUNT AS (
    SELECT DISTINCT
           R.SEQUENCE_CUSTOMER_ID
         , R.GROUP_ID
         , 'true up charge'                         AS PRODUCT_NAME
         , NULL                                     AS PRICING_TYPE
         , 'amount'                                 AS TIER_APPLICATION_BASIS
         , DATE_TRUNC('MONTH', R.TRANSACTION_MONTH) AS TRANSACTION_MONTH
         , R.CURRENCY
         , TRX_COUNT.TRANSACTION_COUNT
         , MIN(
                   CASE
                       WHEN TRX_COUNT.TRANSACTION_COUNT > 0 THEN R.REMAINING_MINIMUM_AMOUNT
                       END
           )                                        AS CALCULATED_REVENUE
         , 1                                        AS TIER_APPLICATION_VALUE
    FROM {{ ref('mart_revenue_by_group_id_by_month') }} R
    LEFT JOIN TRX_COUNT
        ON R.SEQUENCE_CUSTOMER_ID = TRX_COUNT.SEQUENCE_CUSTOMER_ID
        AND R.TRANSACTION_MONTH::DATE = TRX_COUNT.TRANSACTION_MONTH::DATE
    WHERE TRX_COUNT.TRANSACTION_COUNT > 0
      AND R.PRODUCT_NAME NOT ILIKE '%discount%'
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
    )

   , BASE AS (
    SELECT SEQUENCE_CUSTOMER_ID
         , GROUP_ID
         , PRODUCT_NAME
         , PRICING_TYPE
         , TIER_APPLICATION_BASIS
         , TRANSACTION_MONTH
         , CURRENCY
         , CALCULATED_REVENUE
         , TIER_APPLICATION_VALUE
    FROM BASE_CALCULATED
    UNION ALL
    SELECT SEQUENCE_CUSTOMER_ID
         , GROUP_ID
         , PRODUCT_NAME
         , PRICING_TYPE
         , TIER_APPLICATION_BASIS
         , TRANSACTION_MONTH
         , CURRENCY
         , CALCULATED_REVENUE
         , TIER_APPLICATION_VALUE
    FROM REMAINING_MINIMUM_AMOUNT
    )

   , SEQUENCE_ AS (
    SELECT C.ID                                                 AS CUSTOMER_ID
         , C.LEGAL_NAME
         , C.CUSTOMER_ALIASES
         , IL.INVOICE_ID
         , IL.BILLING_SCHEDULE_ID
         , IL.PRICE_ID
         , DATE_TRUNC('MONTH', TO_DATE(I.BILLING_PERIOD_START)) AS BILLING_PERIOD_START
         , LAST_DAY(TO_DATE(I.BILLING_PERIOD_END))              AS BILLING_PERIOD_END
         , CASE
               WHEN LOWER(IL.TITLE) = 'new discount' THEN 'discount'
               ELSE LOWER(IL.TITLE)
        END                                                     AS TITLE
         , CASE
               WHEN LOWER(P.PRODUCT_NAME) = 'new discount' THEN 'discount'
               ELSE LOWER(P.PRODUCT_NAME)
        END                                                     AS PRODUCT_NAME
         , ROUND(IL.NET_TOTAL)                                  AS NET_TOTAL
         , IL.CURRENCY
         , ROUND(SC.PRICE_MINIMUM_AMOUNT)                       AS PRICE_MINIMUM_AMOUNT
         , IL.QUANTITY                                          AS TIER_APPLICATION_VALUE
    FROM COBRE_GOLD_DB.SEQUENCE.INVOICES I
    LEFT JOIN COBRE_GOLD_DB.SEQUENCE.INVOICE_LINE_ITEMS IL
        ON IL.INVOICE_ID = I.ID
    LEFT JOIN COBRE_GOLD_DB.SEQUENCE.CUSTOMERS C
        ON I.CUSTOMER_ID = C.ID
    LEFT JOIN COBRE_GOLD_DB.SEQUENCE.PRICES P
        ON IL.PRICE_ID = P.ID
    LEFT JOIN COBRE_GOLD_DB.SEQUENCE.BILLING_SCHEDULE_PHASE_PRICES SC
        ON IL.PRICE_ID = SC.PRICE_ID
        AND IL.BILLING_SCHEDULE_ID = SC.BILLING_SCHEDULE_ID
        AND I.BILLING_PERIOD_START >= SC.PHASE_START_DATE
        AND I.BILLING_PERIOD_START <= COALESCE(SC.PHASE_END_DATE, '2099-01-01')
        AND (SC.STATUS IN ('ACTIVE', 'COMPLETED') OR SC.STATUS IS NULL)
    WHERE 1 = 1
      AND SC.PHASE_ARCHIVED_AT IS NULL
      AND IL.DELETED_AT IS NULL
      AND I.STATUS <> 'VOIDED'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY C.ID, I.BILLING_PERIOD_START, TITLE, PRODUCT_NAME ORDER BY I.CALCULATED_AT DESC
        ) = 1
    )

   , SEQUENCE_PRODUCT AS (
    SELECT LEGAL_NAME
         , CUSTOMER_ID
         , IFF(TITLE ILIKE '%discount%', 'discount', COALESCE(PRODUCT_NAME, TITLE)) AS PRODUCT_NAME
         , DATE_TRUNC('MONTH', BILLING_PERIOD_START)                                AS TRANSACTION_MONTH
         , SUM(NET_TOTAL)                                                           AS NET_TOTAL
         , SUM(TIER_APPLICATION_VALUE)                                              AS TIER_APPLICATION_VALUE
    FROM SEQUENCE_
    GROUP BY 1, 2, 3, 4
    )

SELECT S.LEGAL_NAME                                                         AS GROUP_NAME
     , B.GROUP_ID
     , B.SEQUENCE_CUSTOMER_ID
     , B.PRODUCT_NAME                                                       AS CALCULATED_PRODUCT_NAME
     , S.PRODUCT_NAME                                                       AS SEQUENCE_PRODUCT_NAME
     , S.TRANSACTION_MONTH                                                  AS BILLING_PERIOD
     , B.CURRENCY
     , B.PRICING_TYPE
     , CASE
           WHEN B.TIER_APPLICATION_BASIS IS NULL
               THEN B.PRICING_TYPE
           ELSE UPPER(B.TIER_APPLICATION_BASIS)
    END                                                                     AS TIER_APPLICATION_BASIS
     , ROUND(B.TIER_APPLICATION_VALUE, 0)                                   AS CALCULATED_TIER_APPLICATION_VALUE
     , ROUND(S.TIER_APPLICATION_VALUE, 0)                                   AS SEQUENCE_TIER_APPLICATION_VALUE
     , ROUND(COALESCE(CALCULATED_REVENUE, 0), 0)                            AS CALCULATED_REVENUE_
     , ROUND(COALESCE(S.NET_TOTAL, 0), 0)                                   AS SEQUENCE_REVENUE_
     , ROUND(COALESCE(CALCULATED_REVENUE, 0) - COALESCE(S.NET_TOTAL, 0), 0) AS _DIFFERENCE
     , COALESCE(
        ROUND(
                CASE
                    WHEN CALCULATED_REVENUE_ = 0 AND SEQUENCE_REVENUE_ = 0 THEN 0
                    ELSE (CALCULATED_REVENUE_::FLOAT - SEQUENCE_REVENUE_::FLOAT)
                             / NULLIF((CALCULATED_REVENUE_::FLOAT + SEQUENCE_REVENUE_::FLOAT) / 2, 0)
                        * 100
                    END,
                2),
        0)                                                                  AS RELATIVE_PERCENTAGE_DIFFERENCE


FROM BASE B
FULL OUTER JOIN SEQUENCE_PRODUCT S
    ON B.SEQUENCE_CUSTOMER_ID = S.CUSTOMER_ID
    AND
       CASE WHEN B.PRODUCT_NAME LIKE '%Discount%' THEN 'discount' ELSE LOWER(B.PRODUCT_NAME) END =
       LOWER(S.PRODUCT_NAME)
    AND B.TRANSACTION_MONTH = S.TRANSACTION_MONTH
-- WHERE
--     (B.GROUP_ID IS NOT NULL AND ( GROUP_NAME IS NULL AND CALCULATED_PRODUCT_NAME <> 'true up charge'))
--     OR S.CUSTOMER_ID IN (
--         SELECT DISTINCT SEQUENCE_CUSTOMER_ID
--         FROM BASE
--     )
ORDER BY GROUP_NAME ASC, BILLING_PERIOD ASC, CALCULATED_PRODUCT_NAME ASC, _DIFFERENCE