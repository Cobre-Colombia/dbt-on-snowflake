WITH cash_out_config AS (
    SELECT
        INVOICE_ID,
        PROPERTY_FILTERS,
        USAGE_METRIC
    FROM {{ ref('int_invoice_pricing') }}
    WHERE USAGE_METRIC = 'Any Cash-out Transactions'
    QUALIFY ROW_NUMBER() OVER (ORDER BY CALCULATED_AT DESC) = 1
),

transactions_with_rules AS (
    SELECT
        mm.*,
        cc.INVOICE_ID,
        cc.PROPERTY_FILTERS,
        CASE
            WHEN mm.TRANSACTION_TYPE = ANY(cc.PROPERTY_FILTERS['transaction_type'])
            AND mm.TRANSFER_METHOD = ANY(cc.PROPERTY_FILTERS['transfer_method'])
            THEN TRUE
            ELSE FALSE
        END AS matches_pricing_rule
    FROM {{ ref('stg_money_movements') }} mm
    CROSS JOIN cash_out_config cc
)

SELECT
    MM_ID,
    INVOICE_ID,
    TRANSACTION_TYPE,
    TRANSFER_METHOD,
    matches_pricing_rule,
    CASE 
        WHEN matches_pricing_rule THEN 'Any Cash-out Transactions'
        ELSE NULL 
    END as pricing_rule_name,
    EVENTTIMESTAMP,
    CLIENT_ID
FROM transactions_with_rules 