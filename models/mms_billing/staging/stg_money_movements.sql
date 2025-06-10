WITH source AS (
    SELECT
        ID AS MM_ID,
        CLIENT_ID,
        DATE_TRUNC('DAY',
            TO_DATE(
                CONVERT_TIMEZONE(
                    'UTC',
                    CASE
                        WHEN SOURCE_GEOGRAPHY = 'col' THEN 'America/Bogota'
                        WHEN SOURCE_GEOGRAPHY = 'mex' THEN 'America/Mexico_City'
                        ELSE 'UTC'
                    END,
                    TO_TIMESTAMP_NTZ(CREATED_AT)
                )
            )
        ) AS EVENTTIMESTAMP,
        UPPER(TYPE) AS TRANSACTION_TYPE,
        CASE
            WHEN TYPE = 'payout' THEN
                CASE
                    WHEN DESTINATION_ACCOUNT_INSTITUTION_CODE IS NULL OR SOURCE_ACCOUNT_INSTITUTION_CODE IS NULL
                        THEN 'UNKNOWN'
                    WHEN DESTINATION_ACCOUNT_INSTITUTION_CODE = SOURCE_ACCOUNT_INSTITUTION_CODE THEN 'FAST_PAY'
                    WHEN SOURCE_ACCOUNT_INSTITUTION_CODE = '1023' AND
                        DESTINATION_ACCOUNT_INSTITUTION_CODE IN ('1001', '1002', '1052') THEN 'FAST_PAY'
                    WHEN SOURCE_ACCOUNT_INSTITUTION_CODE = '1007' AND DESTINATION_ACCOUNT_INSTITUTION_CODE = '1507'
                        THEN 'FAST_PAY' -- NEQUI
                    WHEN SOURCE_ACCOUNT_INSTITUTION_CODE = '1051' AND DESTINATION_ACCOUNT_INSTITUTION_CODE = '1551'
                        THEN 'FAST_PAY' -- DAVIPLATA
                    ELSE 'ACH'
                END
            WHEN TYPE IN ('spei', 'spei_card') THEN 'SPEI'
            WHEN TYPE = 'fast_pay' THEN 'FAST_PAY'
            WHEN TYPE = 'ach' THEN 'ACH'
            ELSE 'UNKNOWN'
        END AS TRANSFER_METHOD
    FROM {{ source('datawarehouse', 'MONEY_MOVEMENTS') }}
    WHERE CLIENT_ID = 'cli_bdgczxcwd0' -- dac bancolombia
        AND TYPE IN ('payout', 'ach', 'fast_pay', 'spei', 'spei_card')
        AND STATUS_STATE IN ('finished', 'completed')
)

SELECT * FROM source 