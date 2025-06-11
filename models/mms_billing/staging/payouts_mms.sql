SELECT AMOUNT,ID                                                               AS MM_ID
     , CLIENT_ID
     , 'COBRE BILLING'                                                  AS EVENTTYPE
     , DATE_TRUNC(
        'DAY',
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
       )                                                                AS EVENTTIMESTAMP
     , 'PAYOUT'                                                         AS FLOW
     , UPPER(TYPE)                                                      AS TRANSACTION_TYPE
     , CASE
           WHEN COBRE_ORIGIN_CHANNEL = 'api' THEN 'API'
           WHEN COBRE_ORIGIN_CHANNEL NOT IN ('api') THEN 'PORTAL'
           ELSE 'OTHER'
    END                                                                 AS ORIGINATION_SYSTEM
     , CASE
           WHEN LOWER(SOURCE_ACCOUNT_PROVIDER_ID) LIKE '%cobre%' THEN 'COBRE BALANCE'
           ELSE 'CONNECT'
    END                                                                 AS SOURCE_ACCOUNT_TYPE
     , CASE
           WHEN TYPE = 'payout' THEN
               CASE
                   WHEN DESTINATION_ACCOUNT_INSTITUTION_CODE IS NULL OR SOURCE_ACCOUNT_INSTITUTION_CODE IS NULL
                       THEN 'UNKNOWN'
                   WHEN DESTINATION_ACCOUNT_INSTITUTION_CODE = SOURCE_ACCOUNT_INSTITUTION_CODE THEN 'FAST_PAY'
                   WHEN SOURCE_ACCOUNT_INSTITUTION_CODE = '1023' AND
                        DESTINATION_ACCOUNT_INSTITUTION_CODE IN ('1001', '1002', '1052') THEN 'FAST_PAY'
                   WHEN SOURCE_ACCOUNT_INSTITUTION_CODE = '1007' AND DESTINATION_ACCOUNT_INSTITUTION_CODE = '1507'
                       THEN 'FAST_PAY'
                   WHEN SOURCE_ACCOUNT_INSTITUTION_CODE = '1051' AND DESTINATION_ACCOUNT_INSTITUTION_CODE = '1551'
                       THEN 'FAST_PAY'
                   ELSE 'ACH'
                   END
           WHEN TYPE IN ('spei', 'spei_card') THEN 'SPEI'
           WHEN TYPE = 'fast_pay' THEN 'FAST_PAY'
           WHEN TYPE = 'ach' THEN 'ACH'
           ELSE 'UNKNOWN'
    END                                                                 AS TRANSFER_METHOD
     , COALESCE(UPPER(DESTINATION_ACCOUNT_INSTITUTION_NAME), 'UNKNOWN') AS DESTINATION_BANK
     , COALESCE(
        CASE
            WHEN TYPE = 'payout' THEN UPPER(SOURCE_ACCOUNT_INSTITUTION_NAME)
            WHEN TYPE IN ('fast_pay', 'ach') THEN COALESCE(
                    CASE SOURCE_UNDERLAYING_COBRE_INSTITUTION
                        WHEN '1051' THEN 'BANCO DAVIVIENDA SA'
                        WHEN '1066' THEN 'BANCO COOPERATIVO COOPCENTRAL'
                        WHEN '1023' THEN 'BANCO DE OCCIDENTE'
                        WHEN '1001' THEN 'BANCO DE BOGOTA'
                        WHEN '9999' THEN 'COBRE'
                        WHEN '1019' THEN 'SCOTIABANK COLPATRIA S.A'
                        WHEN '1013' THEN 'BBVA COLOMBIA'
                        WHEN '1507' THEN 'NEQUI'
                        WHEN '1007' THEN 'BANCOLOMBIA'
                        ELSE 'UNKNOWN'
                        END,
                    CASE RIGHT(SUPPLIER, 4)
                        WHEN '1051' THEN 'BANCO DAVIVIENDA SA'
                        WHEN '1066' THEN 'BANCO COOPERATIVO COOPCENTRAL'
                        WHEN '1023' THEN 'BANCO DE OCCIDENTE'
                        WHEN '1001' THEN 'BANCO DE BOGOTA'
                        WHEN '9999' THEN 'COBRE'
                        WHEN '1019' THEN 'SCOTIABANK COLPATRIA S.A'
                        WHEN '1013' THEN 'BBVA COLOMBIA'
                        WHEN '1507' THEN 'NEQUI'
                        WHEN '1007' THEN 'BANCOLOMBIA'
                        ELSE 'COBRE'
                        END,
                    'UNKNOWN'
                                                  )
            WHEN TYPE IN ('spei', 'spei_card') THEN UPPER(SOURCE_ACCOUNT_INSTITUTION_NAME)
            ELSE 'UNKNOWN'
            END, 'UNKNOWN')                                             AS ORIGIN_BANK
     , CASE
           WHEN SOURCE_GEOGRAPHY = 'col' THEN 'COLOMBIA'
           WHEN SOURCE_GEOGRAPHY = 'mex' THEN 'MEXICO'
           ELSE 'OTHER'
    END                                                                 AS COUNTRY
FROM {{ source('DATAWAREHOUSE', 'MONEY_MOVEMENTS') }}
WHERE
    CLIENT_ID NOT IN ('cli_pxt_product', 'cli_coe542', 'cli_siikld3ljw', 'PIC00', 'DEM49', 'cli_u2zvkkh50b',
                      'cli_rx0g0ajunr', 'cli_phdanw1wsc', 'cli_vdke2laqnj', 'cli_zkggvlvqf0', 'PXT01', 'PPF594',
                      'SCI528', 'R1.PRL637', 'R1C1E478', 'R1.BBC634', 'PLO429', 'R1.BIL620', 'POG521',
                      'cli_pxzh3oo8cy', 'R1.C1E478', 'cli_8gv2wyvywv', 'CNE614', 'cli_xxz85j60is', 'COS394',
                      'R1.CET436', 'cli_inf001', 'CBC561', 'CNR563', 'CEF564', 'CTS565', 'cli_geyjth0opt', 'CTC602',
                      'cli_1wkwadayff', 'r1.PEL603', 'cli_f3atilxc7k', 'cli_9xyiscji0k', 'cli_l5w6myjnek')
  AND TYPE IN ('payout', 'ach', 'fast_pay', 'spei', 'spei_card')
  AND STATUS_STATE IN ('finished', 'completed')
  AND DATE_TRUNC('MONTH',
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
      ) = '{{ var('billing_period_start') }}'
  AND CLIENT_ID = '{{ var('client_id') }}'