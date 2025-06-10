WITH source AS (
    SELECT 
        I.ID,
        C.CUSTOMER_ALIASES
    FROM {{ source('sequence', 'INVOICES') }} I
    LEFT JOIN {{ source('sequence', 'CUSTOMERS') }} C
        ON I.CUSTOMER_ID = C.ID
    WHERE C.CUSTOMER_ALIASES = 'cli_bdgczxcwd0' -- dac bancolombia
)

SELECT * FROM source 