{{ config(
    materialized='incremental',
    unique_key=['MM_ID', 'SEQUENCE_CUSTOMER_ID', 'MATCHED_PRODUCT_NAME', 'TRANSACTION_MONTH', 'AMOUNT'],
    incremental_strategy='merge'
) }}

with all_rules as (
    select *
    from {{ ref('int_rules_mms') }}

    union all

    select *
    from {{ ref('int_rules_platform_fee_always_charged') }}

    union all

    select tuc.*
    from {{ ref('int_rules_true_up_charge') }} tuc
    left join {{ ref('int_rules_mms_count') }} t
          on tuc.sequence_customer_id = t.sequence_customer_id
         and tuc.transaction_month = t.transaction_month
    where (t.trx_count = 0 or t.trx_count is null)

    union all

    select *
    from {{ ref('int_rules_discounts') }}
)

select *
from all_rules
{% if is_incremental() %}
  where transaction_month >= (select max(transaction_month) from {{ this }})
{% endif %}
