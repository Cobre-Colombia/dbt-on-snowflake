select *
from {{ ref('int_rules_mms') }}

union all

select *
from {{ ref('int_rules_platform_fee_always_charged') }}

union all

select *
from {{ ref('int_rules_discounts') }}

union all

select tuc.*
from {{ ref('int_rules_true_up_charge') }} tuc
left join {{ ref('int_rules_mms_count') }} t
      on tuc.sequence_customer_id = t.sequence_customer_id
     and tuc.transaction_month = t.transaction_month
where t.trx_count = 0 or t.trx_count is null
