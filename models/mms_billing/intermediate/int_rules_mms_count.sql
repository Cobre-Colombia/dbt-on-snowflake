select
    sequence_customer_id,
    date_trunc('month', local_created_at) as transaction_month,
    count(*) as trx_count
from {{ ref('int_rules_mms') }}
where consumes_saas and should_be_charged
group by 1, 2
