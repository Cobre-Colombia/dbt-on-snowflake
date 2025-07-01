select *
from {{ ref('mart_revenue_mms') }}

union all

select *
from {{ ref('mart_revenue_platform_fee') }}

union all

select *
from {{ ref('mart_revenue_discount') }}

union all

select *
from {{ ref('mart_revenue_true_up_charge') }} tuc


{% if is_incremental() %}
    where local_updated_at > (select max(local_updated_at) from {{ this }})
{% endif %}



