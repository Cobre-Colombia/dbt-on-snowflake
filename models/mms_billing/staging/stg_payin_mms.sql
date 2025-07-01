{% set end_cutoff_date = modules.datetime.datetime.now().date() - modules.datetime.timedelta(days=0) %}
{% set start_cutoff_date = (end_cutoff_date.replace(day=1) - modules.datetime.timedelta(days=90)).replace(day=1) %}

with data as (
    select *, ID AS MM_ID, TOTAL_AMOUNT as AMOUNT, UTC_CREATED_AT as LOCAL_CREATED_AT
    from {{ source('SALES_OPS', 'PAYIN_TRANSACTIONS') }}
    where EVENTTIMESTAMP::DATE between '{{ start_cutoff_date }}' and '{{ end_cutoff_date }}'
)
select *
from data
