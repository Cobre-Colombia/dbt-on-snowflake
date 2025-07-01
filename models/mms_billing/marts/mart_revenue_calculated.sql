{{ config(materialized='view') }}

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
from {{ ref('mart_revenue_true_up_charge') }}

