{{ config(
    post_hook=[
        "grant select on view {{ this }} to role DATA_DEV_L1",
        "grant select on view {{ this }} to role SALES_OPS_DEV_L0"
    ]
) }}

select * from {{ ref('int_revenue_cumulative_amount') }}
