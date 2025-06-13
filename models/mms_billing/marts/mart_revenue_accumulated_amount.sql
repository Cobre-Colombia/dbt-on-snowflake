{{ config(
    post_hook=[
        "grant select on view {{ this }} to role data_dev_l1",
        "grant select on view {{ this }} to role sales_ops_l1"
    ]
) }}

select * from {{ ref('int_revenue_cumulative_amount') }}
