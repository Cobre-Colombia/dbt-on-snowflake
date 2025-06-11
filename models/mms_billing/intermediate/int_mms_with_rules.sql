with rules as (
    select
        client_id,
        product_name,
        price_structure_json,
        price_minimum_amount,
        consumes_saas,
        property_filters_json
    from {{ ref('stg_invoice_pricing') }}
),

flow_flat as (
    select
        product_name,
        f.value::string as flow
    from rules,
         lateral flatten(input => property_filters_json:flow) f
),

tx_flat as (
    select
        product_name,
        f.value::string as transaction_type
    from rules,
         lateral flatten(input => property_filters_json:transaction_type) f
),

origin_flat as (
    select
        product_name,
        f.value::string as origination_system
    from rules,
         lateral flatten(input => property_filters_json:origination_system) f
),

source_flat as (
    select
        product_name,
        f.value::string as source_account_type
    from rules,
         lateral flatten(input => property_filters_json:source_account_type) f
),

country_flat as (
    select
        product_name,
        f.value::string as country
    from rules,
         lateral flatten(input => property_filters_json:country) f
),
rules_joined as (
    select
        r.client_id,
        r.product_name,
        r.price_structure_json,
        r.price_minimum_amount,
        r.consumes_saas,
        flow_flat.flow,
        tx_flat.transaction_type,
        origin_flat.origination_system,
        source_flat.source_account_type,
        country_flat.country
    from rules r
    left join flow_flat on r.product_name = flow_flat.product_name
    left join tx_flat on r.product_name = tx_flat.product_name
    left join origin_flat on r.product_name = origin_flat.product_name
    left join source_flat on r.product_name = source_flat.product_name
    left join country_flat on r.product_name = country_flat.product_name
)

, mm as (
    select * from {{ ref('stg_payouts_mms') }}
    union all
    select * from {{ ref('stg_payin_mms') }}
    union all
    select * from {{ ref('stg_dag_mms') }}
    union all
    select * from {{ ref('stg_balance_recharges') }}
),

joined as (
    select
        mm.*,
        rf.product_name as matched_product_name,
        rf.price_structure_json,
        rf.price_minimum_amount,
        rf.consumes_saas
    from mm
    join rules_joined rf
        on (rf.flow is null or upper(mm.flow) = upper(rf.flow))
       and (rf.transaction_type is null or upper(mm.transaction_type) = upper(rf.transaction_type))
       and (rf.origination_system is null or upper(mm.origination_system) = upper(rf.origination_system))
       and (rf.source_account_type is null or upper(mm.source_account_type) = upper(rf.source_account_type))
       and (rf.country is null or upper(mm.country) = upper(rf.country))
       and (mm.client_id = rf.client_id)
)

select * from joined
