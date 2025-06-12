with rules as (
    select
        client_id,
        product_name,
        price_structure_json,
        price_minimum_amount,
        consumes_saas,
        property_filters_json,
        PROPERTIES_TO_NEGATE as properties_to_negate
    from {{ ref('stg_invoice_pricing') }}
),

-- Flatten de cada campo de filtros
flow_flat as (
    select product_name, f.value::string as flow
    from rules, lateral flatten(input => property_filters_json:flow) f
),
tx_flat as (
    select product_name, f.value::string as transaction_type
    from rules, lateral flatten(input => property_filters_json:transaction_type) f
),
origin_flat as (
    select product_name, f.value::string as origination_system
    from rules, lateral flatten(input => property_filters_json:origination_system) f
),
source_flat as (
    select product_name, f.value::string as source_account_type
    from rules, lateral flatten(input => property_filters_json:source_account_type) f
),
country_flat as (
    select product_name, f.value::string as country
    from rules, lateral flatten(input => property_filters_json:country) f
),
origin_bank_flat as (
    select product_name, f.value::string as origin_bank
    from rules, lateral flatten(input => property_filters_json:origin_bank) f
),
destination_bank_flat as (
    select product_name, f.value::string as destination_bank
    from rules, lateral flatten(input => property_filters_json:destination_bank) f
),
status_flat as (
    select product_name, f.value::string as status
    from rules, lateral flatten(input => property_filters_json:status) f
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
        country_flat.country,
        origin_bank_flat.origin_bank,
        destination_bank_flat.destination_bank,
        status_flat.status,

        -- Negation flags
        array_contains(coalesce(r.properties_to_negate::array, []), to_variant('flow')) as negate_flow,
        array_contains(coalesce(r.properties_to_negate::array, []), to_variant('transaction_type')) as negate_transaction_type,
        array_contains(coalesce(r.properties_to_negate::array, []), to_variant('origination_system')) as negate_origination_system,
        array_contains(coalesce(r.properties_to_negate::array, []), to_variant('source_account_type')) as negate_source_account_type,
        array_contains(coalesce(r.properties_to_negate::array, []), to_variant('country')) as negate_country,
        array_contains(coalesce(r.properties_to_negate::array, []), to_variant('origin_bank')) as negate_origin_bank,
        array_contains(coalesce(r.properties_to_negate::array, []), to_variant('destination_bank')) as negate_destination_bank,
        array_contains(coalesce(r.properties_to_negate::array, []), to_variant('status')) as negate_status
    from rules r
    left join flow_flat on r.product_name = flow_flat.product_name
    left join tx_flat on r.product_name = tx_flat.product_name
    left join origin_flat on r.product_name = origin_flat.product_name
    left join source_flat on r.product_name = source_flat.product_name
    left join country_flat on r.product_name = country_flat.product_name
    left join origin_bank_flat on r.product_name = origin_bank_flat.product_name
    left join destination_bank_flat on r.product_name = destination_bank_flat.product_name
    left join status_flat on r.product_name = status_flat.product_name
),

mm as (
    select * from {{ ref('stg_payouts_mms') }}
    union all
    select * from {{ ref('stg_payin_mms') }}
    union all
    select * from {{ ref('stg_dac_mms') }}
    union all
    select * from {{ ref('stg_balance_recharges') }}
),

matched as (
    select
        mm.mm_id,
        mm.client_id,
        rf.product_name as matched_product_name,
        rf.price_structure_json,
        rf.price_minimum_amount,
        rf.consumes_saas,

        (
            rf.negate_flow OR
            rf.negate_transaction_type OR
            rf.negate_origination_system OR
            rf.negate_source_account_type OR
            rf.negate_country OR
            rf.negate_origin_bank OR
            rf.negate_destination_bank OR
            rf.negate_status
        ) as negate_applied

    from mm
    join rules_joined rf
      on (rf.flow is null or (
            (not rf.negate_flow and upper(mm.flow) = upper(rf.flow)) or
            (rf.negate_flow and upper(mm.flow) != upper(rf.flow))
         ))
     and (rf.transaction_type is null or (
            (not rf.negate_transaction_type and upper(mm.transaction_type) = upper(rf.transaction_type)) or
            (rf.negate_transaction_type and upper(mm.transaction_type) != upper(rf.transaction_type))
         ))
     and (rf.origination_system is null or (
            (not rf.negate_origination_system and upper(mm.origination_system) = upper(rf.origination_system)) or
            (rf.negate_origination_system and upper(mm.origination_system) != upper(rf.origination_system))
         ))
     and (rf.source_account_type is null or (
            (not rf.negate_source_account_type and upper(mm.source_account_type) = upper(rf.source_account_type)) or
            (rf.negate_source_account_type and upper(mm.source_account_type) != upper(rf.source_account_type))
         ))
     and (rf.country is null or (
            (not rf.negate_country and upper(mm.country) = upper(rf.country)) or
            (rf.negate_country and upper(mm.country) != upper(rf.country))
         ))
     and (rf.origin_bank is null or (
            (not rf.negate_origin_bank and upper(mm.origin_bank) = upper(rf.origin_bank)) or
            (rf.negate_origin_bank and upper(mm.origin_bank) != upper(rf.origin_bank))
         ))
     and (rf.destination_bank is null or (
            (not rf.negate_destination_bank and upper(mm.destination_bank) = upper(rf.destination_bank)) or
            (rf.negate_destination_bank and upper(mm.destination_bank) != upper(rf.destination_bank))
         ))
     and (rf.status is null or (
            (not rf.negate_status and upper(mm.status) = upper(rf.status)) or
            (rf.negate_status and upper(mm.status) != upper(rf.status))
         ))
     and mm.client_id = rf.client_id
),

final as (
    select
        mm.*,
        m.matched_product_name,
        m.price_structure_json,
        m.price_minimum_amount,
        m.consumes_saas,
        case when m.matched_product_name is not null then true else false end as should_be_charged,
        m.negate_applied
    from mm
    left join matched m
      on mm.mm_id = m.mm_id
     and mm.client_id = m.client_id
)

select distinct * from final
where price_structure_json is not null
