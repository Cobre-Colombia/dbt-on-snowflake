{{ config(
    materialized='table',
    post_hook=[
        "grant select on view {{ this }} to role DATA_DEV_L1"
    ]
) }}

{% set cutoff_date = modules.datetime.datetime.utcnow().date() - modules.datetime.timedelta(days=0) %}

with rules as (
    select distinct
        client_id,
        sequence_customer_id,
        group_id,
        product_name,
        price_structure_json,
        price_minimum_amount,
        consumes_saas,
        property_filters_json,

        upper(array_to_string(case when is_array(try_parse_json(property_filters_json):flow) then try_parse_json(property_filters_json):flow else null end, ',')) as flow_filters,
        upper(array_to_string(case when is_array(try_parse_json(property_filters_json):transaction_type) then try_parse_json(property_filters_json):transaction_type else null end, ',')) as transaction_type_filters,
        upper(array_to_string(case when is_array(try_parse_json(property_filters_json):origination_system) then try_parse_json(property_filters_json):origination_system else null end, ',')) as origination_system_filters,
        upper(array_to_string(case when is_array(try_parse_json(property_filters_json):source_account_type) then try_parse_json(property_filters_json):source_account_type else null end, ',')) as source_account_type_filters,
        upper(array_to_string(case when is_array(try_parse_json(property_filters_json):country) then try_parse_json(property_filters_json):country else null end, ',')) as country_filters,
        upper(array_to_string(case when is_array(try_parse_json(property_filters_json):origin_bank) then try_parse_json(property_filters_json):origin_bank else null end, ',')) as origin_bank_filters,
        upper(array_to_string(case when is_array(try_parse_json(property_filters_json):destination_bank) then try_parse_json(property_filters_json):destination_bank else null end, ',')) as destination_bank_filters,
        upper(array_to_string(case when is_array(try_parse_json(property_filters_json):status) then try_parse_json(property_filters_json):status else null end, ',')) as status_filters,

        array_to_string(case when is_array(try_parse_json(properties_to_negate)) then try_parse_json(properties_to_negate) else null end, ',') as properties_to_negate_str,

        contains(properties_to_negate_str, 'flow') as negate_flow,
        contains(properties_to_negate_str, 'transaction_type') as negate_transaction_type,
        contains(properties_to_negate_str, 'origination_system') as negate_origination_system,
        contains(properties_to_negate_str, 'source_account_type') as negate_source_account_type,
        contains(properties_to_negate_str, 'country') as negate_country,
        contains(properties_to_negate_str, 'origin_bank') as negate_origin_bank,
        contains(properties_to_negate_str, 'destination_bank') as negate_destination_bank,
        contains(properties_to_negate_str, 'status') as negate_status,

        date(price_structure_month) price_structure_month,
        currency
    from {{ ref('stg_invoice_pricing_by_month') }}
    where price_structure_json is not null
      and upper(product_name) not in ('DISCOUNT', 'PLATFORM FEE')
),

mm as (
    select *, date_trunc('month', eventtimestamp) as event_month from (
        select mm_id, client_id, eventtype, eventtimestamp, flow, transaction_type, origination_system,
               source_account_type, destination_bank, origin_bank, country, status, amount, updated_at,
               local_created_at, client_id_edit as regional_client_id
        from {{ ref('stg_payouts_mms') }} where local_created_at < '{{ cutoff_date }}'
        union all
        select mm_id, client_id, eventtype, eventtimestamp, flow, transaction_type, origination_system,
               source_account_type, destination_bank, origin_bank, country, status, amount, updated_at,
               local_created_at, client_id_edit as regional_client_id
        from {{ ref('stg_payin_mms') }} where local_created_at < '{{ cutoff_date }}'
        union all
        select mm_id, client_id, eventtype, eventtimestamp, flow, transaction_type, origination_system,
               source_account_type, destination_bank, origin_bank, country, status, amount, updated_at,
               local_created_at, client_id_edit as regional_client_id
        from {{ ref('stg_dac_mms') }} where local_created_at < '{{ cutoff_date }}'
        union all
        select mm_id, client_id, eventtype, eventtimestamp, flow, transaction_type, origination_system,
               source_account_type, destination_bank, origin_bank, country, status, amount, updated_at,
               local_created_at, client_id_edit as regional_client_id
        from {{ ref('stg_balance_recharges') }} where local_created_at < '{{ cutoff_date }}'
    )
)

, matched_raw as (
    select
        mm.*,
        r.sequence_customer_id,
        r.group_id,
        r.product_name as matched_product_name,
        r.price_structure_json,
        r.price_minimum_amount,
        r.consumes_saas,
        r.property_filters_json,
        r.properties_to_negate_str,
        r.price_structure_month,
        r.currency,

        contains(r.flow_filters, upper(mm.flow)) as match_flow,
        contains(r.transaction_type_filters, upper(mm.transaction_type)) as match_transaction_type,
        contains(r.origination_system_filters, upper(mm.origination_system)) as match_origination_system,
        contains(r.source_account_type_filters, upper(mm.source_account_type)) as match_source_account_type,
        contains(r.country_filters, upper(mm.country)) as match_country,
        contains(r.origin_bank_filters, upper(mm.origin_bank)) as match_origin_bank,
        contains(r.destination_bank_filters, upper(mm.destination_bank)) as match_destination_bank,
        contains(r.status_filters, upper(mm.status)) as match_status,

        (r.negate_flow and match_flow) as negate_match_flow,
        (r.negate_transaction_type and match_transaction_type) as negate_match_transaction_type,
        (r.negate_origination_system and match_origination_system) as negate_match_origination_system,
        (r.negate_source_account_type and match_source_account_type) as negate_match_source_account_type,
        (r.negate_country and match_country) as negate_match_country,
        (r.negate_origin_bank and match_origin_bank) as negate_match_origin_bank,
        (r.negate_destination_bank and match_destination_bank) as negate_match_destination_bank,
        (r.negate_status and match_status) as negate_match_status,
        
        iff(r.properties_to_negate_str is not null,true, false) properties_to_negate_flag,
        
        (
          iff(r.negate_flow, true, (r.flow_filters is null or match_flow))
          and iff(r.negate_transaction_type, true, (r.transaction_type_filters is null or match_transaction_type))
          and iff(r.negate_origination_system, true, (r.origination_system_filters is null or match_origination_system))
          and iff(r.negate_source_account_type, true, (r.source_account_type_filters is null or match_source_account_type))
          and iff(r.negate_country, true, (r.country_filters is null or match_country))
          and iff(r.negate_origin_bank, true, (r.origin_bank_filters is null or match_origin_bank))
          and iff(r.negate_destination_bank, true, (r.destination_bank_filters is null or match_destination_bank))
          and iff(r.negate_status, true, (r.status_filters is null or match_status))
        ) as matches_positive_filters,
        
        ( properties_to_negate_flag and (
          (
               NEGATE_MATCH_FLOW
            OR NEGATE_MATCH_TRANSACTION_TYPE
            OR NEGATE_MATCH_ORIGINATION_SYSTEM
            OR NEGATE_MATCH_SOURCE_ACCOUNT_TYPE
            OR NEGATE_MATCH_COUNTRY
            OR NEGATE_MATCH_ORIGIN_BANK
            OR NEGATE_MATCH_DESTINATION_BANK
            OR NEGATE_MATCH_STATUS
          )
        )) as matches_negate_filters,

        array_to_string(array_construct_compact(
            iff(match_flow and not r.negate_flow, 'FLOW', null),
            iff(not match_flow and r.negate_flow, 'NOT_FLOW', null),
        
            iff(match_transaction_type and not r.negate_transaction_type, 'TRANSACTION_TYPE', null),
            iff(not match_transaction_type and r.negate_transaction_type, 'NOT_TRANSACTION_TYPE', null),
        
            iff(match_origination_system and not r.negate_origination_system, 'ORIGINATION_SYSTEM', null),
            iff(not match_origination_system and r.negate_origination_system, 'NOT_ORIGINATION_SYSTEM', null),
        
            iff(match_source_account_type and not r.negate_source_account_type, 'SOURCE_ACCOUNT_TYPE', null),
            iff(not match_source_account_type and r.negate_source_account_type, 'NOT_SOURCE_ACCOUNT_TYPE', null),
        
            iff(match_country and not r.negate_country, 'COUNTRY', null),
            iff(not match_country and r.negate_country, 'NOT_COUNTRY', null),
        
            iff(match_origin_bank and not r.negate_origin_bank, 'ORIGIN_BANK', null),
            iff(not match_origin_bank and r.negate_origin_bank, 'NOT_ORIGIN_BANK', null),
        
            iff(match_destination_bank and not r.negate_destination_bank, 'DESTINATION_BANK', null),
            iff(not match_destination_bank and r.negate_destination_bank, 'NOT_DESTINATION_BANK', null),
        
            iff(match_status and not r.negate_status, 'STATUS', null),
            iff(not match_status and r.negate_status, 'NOT_STATUS', null)
        ), ', ') as match_reason,
        
        CASE
           WHEN properties_to_negate_flag = false 
                AND matches_positive_filters
           THEN TRUE 
           WHEN properties_to_negate_flag = true
                AND NOT matches_negate_filters
                AND matches_positive_filters
           THEN TRUE
        ELSE FALSE END as should_be_charged
        
    from mm
    join rules r
      on coalesce(mm.regional_client_id, mm.client_id) = r.client_id
     and mm.event_month = r.price_structure_month
)

, platform_fee_always_charged as (
    select
        null as mm_id,
        null as amount,
        r.client_id,
        r.sequence_customer_id,
        r.group_id,
        month as local_created_at,
        r.product_name as matched_product_name,
        r.price_structure_json,
        r.price_minimum_amount,
        r.consumes_saas,
        true as should_be_charged,
        r.currency,
        null as flow,
        null as transaction_type,
        null as origination_system,
        null as source_account_type,
        null as country,
        null as origin_bank,
        null as destination_bank,
        null as status,
        r.property_filters_json,
        null as properties_to_negate,
        date_trunc('month', local_created_at) as transaction_month,
        month as local_updated_at,
        hash(null, sequence_customer_id, matched_product_name, local_created_at, null) as hash_match
    from {{ ref('stg_invoice_pricing_by_month') }} r
    where upper(product_name) = 'PLATFORM FEE'
      and try_parse_json(price_structure_json):pricingType::string = 'FIXED'
)
, discount as (
    select
        null as mm_id,
        null as amount,
        r.client_id,
        r.sequence_customer_id,
        r.group_id,
        month as local_created_at,
        concat(r.title, iff(r.product_name is not null, concat('||', r.product_name), '')) as matched_product_name,
        --r.product_name as matched_product_name,
        coalesce(r.price_structure_json, parse_json('{
            "price": ' || r.net_total || ',
            "pricingType": "FIXED"
        }')) as price_structure_json,
        r.price_minimum_amount,
        r.consumes_saas,
        true as should_be_charged,
        r.currency,
        null as flow,
        null as transaction_type,
        null as origination_system,
        null as source_account_type,
        null as country,
        null as origin_bank,
        null as destination_bank,
        null as status,
        r.property_filters_json,
        null as properties_to_negate,
        date_trunc('month', local_created_at) as transaction_month,
        month as local_updated_at,
        hash(null, sequence_customer_id, matched_product_name, local_created_at, null) as hash_match
    from {{ ref('stg_invoice_pricing_by_month') }} r
    where upper(title) = 'DISCOUNT'
)

, true_up_charge as (
    select
        null as mm_id,
        null as amount,
        null as client_id,
        r.sequence_customer_id,
        r.group_id,
        month as local_created_at,
        r.title as matched_product_name,
        coalesce(r.price_structure_json, parse_json('{
            "price": ' || r.net_total || ',
            "pricingType": "TRUE_UP_CHARGE"
        }')) as price_structure_json,
        r.price_minimum_amount,
        r.consumes_saas,
        true as should_be_charged,
        r.currency,
        null as flow,
        null as transaction_type,
        null as origination_system,
        null as source_account_type,
        null as country,
        null as origin_bank,
        null as destination_bank,
        null as status,
        r.property_filters_json,
        null as properties_to_negate,
        date_trunc('month', local_created_at) as transaction_month,
        month as local_updated_at,
        hash(null, sequence_customer_id, matched_product_name, local_created_at, null) as hash_match
    from {{ ref('stg_invoice_pricing_by_month') }} r
    where upper(title) = 'TRUE UP CHARGE'
)

, trx_count as (
    select
        sequence_customer_id,
        date_trunc('month', local_created_at) as transaction_month,
        count(*) as trx_count
    from matched_raw
    where consumes_saas
    group by 1, 2
)

select
    mm_id, amount, client_id, sequence_customer_id, group_id, local_created_at,
    matched_product_name, price_structure_json, price_minimum_amount,
    consumes_saas, should_be_charged, currency,
    flow, transaction_type, origination_system, source_account_type,
    country, origin_bank, destination_bank, status,
    property_filters_json, properties_to_negate_str as properties_to_negate,
    date_trunc('month', local_created_at) as transaction_month,
    updated_at as local_updated_at,
    hash(mm_id, sequence_customer_id, matched_product_name, local_created_at, amount) as hash_match
from matched_raw

union all

select *
from platform_fee_always_charged

union all

select *
from discount

union all

select tuc.*
from true_up_charge tuc
left join trx_count t
      on tuc.sequence_customer_id = t.sequence_customer_id
     and tuc.transaction_month = t.transaction_month
where t.trx_count = 0 or t.trx_count is null
