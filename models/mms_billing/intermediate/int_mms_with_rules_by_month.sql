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

        array_to_string(case when is_array(try_parse_json(property_filters_json):flow) then try_parse_json(property_filters_json):flow else null end, ',') as flow_filters,
        array_to_string(case when is_array(try_parse_json(property_filters_json):transaction_type) then try_parse_json(property_filters_json):transaction_type else null end, ',') as transaction_type_filters,
        array_to_string(case when is_array(try_parse_json(property_filters_json):origination_system) then try_parse_json(property_filters_json):origination_system else null end, ',') as origination_system_filters,
        array_to_string(case when is_array(try_parse_json(property_filters_json):source_account_type) then try_parse_json(property_filters_json):source_account_type else null end, ',') as source_account_type_filters,
        array_to_string(case when is_array(try_parse_json(property_filters_json):country) then try_parse_json(property_filters_json):country else null end, ',') as country_filters,
        array_to_string(case when is_array(try_parse_json(property_filters_json):origin_bank) then try_parse_json(property_filters_json):origin_bank else null end, ',') as origin_bank_filters,
        array_to_string(case when is_array(try_parse_json(property_filters_json):destination_bank) then try_parse_json(property_filters_json):destination_bank else null end, ',') as destination_bank_filters,
        array_to_string(case when is_array(try_parse_json(property_filters_json):status) then try_parse_json(property_filters_json):status else null end, ',') as status_filters,

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
),

matched_raw as (
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

        (
          (r.flow_filters is null or match_flow)
          and (r.transaction_type_filters is null or match_transaction_type)
          and (r.origination_system_filters is null or match_origination_system)
          and (r.source_account_type_filters is null or match_source_account_type)
          and (r.country_filters is null or match_country)
          and (r.origin_bank_filters is null or match_origin_bank)
          and (r.destination_bank_filters is null or match_destination_bank)
          and (r.status_filters is null or match_status)
        ) as is_match_all_filters,

        (
          (r.negate_flow and match_flow)
          or (r.negate_transaction_type and match_transaction_type)
          or (r.negate_origination_system and match_origination_system)
          or (r.negate_source_account_type and match_source_account_type)
          or (r.negate_country and match_country)
          or (r.negate_origin_bank and match_origin_bank)
          or (r.negate_destination_bank and match_destination_bank)
          or (r.negate_status and match_status)
        ) as is_negated_by_any_filter,

        array_to_string(array_construct_compact(
            iff(match_flow, iff(r.negate_flow, 'NOT_FLOW', 'FLOW'), null),
            iff(match_transaction_type, iff(r.negate_transaction_type, 'NOT_TRANSACTION_TYPE', 'TRANSACTION_TYPE'), null),
            iff(match_origination_system, iff(r.negate_origination_system, 'NOT_ORIGINATION_SYSTEM', 'ORIGINATION_SYSTEM'), null),
            iff(match_source_account_type, iff(r.negate_source_account_type, 'NOT_SOURCE_ACCOUNT_TYPE', 'SOURCE_ACCOUNT_TYPE'), null),
            iff(match_country, iff(r.negate_country, 'NOT_COUNTRY', 'COUNTRY'), null),
            iff(match_origin_bank, iff(r.negate_origin_bank, 'NOT_ORIGIN_BANK', 'ORIGIN_BANK'), null),
            iff(match_destination_bank, iff(r.negate_destination_bank, 'NOT_DESTINATION_BANK', 'DESTINATION_BANK'), null),
            iff(match_status, iff(r.negate_status, 'NOT_STATUS', 'STATUS'), null)
        ), ', ') as match_reason,

        coalesce(not is_negated_by_any_filter, true) as should_be_charged

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
,

discount as (
    select
        null as mm_id,
        null as amount,
        r.client_id,
        r.sequence_customer_id,
        r.group_id,
        month as local_created_at,
        concat(r.title, iff(r.product_name is not null, concat('||', r.product_name), '')) as matched_product_name,
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
where is_match_all_filters

union all

select *
from platform_fee_always_charged

union all

select *
from discount
