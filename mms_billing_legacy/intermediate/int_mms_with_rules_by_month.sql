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
        parse_json(properties_to_negate) as properties_to_negate,
        price_structure_month,
        currency
    from {{ ref('stg_invoice_pricing_by_month') }}
    where price_structure_json is not null and upper(product_name) not in ('DISCOUNT', 'PLATFORM FEE')
),

platform_fee_always_charged as (
    select distinct
        null as id,
        r.client_id,
        null as eventtype,
        null as eventtimestamp,
        null as flow,
        null as transaction_type,
        null as origination_system,
        null as source_account_type,
        null as destination_bank,
        null as origin_bank,
        null as country,
        null as status,
        null as total_amount,
        month as local_updated_at,
        month as local_created_at,
        null as mm_id,
        null as amount,
        month as event_month,
        r.product_name as matched_product_name,
        r.price_structure_json,
        r.price_minimum_amount,
        r.consumes_saas,
        r.property_filters_json,
        r.properties_to_negate,
        r.sequence_customer_id,
        r.group_id,
        r.price_structure_month,
        r.currency,
        -- filtros
        null as flow_filter,
        null as transaction_type_filter,
        null as origination_system_filter,
        null as source_account_type_filter,
        null as country_filter,
        null as origin_bank_filter,
        null as destination_bank_filter,
        null as status_filter,

        -- negaciones
        false as negate_flow,
        false as negate_transaction_type,
        false as negate_origination_system,
        false as negate_source_account_type,
        false as negate_country,
        false as negate_origin_bank,
        false as negate_destination_bank,
        false as negate_status,

        -- matches de negaciones
        false as negate_flow_match,
        false as negate_transaction_type_match,
        false as negate_origination_system_match,
        false as negate_source_account_type_match,
        false as negate_country_match,
        false as negate_origin_bank_match,
        false as negate_destination_bank_match,
        false as negate_status_match,

        false as negate_applied,
        'FIXED_PRICE_ALWAYS_CHARGED' as match_reason,
        99 as sequential_match_score,
        1 as rank_by_seq,
        true as should_be_charged,
        0 as tx_type_mismatch,
        1 as rn
    from {{ ref('stg_invoice_pricing_by_month') }} r
    where upper(product_name) = 'PLATFORM FEE'
      and try_parse_json(price_structure_json):pricingType::string = 'FIXED'
),

discount as (
    select distinct
        null as id,
        r.client_id,
        null as eventtype,
        null as eventtimestamp,
        null as flow,
        null as transaction_type,
        null as origination_system,
        null as source_account_type,
        null as destination_bank,
        null as origin_bank,
        null as country,
        null as status,
        null as total_amount,
        month as local_updated_at,
        month as local_created_at,
        null as mm_id,
        null as amount,
        month as event_month,
        concat(r.title, iff(r.product_name is not null, concat('||', r.product_name), '')) as matched_product_name, 
        coalesce(r.price_structure_json, 
            parse_json('{
                "price": ' || r.net_total || ',
                "pricingType": "FIXED"
            }'
        )) as price_structure_json,
        r.price_minimum_amount,
        r.consumes_saas,
        r.property_filters_json,
        r.properties_to_negate,
        r.sequence_customer_id,
        r.group_id,
        r.price_structure_month,
        r.currency,
        -- filtros
        null as flow_filter,
        null as transaction_type_filter,
        null as origination_system_filter,
        null as source_account_type_filter,
        null as country_filter,
        null as origin_bank_filter,
        null as destination_bank_filter,
        null as status_filter,

        -- negaciones
        false as negate_flow,
        false as negate_transaction_type,
        false as negate_origination_system,
        false as negate_source_account_type,
        false as negate_country,
        false as negate_origin_bank,
        false as negate_destination_bank,
        false as negate_status,

        -- matches de negaciones
        false as negate_flow_match,
        false as negate_transaction_type_match,
        false as negate_origination_system_match,
        false as negate_source_account_type_match,
        false as negate_country_match,
        false as negate_origin_bank_match,
        false as negate_destination_bank_match,
        false as negate_status_match,

        false as negate_applied,
        'DISCOUNT' as match_reason,
        99 as sequential_match_score,
        1 as rank_by_seq,
        true as should_be_charged,
        0 as tx_type_mismatch,
        1 as rn
    from {{ ref('stg_invoice_pricing_by_month') }} r
    where upper(title) = 'DISCOUNT'
),

negations as (
    select distinct sequence_customer_id, product_name,
        max(iff(upper(f.value::string) = 'FLOW', true, false)) as negate_flow,
        max(iff(upper(f.value::string) = 'TRANSACTION_TYPE', true, false)) as negate_transaction_type,
        max(iff(upper(f.value::string) = 'ORIGINATION_SYSTEM', true, false)) as negate_origination_system,
        max(iff(upper(f.value::string) = 'SOURCE_ACCOUNT_TYPE', true, false)) as negate_source_account_type,
        max(iff(upper(f.value::string) = 'COUNTRY', true, false)) as negate_country,
        max(iff(upper(f.value::string) = 'ORIGIN_BANK', true, false)) as negate_origin_bank,
        max(iff(upper(f.value::string) = 'DESTINATION_BANK', true, false)) as negate_destination_bank,
        max(iff(upper(f.value::string) = 'STATUS', true, false)) as negate_status
    from rules,
         lateral flatten(input => properties_to_negate) f
    group by sequence_customer_id, product_name
),

flow_flat as (
    select distinct sequence_customer_id, product_name,upper(f.value::string) as flow_filter
    from rules, lateral flatten(input => property_filters_json:flow) f
),
tx_flat as (
    select distinct sequence_customer_id, product_name,upper(f.value::string) as transaction_type_filter
    from rules, lateral flatten(input => property_filters_json:transaction_type) f
),
origin_flat as (
    select distinct sequence_customer_id, product_name,upper(f.value::string) as origination_system_filter
    from rules, lateral flatten(input => property_filters_json:origination_system) f
),
source_flat as (
    select distinct sequence_customer_id, product_name,upper(f.value::string) as source_account_type_filter
    from rules, lateral flatten(input => property_filters_json:source_account_type) f
),
country_flat as (
    select distinct sequence_customer_id, product_name,upper(f.value::string) as country_filter
    from rules, lateral flatten(input => property_filters_json:country) f
),
origin_bank_flat as (
    select distinct sequence_customer_id, product_name,upper(f.value::string) as origin_bank_filter
    from rules, lateral flatten(input => property_filters_json:origin_bank) f
),
destination_bank_flat as (
    select distinct sequence_customer_id, product_name, upper(f.value::string) as destination_bank_filter
    from rules, lateral flatten(input => property_filters_json:destination_bank) f
)
,
status_flat as (
    select distinct sequence_customer_id, product_name, upper(f.value::string) as status_filter
    from rules, lateral flatten(input => property_filters_json:status) f
),

rules_expanded as (
    select distinct
        r.client_id,
        r.sequence_customer_id,
        r.group_id,
        r.product_name,
        r.price_structure_json,
        r.price_minimum_amount,
        r.consumes_saas,
        r.property_filters_json,
        r.properties_to_negate,
        r.price_structure_month,
        r.currency,
        nf.negate_flow,
        nf.negate_transaction_type,
        nf.negate_origination_system,
        nf.negate_source_account_type,
        nf.negate_country,
        nf.negate_origin_bank,
        nf.negate_destination_bank,
        nf.negate_status,

        flow_filter,
        transaction_type_filter,
        origination_system_filter,
        source_account_type_filter,
        country_filter,
        origin_bank_filter,
        destination_bank_filter,
        status_filter
    from rules r
    left join negations nf on r.product_name = nf.product_name and r.sequence_customer_id = nf.sequence_customer_id
    left join flow_flat on r.product_name = flow_flat.product_name and r.sequence_customer_id = flow_flat.sequence_customer_id
    left join tx_flat on r.product_name = tx_flat.product_name and r.sequence_customer_id = tx_flat.sequence_customer_id
    left join origin_flat on r.product_name = origin_flat.product_name and r.sequence_customer_id = origin_flat.sequence_customer_id
    left join source_flat on r.product_name = source_flat.product_name and r.sequence_customer_id = source_flat.sequence_customer_id
    left join country_flat on r.product_name = country_flat.product_name and r.sequence_customer_id = country_flat.sequence_customer_id
    left join origin_bank_flat on r.product_name = origin_bank_flat.product_name and r.sequence_customer_id = origin_bank_flat.sequence_customer_id
    left join destination_bank_flat on r.product_name = destination_bank_flat.product_name and r.sequence_customer_id = destination_bank_flat.sequence_customer_id
    left join status_flat on r.product_name = status_flat.product_name and r.sequence_customer_id = status_flat.sequence_customer_id
),

mm as (
    select *,
        date_trunc('month', eventtimestamp) as event_month
    from (
        select mm_id, CLIENT_ID, EVENTTYPE, EVENTTIMESTAMP, FLOW, TRANSACTION_TYPE, ORIGINATION_SYSTEM, SOURCE_ACCOUNT_TYPE, DESTINATION_BANK, ORIGIN_BANK, 
                COUNTRY, STATUS, amount, UPDATED_AT, local_created_at, CLIENT_ID_EDIT as regional_client_id
        from {{ ref('stg_payouts_mms') }} where local_created_at < '{{ cutoff_date }}'
        union all
        select mm_id, CLIENT_ID, EVENTTYPE, EVENTTIMESTAMP, FLOW, TRANSACTION_TYPE, ORIGINATION_SYSTEM, SOURCE_ACCOUNT_TYPE, DESTINATION_BANK, ORIGIN_BANK, 
                COUNTRY, STATUS, amount, UPDATED_AT, local_created_at, CLIENT_ID_EDIT as regional_client_id
        from {{ ref('stg_payin_mms') }} where local_created_at < '{{ cutoff_date }}'
        union all
        select mm_id, CLIENT_ID, EVENTTYPE, EVENTTIMESTAMP, FLOW, TRANSACTION_TYPE, ORIGINATION_SYSTEM, SOURCE_ACCOUNT_TYPE, DESTINATION_BANK, ORIGIN_BANK, 
                COUNTRY, STATUS, amount, UPDATED_AT, local_created_at, CLIENT_ID_EDIT as regional_client_id
        from {{ ref('stg_dac_mms') }} where local_created_at < '{{ cutoff_date }}'
        union all
        select mm_id, CLIENT_ID, EVENTTYPE, EVENTTIMESTAMP, FLOW, TRANSACTION_TYPE, ORIGINATION_SYSTEM, SOURCE_ACCOUNT_TYPE, DESTINATION_BANK, ORIGIN_BANK, 
                COUNTRY, STATUS, amount, UPDATED_AT, local_created_at, CLIENT_ID_EDIT as regional_client_id
        from {{ ref('stg_balance_recharges') }} where local_created_at < '{{ cutoff_date }}'
    ) as all_mm
),
matched_raw as (
    select
        mm.mm_id,
        case when rx.client_id like '%_%' then mm.regional_client_id else mm.client_id end as client_id,
        mm.eventtype,
        mm.eventtimestamp,
        mm.flow,
        mm.transaction_type,
        mm.origination_system,
        mm.source_account_type,
        mm.destination_bank,
        mm.origin_bank,
        mm.country,
        mm.status,
        mm.amount,
        mm.updated_at as local_updated_at,
        mm.local_created_at,
        mm.event_month,
        rx.product_name as matched_product_name,
        rx.price_structure_json,
        rx.price_minimum_amount,
        rx.consumes_saas,
        rx.property_filters_json,
        rx.properties_to_negate,
        rx.sequence_customer_id,
        rx.group_id,
        rx.price_structure_month,
        rx.currency,

        rx.flow_filter,
        rx.transaction_type_filter,
        rx.origination_system_filter,
        rx.source_account_type_filter,
        rx.country_filter,
        rx.origin_bank_filter,
        rx.destination_bank_filter,
        rx.status_filter,

        rx.negate_flow,
        rx.negate_transaction_type,
        rx.negate_origination_system,
        rx.negate_source_account_type,
        rx.negate_country,
        rx.negate_origin_bank,
        rx.negate_destination_bank,
        rx.negate_status,

        (rx.negate_flow and upper(mm.flow) = rx.flow_filter) as negate_flow_match,
        (rx.negate_transaction_type and upper(mm.transaction_type) = rx.transaction_type_filter) as negate_transaction_type_match,
        (rx.negate_origination_system and upper(mm.origination_system) = rx.origination_system_filter) as negate_origination_system_match,
        (rx.negate_source_account_type and upper(mm.source_account_type) = rx.source_account_type_filter) as negate_source_account_type_match,
        (rx.negate_country and upper(mm.country) = rx.country_filter) as negate_country_match,
        (rx.negate_origin_bank and upper(mm.origin_bank) = rx.origin_bank_filter) as negate_origin_bank_match,
        (rx.negate_destination_bank and upper(mm.destination_bank) = rx.destination_bank_filter) as negate_destination_bank_match,
        (rx.negate_status and upper(mm.status) = rx.status_filter) as negate_status_match,
        
        iff((rx.properties_to_negate is not null or array_size(rx.properties_to_negate) > 0), true, false) as flag_properties_to_negate,
        ( flag_properties_to_negate and (
            (negate_flow_match) or
            (negate_transaction_type_match) or
            (negate_origination_system_match) or
            (negate_source_account_type_match) or
            (negate_country_match) or
            (negate_origin_bank_match) or
            (negate_destination_bank_match) or
            (negate_status_match)
        ) ) as negate_applied,

        array_to_string(array_construct_compact(
            iff(not negate_flow_match and upper(mm.flow) = rx.flow_filter, 'FLOW', null),
            iff(not negate_transaction_type_match and upper(mm.transaction_type) = rx.transaction_type_filter, 'TRANSACTION_TYPE', null),
            iff(not negate_origination_system_match and upper(mm.origination_system) = rx.origination_system_filter, 'ORIGINATION_SYSTEM', null),
            iff(not negate_source_account_type_match and upper(mm.source_account_type) = rx.source_account_type_filter, 'SOURCE_ACCOUNT_TYPE', null),
            iff(not negate_country_match and upper(mm.country) = rx.country_filter, 'COUNTRY', null),
            iff(not negate_origin_bank_match and upper(mm.origin_bank) = rx.origin_bank_filter, 'ORIGIN_BANK', null),
            iff(not negate_destination_bank_match and upper(mm.destination_bank) = rx.destination_bank_filter, 'DESTINATION_BANK', null),
            iff(not negate_status_match and upper(mm.status) = rx.status_filter, 'STATUS', null),

            iff(negate_flow_match, 'NOT_FLOW', null),
            iff(negate_transaction_type_match, 'NOT_TRANSACTION_TYPE', null),
            iff(negate_origination_system_match, 'NOT_ORIGINATION_SYSTEM', null),
            iff(negate_source_account_type_match, 'NOT_SOURCE_ACCOUNT_TYPE', null),
            iff(negate_country_match, 'NOT_COUNTRY', null),
            iff(negate_origin_bank_match, 'NOT_ORIGIN_BANK', null),
            iff(negate_destination_bank_match, 'NOT_DESTINATION_BANK', null),
            iff(negate_status_match, 'NOT_STATUS', null)
        ), ', ') as match_reason

    from mm
    join rules_expanded rx
      on case when mm.client_id like '%_%' then mm.regional_client_id else mm.client_id end = rx.client_id
     and mm.event_month = rx.price_structure_month
     and (
         (rx.flow_filter is not null and upper(mm.flow) = rx.flow_filter) or
         (rx.transaction_type_filter is not null and upper(mm.transaction_type) = rx.transaction_type_filter) or
         (rx.origination_system_filter is not null and upper(mm.origination_system) = rx.origination_system_filter) or
         (rx.source_account_type_filter is not null and upper(mm.source_account_type) = rx.source_account_type_filter) or
         (rx.country_filter is not null and upper(mm.country) = rx.country_filter) or
         (rx.origin_bank_filter is not null and upper(mm.origin_bank) = rx.origin_bank_filter) or
         (rx.destination_bank_filter is not null and upper(mm.destination_bank) = rx.destination_bank_filter) or
         (rx.status_filter is not null and upper(mm.status) = rx.status_filter)
     )
)
,

matched_with_priority as (
    select *,
    case
      when (flow_filter is null) 
            or (flag_properties_to_negate = false and upper(flow) = upper(flow_filter))
            or (flag_properties_to_negate = true and negate_applied) then
        case
          when transaction_type_filter is null 
                or (flag_properties_to_negate = false and upper(transaction_type) = upper(transaction_type_filter))
                or (flag_properties_to_negate = true and negate_applied) then
            case
              when origination_system_filter is null  
                    or (flag_properties_to_negate = false and upper(origination_system) = upper(origination_system_filter))
                    or (flag_properties_to_negate = true and negate_applied) then
                case
                  when source_account_type_filter is null 
                        or (flag_properties_to_negate = false and upper(source_account_type) = upper(source_account_type_filter))
                        or (flag_properties_to_negate = true and negate_applied) then
                    case
                      when country_filter is null 
                            or (flag_properties_to_negate = false and upper(country) = upper(country_filter))
                            or (flag_properties_to_negate = true and negate_applied) then
                        case
                          when origin_bank_filter is null 
                                or (flag_properties_to_negate = false and upper(origin_bank) = upper(origin_bank_filter))
                                or (flag_properties_to_negate = true and negate_applied) then
                            case
                              when destination_bank_filter is null 
                                    or (flag_properties_to_negate = false and upper(destination_bank) = upper(destination_bank_filter))
                                    or (flag_properties_to_negate = true and negate_applied) then
                                case
                                  when status_filter is null 
                                        or (flag_properties_to_negate = false and upper(status) = upper(status_filter))
                                        or (flag_properties_to_negate = true and negate_applied) then 1
                                  else 0
                                end
                              else 0
                            end
                          else 0
                        end
                      else 0
                    end
                  else 0
                end
              else 0
            end
          else 0
        end
      else 0
    end as sequential_match_score
    from matched_raw
),

ranked as (
    select *,
        rank() over (partition by mm_id order by sequential_match_score desc) as rank_by_seq
    from matched_with_priority
    where sequential_match_score = 1
),

should_be_charged as (
    select *,
        coalesce(not negate_applied, true) as should_be_charged
    from ranked
    where rank_by_seq = 1 
),

resultado_final_filtrado as (
    select *,
        case
            when upper(transaction_type_filter) is not null
              and upper(transaction_type_filter) != upper(transaction_type)
            then 1
            else 0
        end as tx_type_mismatch
    from should_be_charged
),

resultado_final_ranked as (
    select *,
        row_number() over (
            partition by mm_id, matched_product_name, tx_type_mismatch
            order by local_created_at
        ) as rn
    from resultado_final_filtrado
),

fin as (
    select *
    from resultado_final_ranked
    where 
        upper(transaction_type) = 'PAYIN - OPEN_LINK'
        or (
            tx_type_mismatch = 0
            or (tx_type_mismatch = 1 and rn = 1)
        )
)

select {{ invoice_match_columns() }}
from fin
where upper(transaction_type) = 'PAYIN - OPEN_LINK'
qualify row_number() over (
    partition by mm_id, matched_product_name, local_created_at
    order by 
        case 
            when upper(transaction_type_filter) = upper(transaction_type) then 0
            else 1
        end,
        rn
) = 1

union all

select {{ invoice_match_columns() }}
from fin
where upper(transaction_type) != 'PAYIN - OPEN_LINK'
qualify row_number() over (
    partition by mm_id, matched_product_name
    order by 
        case 
            when upper(transaction_type_filter) = upper(transaction_type) then 0
            else 1
        end,
        rn
) = 1


union all

select {{ invoice_match_columns() }}
from platform_fee_always_charged

union all

select {{ invoice_match_columns() }}
from discount
