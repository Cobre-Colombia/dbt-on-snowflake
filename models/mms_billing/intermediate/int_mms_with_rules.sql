with rules as (
    select
        client_id,
        sequence_customer_id,
        group_id,
        product_name,
        price_structure_json,
        price_minimum_amount,
        consumes_saas,
        property_filters_json,
        parse_json(properties_to_negate) as properties_to_negate
    from {{ ref('stg_invoice_pricing') }}
    where price_structure_json is not null
),

negations as (
    select
        product_name,
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
    group by product_name
),

-- Flatten filters
flow_flat as (
    select product_name, upper(f.value::string) as flow_filter
    from rules, lateral flatten(input => property_filters_json:flow) f
),
tx_flat as (
    select product_name, upper(f.value::string) as transaction_type_filter
    from rules, lateral flatten(input => property_filters_json:transaction_type) f
),
origin_flat as (
    select product_name, upper(f.value::string) as origination_system_filter
    from rules, lateral flatten(input => property_filters_json:origination_system) f
),
source_flat as (
    select product_name, upper(f.value::string) as source_account_type_filter
    from rules, lateral flatten(input => property_filters_json:source_account_type) f
),
country_flat as (
    select product_name, upper(f.value::string) as country_filter
    from rules, lateral flatten(input => property_filters_json:country) f
),
origin_bank_flat as (
    select product_name, upper(f.value::string) as origin_bank_filter
    from rules, lateral flatten(input => property_filters_json:origin_bank) f
),
destination_bank_flat as (
    select product_name, upper(f.value::string) as destination_bank_filter
    from rules, lateral flatten(input => property_filters_json:destination_bank) f
),
status_flat as (
    select product_name, upper(f.value::string) as status_filter
    from rules, lateral flatten(input => property_filters_json:status) f
),

-- Expand rules with all properties
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
    left join negations nf on r.product_name = nf.product_name
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
    union all select * from {{ ref('stg_payin_mms') }}
    union all select * from {{ ref('stg_dac_mms') }}
    union all select * from {{ ref('stg_balance_recharges') }}
),

matched_raw as (
    select
        mm.*,
        rx.product_name as matched_product_name,
        rx.price_structure_json,
        rx.price_minimum_amount,
        rx.consumes_saas,
        rx.property_filters_json,
        rx.properties_to_negate,
        rx.sequence_customer_id,
        rx.group_id,

        rx.flow_filter,
        rx.transaction_type_filter,
        rx.origination_system_filter,
        rx.source_account_type_filter,
        rx.country_filter,
        rx.origin_bank_filter,
        rx.destination_bank_filter,
        rx.status_filter,

        (rx.negate_flow and upper(mm.flow) = rx.flow_filter) as negate_flow_match,
        (rx.negate_transaction_type and upper(mm.transaction_type) = rx.transaction_type_filter) as negate_transaction_type_match,
        (rx.negate_origination_system and upper(mm.origination_system) = rx.origination_system_filter) as negate_origination_system_match,
        (rx.negate_source_account_type and upper(mm.source_account_type) = rx.source_account_type_filter) as negate_source_account_type_match,
        (rx.negate_country and upper(mm.country) = rx.country_filter) as negate_country_match,
        (rx.negate_origin_bank and upper(mm.origin_bank) = rx.origin_bank_filter) as negate_origin_bank_match,
        (rx.negate_destination_bank and upper(mm.destination_bank) = rx.destination_bank_filter) as negate_destination_bank_match,
        (rx.negate_status and upper(mm.status) = rx.status_filter) as negate_status_match,

        (
            (rx.negate_flow and upper(mm.flow) = rx.flow_filter) or
            (rx.negate_transaction_type and upper(mm.transaction_type) = rx.transaction_type_filter) or
            (rx.negate_origination_system and upper(mm.origination_system) = rx.origination_system_filter) or
            (rx.negate_source_account_type and upper(mm.source_account_type) = rx.source_account_type_filter) or
            (rx.negate_country and upper(mm.country) = rx.country_filter) or
            (rx.negate_origin_bank and upper(mm.origin_bank) = rx.origin_bank_filter) or
            (rx.negate_destination_bank and upper(mm.destination_bank) = rx.destination_bank_filter) or
            (rx.negate_status and upper(mm.status) = rx.status_filter)
        ) as negate_applied,

        array_to_string(array_construct_compact(
            iff(upper(mm.flow) = rx.flow_filter, 'FLOW', null),
            iff(upper(mm.transaction_type) = rx.transaction_type_filter, 'TRANSACTION_TYPE', null),
            iff(upper(mm.origination_system) = rx.origination_system_filter, 'ORIGINATION_SYSTEM', null),
            iff(upper(mm.source_account_type) = rx.source_account_type_filter, 'SOURCE_ACCOUNT_TYPE', null),
            iff(upper(mm.country) = rx.country_filter, 'COUNTRY', null),
            iff(upper(mm.origin_bank) = rx.origin_bank_filter, 'ORIGIN_BANK', null),
            iff(upper(mm.destination_bank) = rx.destination_bank_filter, 'DESTINATION_BANK', null),
            iff(upper(mm.status) = rx.status_filter, 'STATUS', null)
        ), ', ') as match_reason
    from mm
    join rules_expanded rx
      on mm.client_id = rx.client_id
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
),
matched_with_priority as (
    select *,
 case
  when (flow_filter is null or upper(flow) = flow_filter) then
    case
      when (transaction_type_filter is null or upper(transaction_type) = transaction_type_filter) then
        case
          when (origination_system_filter is null or upper(origination_system) = origination_system_filter) then
            case
              when (source_account_type_filter is null or upper(source_account_type) = source_account_type_filter) then
                case
                  when (country_filter is null or upper(country) = country_filter) then
                    case
                      when (origin_bank_filter is null or upper(origin_bank) = origin_bank_filter) then
                        case
                          when (destination_bank_filter is null or upper(destination_bank) = destination_bank_filter) then
                            case
                              when (status_filter is null or upper(status) = status_filter) then 8
                              else 7
                            end
                          else 6
                        end
                      else 5
                    end
                  else 4
                end
              else 3
            end
          else 2
        end
      else 1
    end
  else 0
end as sequential_match_score

    from matched_raw
),

ranked as (
    select *,
        rank() over (partition by mm_id order by sequential_match_score desc) as rank_by_seq
    from matched_with_priority
)
,
resultado_final as (
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
    from resultado_final
),
resultado_final_ranked as (
    select *,
        row_number() over (
            partition by matched_product_name, tx_type_mismatch
            order by eventtimestamp
        ) as rn
    from resultado_final_filtrado
)
select distinct *
from resultado_final_ranked
where 
    tx_type_mismatch = 0
    or (tx_type_mismatch = 1 and rn = 1)