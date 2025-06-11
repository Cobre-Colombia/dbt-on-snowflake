select
    *,
    {{ parse_json_column('PROPERTY_FILTERS') }} as property_filters_json,
    {{ parse_json_column('PRICE_STRUCTURE') }} as price_structure_json
from {{ ref('invoice_pricing') }}
