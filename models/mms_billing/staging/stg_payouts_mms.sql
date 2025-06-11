select
    *,
    upper(transfer_method) as transfer_method_upper,
    upper(transaction_type) as transaction_type_upper,
    upper(origination_system) as origination_system_upper,
    upper(source_account_type) as source_account_type_upper
from {{ ref('payouts_mms') }}
