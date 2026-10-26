-- Foreign taxpayers carry 10-digit VMI codes instead of a JAR company code; they keep
-- `taxpayer_code` but get no `company_code`.
select
    cast(ja_kodas as varchar) as taxpayer_code,
    case when length(cast(ja_kodas as varchar)) = 9 then cast(ja_kodas as varchar) end
        as company_code,
    ja_pavadinimas as name,
    "sav_kodas.sav_kodas" as municipality_code,
    nepriemoka as arrears,
    pradelsta_nepriemoka as overdue_arrears,
    atideta_nepriemoka as deferred_arrears,
    atideta_pradelsta as deferred_overdue_arrears,
    sukurta as first_recorded_on,
    atnaujinta as updated_on
from {{ source('bronze', 'vmi__tax_arrears') }}
