-- One row per taxpayer. The VMI register repeats VAT fields on every activity row;
-- where they differ (a re-registration), the latest registration wins.
with latest as (
    select
        cast(ja_kodas as varchar) as company_code,
        arg_max(pavadinimas, coalesce(pvm_iregistruota, date '1900-01-01')) as name,
        arg_max(tipo_aprasymas, coalesce(pvm_iregistruota, date '1900-01-01')) as taxpayer_type,
        arg_max(
            pvm_kodas_pref || cast(pvm_kodas as varchar),
            coalesce(pvm_iregistruota, date '1900-01-01')
        ) as vat_code,
        max(pvm_iregistruota) as vat_registered_on,
        arg_max(pvm_isregistruota, coalesce(pvm_iregistruota, date '1900-01-01'))
            as vat_deregistered_on,
        max(ireg_data) as taxpayer_registered_on,
        max(isreg_data) as taxpayer_deregistered_on
    from {{ source('bronze', 'vmi__taxpayers') }}
    group by 1
)

select
    *,
    vat_code is not null and vat_deregistered_on is null as is_vat_payer
from latest
