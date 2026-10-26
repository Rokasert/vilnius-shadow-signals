-- VAT returns aggregated by VMI per month, county and EVRK 2 division (no company detail).
select
    mokestinis_laikotarpis as period_month,
    left(sekcija_pagal_ekonomines_veiklos_rusiu_klasifikatoriu, 1) as activity_section,
    regexp_extract(skyrius_pagal_ekonomines_veiklos_rusiu_klasifikatoriu, '^[A-Z](\d{2})', 1)
        as activity_division,
    skyrius_pagal_ekonomines_veiklos_rusiu_klasifikatoriu as activity_division_name,
    apskritis as county,
    mokesciu_moketoju_skaicius as vat_payers,
    pvm_apmokestinami_sandoriai as taxable_sales,
    pvm_neapmokestinami_sandoriai as exempt_sales,
    standartinio_tarifo_pardavimo_pvm as output_vat_standard_rate,
    procentu_9_pardavimo_pvm as output_vat_9_percent,
    procentu_5_pardavimo_pvm as output_vat_5_percent,
    isigytu_prekiu_ir_paslaugu_pirkmo_pvm as input_vat,
    moketinas_i_biudzeta_arba_grazintinas_is_biudzeto_pvm as vat_payable
from {{ source('bronze', 'vmi__vat_returns_by_sector') }}
