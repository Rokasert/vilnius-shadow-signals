-- Amounts VMI assessed in tax audits, aggregated by year and EVRK 2 activity (national).
select
    a.metai as year,
    e.kodas as activity_code,
    left(e.kodas, 2) as activity_division,
    e.pavadinimas_lt as activity_name,
    a.mm_skaicius as audited_payers,
    a.suma_pvm as assessed_vat,
    a.suma_pln as assessed_profit_tax,
    a.suma_gpm as assessed_income_tax,
    a.suma_akc as assessed_excise,
    a.suma_kiti as assessed_other,
    a.suma_viso as assessed_total
from {{ source('bronze', 'vmi__audit_assessments_by_sector') }} as a
left join {{ source('bronze', 'lsd__evrk_2') }} as e on a."evrk._id" = e._id
