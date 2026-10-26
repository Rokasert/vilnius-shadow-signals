with source as (
    select
        "Draudėjo kodas (code)" as insurer_code,
        "Juridinių asmenų registro kodas (jarCode)" as jar_code,
        "Pavadinimas (name)" as name,
        "Savivaldybė, kurioje registruota(municipality)" as municipality,
        "Mėnuo (month)" as yyyymm,
        "Ekonominės veiklos rūšies kodas(ecoActCode)" as activity_code,
        "Ekonominės veiklos rūšies pavadinimas(ecoActName)" as activity_name,
        "Apdraustųjų skaičius (numInsured)" as insured,
        "Vidutinis darbo užmokestis (avgWage)" as average_wage,
        "Valstybinio socialinio draudimo įmoka (tax)" as contributions,
        "Apdraustųjų skaičius II (numInsured2)" as insured_other_scheme,
        "Vidutinis darbo užmokestis II (avgWage2)" as average_wage_other_scheme,
        _file,
        _run_id
    from {{ source('bronze', 'sodra__employers_monthly') }}
)

select
    insurer_code,
    cast(jar_code as varchar) as company_code,
    name,
    municipality,
    make_date(cast(yyyymm // 100 as integer), cast(yyyymm % 100 as integer), 1) as month,
    -- Sodra switched to EVRK 2.1 codes from January 2026
    case
        when activity_code is null then null
        when yyyymm >= 202601 then '2.1'
        else '2'
    end as activity_code_version,
    lpad(cast(activity_code as varchar), 6, '0') as activity_code,
    activity_name,
    insured,
    average_wage,
    contributions,
    -- Sodra publishes wage and contributions only for employers with 4 or more insured
    insured between 1 and 3 as is_wage_suppressed,
    insured_other_scheme,
    average_wage_other_scheme,
    _file as source_file,
    _run_id as source_run_id
from source
