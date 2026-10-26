-- Business places a company declared to VMI ("Kavinė", "Autoservisas", ...), one row per
-- company and division number. The register repeats each division on every activity row.
with divisions as (
    select
        cast(ja_kodas as varchar) as company_code,
        padalinio_nr as division_no,
        arg_max(padalinio_pvd, suformuota) as division_type,
        arg_max(padalinio_savivaldybe, suformuota) as municipality_code,
        max(suformuota) as opened_on,
        arg_max(isformuota, suformuota) as closed_on
    from {{ source('bronze', 'vmi__taxpayers') }}
    where padalinio_nr is not null
    group by 1, 2
)

select
    d.company_code,
    d.division_no,
    d.division_type,
    {{ match_key('d.division_type') }} as division_type_key,
    d.municipality_code,
    m.pavadinimas as municipality,
    d.opened_on,
    d.closed_on,
    d.closed_on is null as is_open
from divisions as d
left join {{ source('bronze', 'ar__municipalities') }} as m on d.municipality_code = m.sav_kodas
