-- Number of taxpayers VMI ran control actions on, by year, EVRK 2 activity and municipality.
select
    c.metai as year,
    e.kodas as activity_code,
    left(e.kodas, 2) as activity_division,
    e.pavadinimas_lt as activity_name,
    m.sav_kodas as municipality_code,
    m.pavadinimas as municipality,
    c.mm_skaicius as controlled_payers
from {{ source('bronze', 'vmi__control_actions_by_sector') }} as c
left join {{ source('bronze', 'lsd__evrk_2') }} as e on c."evrk._id" = e._id
left join {{ source('bronze', 'ar__municipalities') }} as m on c."savivaldybe._id" = m._id
