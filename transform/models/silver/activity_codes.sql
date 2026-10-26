select
    _id as source_id,
    '2.1' as version,
    kodas as code,
    lygmuo as level,
    pavadinimas_lt as name_lt,
    pavadinimas_en as name_en
from {{ source('bronze', 'lsd__evrk_2_1') }}

union all

select
    _id as source_id,
    '2' as version,
    kodas as code,
    case length(kodas) when 2 then 2 when 3 then 3 when 4 then 4 else 5 end as level,
    pavadinimas_lt as name_lt,
    pavadinimas_en as name_en
from {{ source('bronze', 'lsd__evrk_2') }}
