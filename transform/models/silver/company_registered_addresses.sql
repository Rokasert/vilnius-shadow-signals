-- Registered seat of each company. Only Vilnius addresses resolve to a street and number,
-- because only Vilnius buildings are ingested.
select
    c.company_code,
    r."adresas._id" as address_id,
    r.adresas_nuo as registered_here_since,
    a.address_id is not null as is_vilnius_address,
    a.address_kind,
    a.building_id,
    a.address_text,
    a.street_key,
    a.house_key,
    a.premise_no
from {{ source('bronze', 'jar__registered_addresses') }} as r
inner join {{ ref('companies') }} as c on r."juridinis_asmuo._id" = c.jar_record_id
left join {{ ref('addresses') }} as a on r."adresas._id" = a.address_id
