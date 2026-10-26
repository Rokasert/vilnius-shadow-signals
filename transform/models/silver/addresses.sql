-- Vilnius addresses from the address register: buildings, plus premises (flats, offices)
-- inside them. address_id is the register's address record, which JAR seats point to.
with streets as (
    select _id, pavadinimas as street_name, tipo_santrumpa as street_type
    from {{ source('bronze', 'ar__streets') }}
),

buildings as (
    select
        b."aob_kodas._id" as address_id,
        b._id as building_id,
        s.street_name,
        s.street_type,
        b.nr as house_no,
        b.korpuso_nr as block_no,
        b.pasto_kodas as postcode
    from {{ source('bronze', 'ar__buildings_vilnius') }} as b
    left join streets as s on b."gatve._id" = s._id
),

premises as (
    select
        p."aob_kodas._id" as address_id,
        b.building_id,
        b.street_name,
        b.street_type,
        b.house_no,
        b.block_no,
        b.postcode,
        cast(p.patalpos_nr as varchar) as premise_no
    from {{ source('bronze', 'ar__premises') }} as p
    inner join buildings as b on p."pastatas._id" = b.building_id
),

combined as (
    select *, 'building' as address_kind, null as premise_no from buildings
    union all by name
    select *, 'premise' as address_kind from premises
)

select
    address_id,
    address_kind,
    building_id,
    street_name,
    street_type,
    house_no,
    block_no,
    premise_no,
    postcode,
    concat_ws(' ', street_name, street_type)
    || ' ' || house_no
    || coalesce(' k. ' || block_no, '')
    || coalesce('-' || premise_no, '')
    || ', Vilnius' as address_text,
    {{ match_key('street_name') }} as street_key,
    {{ house_key('house_no') }} as house_key
from combined
