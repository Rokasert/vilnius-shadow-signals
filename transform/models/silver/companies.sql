with forms as (
    select _id, kodas, pavadinimas, tipas
    from {{ source('bronze', 'jar__legal_forms') }}
),

statuses as (
    select _id, kodas, pavadinimas
    from {{ source('bronze', 'jar__legal_statuses') }}
),

activities as (
    select
        cast(a.kodas as varchar) as company_code,
        c.kodas as activity_code,
        c.pavadinimas_lt as activity_name,
        a.evrk_2_1_data as activity_since,
        coalesce(a.veikiantis, 0) = 1 as is_operating,
        a.nuosavybes_forma_pavadinimas as ownership
    from {{ source('bronze', 'lsd__legal_entity_activities') }} as a
    left join {{ source('bronze', 'lsd__evrk_2_1') }} as c on a."evrk_2_1._id" = c._id
    qualify row_number() over (
        partition by a.kodas order by a.evrk_2_1_data desc nulls last, a._id
    ) = 1
)

select
    cast(e.ja_kodas as varchar) as company_code,
    e._id as jar_record_id,
    e.ja_pavadinimas as name,
    {{ name_key('e.ja_pavadinimas') }} as name_key,
    -- open data withholds the names of sole proprietors (IĮ): the name is just the legal form
    coalesce(e.ja_pavadinimas = f.pavadinimas, false) as is_name_hidden,
    f.kodas as legal_form_code,
    f.pavadinimas as legal_form,
    f.tipas as legal_form_sector,
    coalesce(f.pavadinimas in (
        'Mažoji bendrija',
        'Individuali įmonė',
        'Tikroji ūkinė bendrija',
        'Komanditinė ūkinė bendrija'
    ), false) as is_owner_run_form,
    s.kodas as status_code,
    s.pavadinimas as status,
    e.stat_data as status_since,
    e.reg_data as registered_on,
    e.isreg_data as deregistered_on,
    e.isreg_data is null as is_registered,
    a.activity_code,
    left(a.activity_code, 4) as activity_class,
    left(a.activity_code, 2) as activity_division,
    a.activity_name,
    a.activity_since,
    a.is_operating,
    a.ownership,
    e._run_id as source_run_id
from {{ source('bronze', 'jar__legal_entities') }} as e
left join forms as f on e."forma._id" = f._id
left join statuses as s on e."statusas._id" = s._id
left join activities as a on cast(e.ja_kodas as varchar) = a.company_code
