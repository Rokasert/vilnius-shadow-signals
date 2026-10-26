select
    darbo_vietos_id as vacancy_id,
    cast(jar_kodas as varchar) as employer_code,
    case
        when regexp_full_match(cast(jar_kodas as varchar), '\d{9}') then cast(jar_kodas as varchar)
    end as company_code,
    darbdavys as employer_name,
    statusas as status,
    ikelimo_data as posted_on,
    galioja_nuo as valid_from,
    galioja_iki as valid_to,
    ar_aktuali_siandien = 1 as is_open_today,
    darbo_vietu_skaicius as positions,
    vid_darbo_uzmokestis as wage_offered,
    -- about 4k ads give an hourly rate; the preliminary wage holds its monthly equivalent
    vid_darbo_uzmokestis < 30 as is_hourly_rate,
    case
        when vid_darbo_uzmokestis < 30 then coalesce(prelim_darbo_uzmokestis, vid_darbo_uzmokestis * 168)
        else vid_darbo_uzmokestis
    end as wage_offered_monthly,
    prelim_darbo_uzmokestis as wage_preliminary,
    maks_darbo_uzmokestis as wage_max,
    valiuta as currency,
    profesijos_kodas as occupation_code,
    profesijos_pareigybes_pav as occupation,
    profesijos_grupes_pav as occupation_group,
    kontrakto_tipas as contract_type,
    darbo_vietos_adresas as workplace_address,
    {{ street_key_from_address('darbo_vietos_adresas') }} as workplace_street_key,
    {{ house_key_from_address('darbo_vietos_adresas') }} as workplace_house_key,
    darbdavio_bustine as employer_seat_address
from {{ source('bronze', 'uzt__job_vacancies_vilnius') }}
