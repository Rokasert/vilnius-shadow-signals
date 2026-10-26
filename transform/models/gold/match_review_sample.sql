{{ config(tags=['google']) }}

-- Matches to check by hand: 20 per confidence level plus 10 unmatched places, picked
-- pseudo-randomly but the same on every run. is_correct is left for the reviewer.
with ranked as (
    select
        *,
        row_number() over (partition by confidence order by hash(place_id)) as pick
    from {{ ref('place_company_matches') }}
)

select
    r.confidence,
    r.pick,
    p.place_id,
    p.name as place_name,
    p.formatted_address,
    array_to_string(p.categories, ', ') as categories,
    p.google_maps_uri,
    p.website,
    p.review_count,
    r.company_code,
    c.name as company_name,
    a.address_text as company_registered_address,
    c.activity_name as company_activity,
    r.evidence,
    array_to_string(r.methods, ', ') as methods,
    r.score,
    r.margin,
    r.name_similarity,
    r.is_category_consistent,
    null::boolean as is_correct
from ranked as r
inner join {{ ref('places') }} as p on r.place_id = p.place_id
left join {{ ref('companies') }} as c on r.company_code = c.company_code
left join {{ ref('company_registered_addresses') }} as a on r.company_code = a.company_code
where r.pick <= case when r.confidence = 'unmatched' then 10 else 20 end
