{{ config(tags=['google']) }}

-- A lead may rest only on places linked to its company with high confidence.
with lead_places as (
    select company_code, unnest(place_ids) as place_id from {{ ref('leads') }}
)

select lp.company_code, lp.place_id, m.company_code as matched_company, m.confidence
from lead_places as lp
left join {{ ref('place_company_matches') }} as m on lp.place_id = m.place_id
where m.confidence is distinct from 'high' or m.company_code is distinct from lp.company_code
