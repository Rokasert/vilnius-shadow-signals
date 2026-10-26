{{ config(tags=['google']) }}

-- The lead list: assessed companies in the priority or review tier, ranked, with what an
-- analyst needs to check each one: the Google listings and the evidence linking them to the
-- company, the declared figures, and the data runs they come from.
with leads as (
    select * from {{ ref('company_assessment') }} where tier in ('priority', 'review')
),

lead_places as (
    select company_code, category, unnest(place_ids) as place_id from leads
),

listings as (
    select
        lp.company_code,
        lp.category,
        list(p.name order by p.review_count desc) as place_names,
        list(p.formatted_address order by p.review_count desc) as place_addresses,
        list(p.google_maps_uri order by p.review_count desc) as google_maps_links,
        list(distinct m.evidence order by m.evidence) as match_evidence
    from lead_places as lp
    inner join {{ ref('places') }} as p on lp.place_id = p.place_id
    inner join {{ ref('place_company_matches') }} as m on lp.place_id = m.place_id
    group by lp.company_code, lp.category
)

select
    row_number() over (
        order by case l.tier when 'priority' then 1 else 2 end, l.score desc, l.reviews desc
    ) as rank,
    l.snapshot_month,
    l.tier,
    l.score,
    l.tier_reason,
    l.category,
    l.company_code,
    l.company_name,
    l.legal_form,
    l.reasons,
    l.context,
    l.places,
    l.reviews,
    l.open_hours_all,
    l.staff_declared,
    l.staffing_coverage,
    l.average_wage,
    l.wage_to_minimum,
    l.revenue_2024,
    l.is_vat_payer,
    l.overdue_tax_arrears,
    s.place_names,
    s.place_addresses,
    s.google_maps_links,
    s.match_evidence,
    l.place_ids,
    l.sodra_month,
    l.google_run_id,
    l.sodra_run_id
from leads as l
inner join listings as s on l.company_code = s.company_code and l.category = s.category
