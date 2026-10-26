{{ config(tags=['google']) }}

-- One row per category and month: how much of the category could be checked, and how much of
-- it was flagged. Answers "which categories have the highest share of flagged businesses?"
-- The steps from Google places to leads show where companies drop out, and why.
with registry as (
    select cc.category, count(distinct c.company_code) as registry_companies
    from {{ ref('companies') }} as c
    inner join {{ ref('category_activity_classes') }} as cc
        on c.activity_class = cc.activity_class and cc.is_core
    inner join {{ ref('company_registered_addresses') }} as a on c.company_code = a.company_code
    where c.is_registered and coalesce(c.is_operating, true) and a.is_vilnius_address
    group by cc.category
),

places as (
    select
        pc.category,
        count(*) as places,
        count(*) filter (where m.confidence = 'high') as places_linked
    from {{ ref('place_categories') }} as pc
    inner join {{ ref('place_company_matches') }} as m on pc.place_id = m.place_id
    group by pc.category
),

companies as (
    select
        snapshot_month,
        category,
        count(*) as companies_linked,
        count(*) filter (where tier <> 'not assessed') as companies_assessed,
        count(*) filter (where tier = 'not assessed') as companies_not_assessed,
        count(*) filter (where tier <> 'not assessed' and is_busy) as companies_busy,
        count(*) filter (where tier = 'priority') as priority_leads,
        count(*) filter (where tier = 'review') as review_leads
    from {{ ref('company_assessment') }}
    group by snapshot_month, category
)

select
    c.snapshot_month,
    c.category,
    r.registry_companies,
    p.places,
    p.places_linked,
    round(p.places_linked / p.places, 3) as places_linked_share,
    c.companies_linked,
    c.companies_assessed,
    c.companies_not_assessed,
    c.companies_busy,
    c.priority_leads,
    c.review_leads,
    c.priority_leads + c.review_leads as leads,
    round((c.priority_leads + c.review_leads) / nullif(c.companies_assessed, 0), 3)
        as lead_share_of_assessed,
    round((c.priority_leads + c.review_leads) / nullif(c.companies_busy, 0), 3)
        as lead_share_of_busy
from companies as c
inner join places as p on c.category = p.category
left join registry as r on c.category = r.category
