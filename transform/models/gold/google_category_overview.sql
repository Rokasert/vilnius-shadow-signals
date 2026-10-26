{{ config(tags=['google']) }}

-- How complete and how linkable Google's picture of each category is.
with places as (
    select pc.category, p.*
    from {{ ref('places') }} as p
    inner join {{ ref('place_categories') }} as pc on p.place_id = pc.place_id
    where p.is_in_vilnius
),

with_matches as (
    select
        p.*,
        m.company_code,
        m.confidence,
        m.methods,
        m.is_category_consistent,
        m.confidence in ('high', 'medium') as is_matched
    from places as p
    inner join {{ ref('place_company_matches') }} as m on p.place_id = m.place_id
),

registry as (
    select cc.category, sum(s.companies) as registry_companies
    from {{ ref('category_activity_classes') }} as cc
    inner join {{ ref('sector_profile') }} as s on cc.activity_class = s.activity_class
    where cc.is_core
    group by cc.category
),

found_in_registry as (
    select w.category, count(distinct w.company_code) as registry_companies_found
    from with_matches as w
    inner join {{ ref('companies') }} as c on w.company_code = c.company_code
    inner join {{ ref('category_activity_classes') }} as cc
        on w.category = cc.category and c.activity_class = cc.activity_class and cc.is_core
    where w.is_matched
    group by w.category
)

select
    w.category,
    count(*) as places,
    round(avg(is_operational::int), 3) as operational_share,
    round(avg((website is not null)::int), 3) as with_website_share,
    round(avg((weekly_open_hours is not null)::int), 3) as with_opening_hours_share,
    round(avg((phone is not null)::int), 3) as with_phone_share,
    quantile_disc(review_count, 0.25) as reviews_p25,
    quantile_disc(review_count, 0.5) as reviews_median,
    quantile_disc(review_count, 0.75) as reviews_p75,
    quantile_disc(review_count, 0.9) as reviews_p90,
    round(avg((review_count >= 100)::int), 3) as share_100_plus_reviews,
    round(median(rating), 2) as median_rating,
    round(median(weekly_open_hours), 1) as median_weekly_open_hours,
    round(avg((confidence = 'high')::int), 3) as high_match_share,
    round(avg((confidence = 'medium')::int), 3) as medium_match_share,
    round(avg((confidence = 'low')::int), 3) as low_match_share,
    round(avg((confidence = 'unmatched')::int), 3) as unmatched_share,
    round(avg(is_category_consistent::int) filter (where is_matched), 3)
        as matched_category_consistent_share,
    round(
        avg((list_contains(methods, 'website_code') or list_contains(methods, 'website_vat'))::int)
            filter (where is_matched),
        3
    ) as matched_with_website_evidence_share,
    count(distinct company_code) filter (where is_matched) as matched_companies,
    any_value(r.registry_companies) as registry_companies,
    any_value(f.registry_companies_found) as registry_companies_found,
    round(any_value(f.registry_companies_found) / nullif(any_value(r.registry_companies), 0), 3)
        as registry_found_share
from with_matches as w
left join registry as r on w.category = r.category
left join found_in_registry as f on w.category = f.category
group by w.category
