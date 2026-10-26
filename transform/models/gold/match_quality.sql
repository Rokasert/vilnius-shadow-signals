{{ config(tags=['google']) }}

-- Do the links between Google places and companies make sense? One row per category and per
-- method that contributed to the chosen match.
with best as (
    select pc.category, m.*, unnest(m.methods) as method
    from {{ ref('place_company_matches') }} as m
    inner join {{ ref('place_categories') }} as pc on m.place_id = pc.place_id
    where m.company_code is not null
),

chains as (
    select company_code, count(*) as places
    from {{ ref('place_company_matches') }}
    where confidence in ('high', 'medium')
    group by company_code
),

-- the website names a company, yet a different company was chosen
website_disagreement as (
    select pc.category, count(distinct w.place_id) as places
    from {{ ref('website_company_codes') }} as w
    inner join {{ ref('place_company_matches') }} as m on w.place_id = m.place_id
    inner join {{ ref('place_categories') }} as pc on w.place_id = pc.place_id
    where not exists (
        select 1
        from {{ ref('website_company_codes') }} as w2
        where w2.place_id = w.place_id and w2.company_code = m.company_code
    )
    group by pc.category
)

select
    b.category,
    b.method,
    count(*) as matched_places,
    round(avg((b.confidence = 'high')::int), 3) as high_share,
    round(avg((b.confidence in ('high', 'medium'))::int), 3) as high_or_medium_share,
    round(avg(b.is_category_consistent::int), 3) as category_consistent_share,
    round(median(b.name_similarity), 3) as median_name_similarity,
    round(avg((len(b.methods) >= 2)::int), 3) as corroborated_share,
    round(avg((coalesce(ch.places, 0) >= 3)::int), 3) as chain_share,
    case
        when b.method like 'website%' then coalesce(any_value(wd.places), 0)
    end as places_where_website_disagrees
from best as b
left join chains as ch on b.company_code = ch.company_code
left join website_disagreement as wd on b.category = wd.category
group by b.category, b.method
