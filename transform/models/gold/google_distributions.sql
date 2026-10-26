{{ config(tags=['google']) }}

-- Histograms per category, to see how Google's numbers and the joined declared numbers are
-- spread out rather than only their averages.
with places as (
    select pc.category, p.review_count, p.rating, p.weekly_open_hours, m.confidence
    from {{ ref('places') }} as p
    inner join {{ ref('place_categories') }} as pc on p.place_id = pc.place_id
    inner join {{ ref('place_company_matches') }} as m on p.place_id = m.place_id
    where p.is_in_vilnius
),

companies as (
    select * from {{ ref('company_category_signals') }}
),

bucketed as (
    select category, 'reviews per place' as metric,
        case
            when review_count = 0 then '1: none'
            when review_count < 10 then '2: 1-9'
            when review_count < 50 then '3: 10-49'
            when review_count < 200 then '4: 50-199'
            when review_count < 500 then '5: 200-499'
            when review_count < 1000 then '6: 500-999'
            else '7: 1000+'
        end as bucket
    from places

    union all
    select category, 'rating',
        case
            when rating is null then '0: no rating'
            when rating < 3.5 then '1: under 3.5'
            when rating < 4.0 then '2: 3.5-3.9'
            when rating < 4.5 then '3: 4.0-4.4'
            when rating < 4.8 then '4: 4.5-4.7'
            else '5: 4.8-5.0'
        end
    from places

    union all
    select category, 'weekly opening hours',
        case
            when weekly_open_hours is null then '0: unknown'
            when weekly_open_hours < 40 then '1: under 40'
            when weekly_open_hours < 60 then '2: 40-59'
            when weekly_open_hours < 80 then '3: 60-79'
            when weekly_open_hours < 100 then '4: 80-99'
            else '5: 100+'
        end
    from places

    union all
    select category, 'match confidence',
        case confidence
            when 'high' then '1: high'
            when 'medium' then '2: medium'
            when 'low' then '3: low'
            else '4: unmatched'
        end
    from places

    union all
    select category, 'staff of matched company',
        case
            when staff = 0 then '1: none'
            when staff <= 3 then '2: 1-3'
            when staff <= 9 then '3: 4-9'
            when staff <= 49 then '4: 10-49'
            else '5: 50+'
        end
    from companies

    union all
    select category, 'staffing coverage',
        case
            when staffing_coverage is null then '0: unknown hours'
            when staffing_coverage < 0.25 then '1: under 0.25'
            when staffing_coverage < 0.5 then '2: 0.25-0.49'
            when staffing_coverage < 1 then '3: 0.5-0.99'
            when staffing_coverage < 2 then '4: 1-1.99'
            else '5: 2+'
        end
    from companies

    union all
    select category, 'reviews per staff member',
        case
            when reviews_per_staff < 10 then '1: under 10'
            when reviews_per_staff < 50 then '2: 10-49'
            when reviews_per_staff < 200 then '3: 50-199'
            when reviews_per_staff < 500 then '4: 200-499'
            else '5: 500+'
        end
    from companies
)

select
    category,
    metric,
    bucket,
    count(*) as n,
    round(count(*) / sum(count(*)) over (partition by category, metric), 3) as share
from bucketed
group by category, metric, bucket
