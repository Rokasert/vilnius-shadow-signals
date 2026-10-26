{{ config(tags=['google']) }}

-- One row per company and category it was found in on Google (high or medium confidence
-- matches only): what Google shows next to what the company declares.
with latest_month as (
    select max(month) as month from {{ ref('employer_months') }}
),

minimum_wage as (
    select max(monthly_eur) filter (where year = (select year(month) from latest_month)) as latest
    from {{ ref('minimum_wage') }}
),

confident as (
    select m.company_code, m.place_id, m.confidence
    from {{ ref('place_company_matches') }} as m
    inner join {{ ref('places') }} as p on m.place_id = p.place_id
    where m.confidence in ('high', 'medium') and p.is_operational
),

in_category as (
    select
        c.company_code,
        pc.category,
        count(*) as places,
        sum(p.review_count) as reviews,
        avg(p.rating) as avg_rating,
        max(p.weekly_open_hours) as max_weekly_open_hours,
        bool_and(c.confidence = 'high') as all_matches_high
    from confident as c
    inner join {{ ref('places') }} as p on c.place_id = p.place_id
    inner join {{ ref('place_categories') }} as pc on c.place_id = pc.place_id
    group by c.company_code, pc.category
),

-- all of the company's places, whatever category: staff serve all of them
whole_company as (
    select
        c.company_code,
        count(*) as places_total,
        sum(p.review_count) as reviews_total,
        sum(p.weekly_open_hours) as open_hours_total
    from confident as c
    inner join {{ ref('places') }} as p on c.place_id = p.place_id
    group by c.company_code
),

declared as (
    select company_code, sum(insured) as staff, max(average_wage) as average_wage
    from {{ ref('employer_months') }}
    where month = (select month from latest_month) and company_code is not null
    group by company_code
),

revenue_2024 as (
    select company_code, arg_max(revenue, period_end) as revenue
    from {{ ref('company_financials') }}
    where fiscal_year = 2024
    group by company_code
),

base as (
    select
        i.company_code,
        co.name as company_name,
        i.category,
        co.activity_class,
        co.legal_form,
        co.is_owner_run_form,
        i.places,
        i.reviews,
        round(i.avg_rating, 2) as avg_rating,
        i.max_weekly_open_hours,
        i.all_matches_high,
        w.places_total,
        w.reviews_total,
        w.open_hours_total,
        coalesce(d.staff, 0) as staff,
        d.average_wage,
        round(d.average_wage / mw.latest, 2) as wage_to_minimum,
        -- weekly hours the declared staff can legally cover (40 h each) against the hours the
        -- company's places are open with at least one person on duty
        round(coalesce(d.staff, 0) * 40.0 / nullif(w.open_hours_total, 0), 2) as staffing_coverage,
        w.reviews_total / greatest(coalesce(d.staff, 0), 1) as reviews_per_staff,
        r.revenue as revenue_2024,
        round(r.revenue / nullif(w.reviews_total, 0), 1) as revenue_per_review,
        coalesce(v.is_vat_payer, false) as is_vat_payer,
        coalesce(a.overdue_arrears, 0) > 0 as has_overdue_arrears
    from in_category as i
    inner join whole_company as w on i.company_code = w.company_code
    inner join {{ ref('companies') }} as co on i.company_code = co.company_code
    cross join minimum_wage as mw
    left join declared as d on i.company_code = d.company_code
    left join revenue_2024 as r on i.company_code = r.company_code
    left join {{ ref('company_vat') }} as v on i.company_code = v.company_code
    left join {{ ref('company_tax_arrears') }} as a on i.company_code = a.company_code
),

-- peer comparison inside each category with medians, so a few extreme companies don't skew it
peers as (
    select
        category,
        median(ln(1 + reviews_per_staff)) as reviews_per_staff_median,
        mad(ln(1 + reviews_per_staff)) as reviews_per_staff_mad,
        median(staffing_coverage) as coverage_median,
        mad(staffing_coverage) as coverage_mad
    from base
    group by category
)

select
    b.*,
    round(
        (ln(1 + b.reviews_per_staff) - p.reviews_per_staff_median)
        / nullif(1.4826 * p.reviews_per_staff_mad, 0),
        2
    ) as reviews_per_staff_z,
    round(
        (b.staffing_coverage - p.coverage_median) / nullif(1.4826 * p.coverage_mad, 0), 2
    ) as staffing_coverage_z
from base as b
inner join peers as p on b.category = p.category
