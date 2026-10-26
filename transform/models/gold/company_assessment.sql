{{ config(tags=['google']) }}

-- One row per company and category it runs Google-visible places in: what Google shows, what
-- the company declares, the signals that compare the two, and the resulting lead tier.
-- Only high-confidence links to operating places count. Owner-run and non-profit forms are
-- listed but not scored: their owners or volunteers need not be insured employees.
-- the month of this assessment: the newest of the Google and Sodra runs it uses (a monthly
-- refresh may reuse last month's Google data)
with google_run as (
    select
        greatest(
            max(_snapshot_month),
            (select max(_snapshot_month) from {{ source('bronze', 'sodra__employers_monthly') }})
        ) as snapshot_month,
        max(_run_id) as run_id
    from {{ source('bronze', 'google__places') }}
),

sodra as (
    select max(month) as month, max(source_run_id) as run_id from {{ ref('employer_months') }}
),

minimum_wage as (
    select monthly_eur
    from {{ ref('minimum_wage') }}
    where year = (select year(month) from sodra)
),

linked_places as (
    select m.company_code, p.*
    from {{ ref('place_company_matches') }} as m
    inner join {{ ref('places') }} as p on m.place_id = p.place_id
    where m.confidence = 'high' and p.is_operational
),

-- all of a company's places, whatever the category: its staff serve all of them. A "24/7"
-- listing usually means a phone or online service, so its hours are left out of the check.
company_places as (
    select
        company_code,
        count(*) as places_all,
        sum(review_count) as reviews_all,
        sum(weekly_open_hours) filter (where not coalesce(is_open_24_7, false)) as open_hours_all,
        count(weekly_open_hours) filter (where not coalesce(is_open_24_7, false))
            as places_with_hours,
        bool_or(coalesce(is_open_24_7, false)) as any_open_24_7
    from linked_places
    group by company_code
),

category_places as (
    select
        l.company_code,
        pc.category,
        count(*) as places,
        sum(l.review_count) as reviews,
        list(l.place_id order by l.review_count desc) as place_ids
    from linked_places as l
    inner join {{ ref('place_categories') }} as pc on l.place_id = pc.place_id
    group by l.company_code, pc.category
),

-- Sodra counts insured people (part-timers count as one each). The higher of the latest month
-- and the 12-month average is used, so a temporary dip doesn't create a lead.
staff as (
    select
        company_code,
        sum(insured) filter (where month = (select month from sodra)) as staff_latest,
        sum(insured) filter (where month > (select month - interval 12 month from sodra)) / 12.0
            as staff_avg_12m,
        max(average_wage) filter (where month = (select month from sodra)) as average_wage
    from {{ ref('employer_months') }}
    where company_code is not null
    group by company_code
),

revenue as (
    select company_code, arg_max(revenue, period_end) as revenue_2024
    from {{ ref('company_financials') }}
    where fiscal_year = 2024
    group by company_code
),

missing_2024 as (
    select distinct company_code
    from {{ ref('company_filing_gaps') }}
    where gap_type = 'missing' and missing_year = 2024
),

job_ads as (
    select company_code, count(*) as job_ads, median(wage_offered_monthly) as job_ad_wage
    from {{ ref('job_vacancies') }}
    where company_code is not null and wage_offered_monthly > 0
    group by company_code
),

-- is the company behind the places known beyond its name: its own website or an analyst says so
identity as (
    select
        cp.company_code,
        cp.category,
        bool_or(m.evidence in ('website', 'checked by an analyst')) as identity_checked
    from (select company_code, category, unnest(place_ids) as place_id from category_places) as cp
    inner join {{ ref('place_company_matches') }} as m on cp.place_id = m.place_id
    group by cp.company_code, cp.category
),

-- other active companies of the same trade registered at one of the places' addresses: one of
-- them may run the place, or employ its staff (common in restaurant groups)
neighbours as (
    select
        cp.company_code,
        cp.category,
        list(distinct format('{} ({} insured staff)', c.name, s.staff_latest)
            order by format('{} ({} insured staff)', c.name, s.staff_latest)) as names
    from (select company_code, category, unnest(place_ids) as place_id from category_places) as cp
    inner join {{ ref('place_company_candidates') }} as k
        on
            cp.place_id = k.place_id
            and k.method = 'registered_address'
            and k.is_category_consistent
            and k.company_code <> cp.company_code
    inner join {{ ref('companies') }} as c on k.company_code = c.company_code
    inner join staff as s on k.company_code = s.company_code
    where s.staff_latest > 0
    group by cp.company_code, cp.category
),

facts as (
    select
        cp.category,
        cp.company_code,
        c.name as company_name,
        c.legal_form,
        c.registered_on,
        case
            when c.is_owner_run_form then 'owner-run legal form: owners need not be insured employees'
            when c.legal_form not in ('Uždaroji akcinė bendrovė', 'Akcinė bendrovė')
                then 'non-profit or other legal form: may rely on volunteers or members'
        end as exclusion_reason,
        cp.places,
        cp.reviews,
        cp.place_ids,
        a.places_all,
        a.reviews_all,
        a.open_hours_all,
        a.places_with_hours,
        a.any_open_24_7,
        coalesce(s.staff_latest, 0) as staff_latest,
        round(coalesce(s.staff_avg_12m, 0), 1) as staff_avg_12m,
        greatest(coalesce(s.staff_latest, 0), coalesce(s.staff_avg_12m, 0)) as staff_declared,
        s.average_wage,
        round(s.average_wage / mw.monthly_eur, 2) as wage_to_minimum,
        r.revenue_2024,
        m.company_code is not null as missing_accounts_2024,
        coalesce(v.is_vat_payer, false) as is_vat_payer,
        v.vat_deregistered_on,
        -- VAT-registered at any time since 2024: otherwise it declared turnover under €45,000
        coalesce(
            v.vat_registered_on is not null
            and (v.vat_deregistered_on is null or v.vat_deregistered_on >= date '2024-01-01'),
            false
        ) as vat_registered_since_2024,
        coalesce(t.overdue_arrears, 0) as overdue_tax_arrears,
        j.job_ads,
        j.job_ad_wage,
        coalesce(i.identity_checked, false) as identity_checked,
        n.names as same_trade_neighbours
    from category_places as cp
    inner join company_places as a on cp.company_code = a.company_code
    inner join {{ ref('companies') }} as c on cp.company_code = c.company_code
    cross join minimum_wage as mw
    left join staff as s on cp.company_code = s.company_code
    left join revenue as r on cp.company_code = r.company_code
    left join missing_2024 as m on cp.company_code = m.company_code
    left join {{ ref('company_vat') }} as v on cp.company_code = v.company_code
    left join {{ ref('company_tax_arrears') }} as t on cp.company_code = t.company_code
    left join job_ads as j on cp.company_code = j.company_code
    left join identity as i on cp.company_code = i.company_code and cp.category = i.category
    left join neighbours as n on cp.company_code = n.company_code and cp.category = n.category
),

ratios as (
    select
        *,
        -- for the reason texts: '1.0' -> '1', no insured staff -> 'no'
        case
            when staff_declared = 0 then 'no'
            else regexp_replace(cast(round(staff_declared, 1) as varchar), '\.0$', '')
        end as staff_text,
        -- rough yearly wage bill: declared average wage × staff over the last 12 months
        average_wage * staff_avg_12m * 12 as wage_bill_estimate,
        -- weekly hours the declared staff can legally work (40 h each) against the hours the
        -- company's places are open with at least one person on duty
        round(staff_declared * 40 / nullif(open_hours_all, 0), 2) as staffing_coverage,
        round(reviews_all / greatest(staff_declared, 1), 1) as reviews_per_staff,
        round(revenue_2024 / nullif(reviews_all, 0), 1) as revenue_per_review
    from facts
),

-- peers: assessed companies of the same category
peers as (
    select
        category,
        median(reviews) as busy_threshold,
        quantile_cont(reviews_per_staff, 0.9) as reviews_per_staff_p90,
        quantile_cont(revenue_per_review, 0.1) as revenue_per_review_p10
    from ratios
    where exclusion_reason is null
    group by category
),

signals as (
    select
        r.*,
        p.busy_threshold,
        p.reviews_per_staff_p90,
        p.revenue_per_review_p10,
        r.reviews >= p.busy_threshold as is_busy,
        coalesce(r.staffing_coverage < 1, false) as staff_gap,
        coalesce(r.staffing_coverage < 0.5, false) as staff_gap_severe,
        coalesce(
            r.reviews_per_staff >= p.reviews_per_staff_p90
            or r.revenue_per_review <= p.revenue_per_review_p10,
            false
        ) as activity_outruns_size,
        coalesce(r.wage_to_minimum < 0.8, false) as low_declared_wage,
        r.missing_accounts_2024 and r.registered_on < date '2024-01-01' as accounts_missing,
        not r.vat_registered_since_2024 as not_vat_registered
    from ratios as r
    inner join peers as p on r.category = p.category
),

scored as (
    select
        *,
        2 * staff_gap::int
        + staff_gap_severe::int
        + activity_outruns_size::int
        + low_declared_wage::int
        + accounts_missing::int
        + not_vat_registered::int as score
    from signals
)

select
    (select snapshot_month from google_run) as snapshot_month,
    category,
    company_code,
    company_name,
    legal_form,
    -- priority needs a staffing gap plus support, and a company link that doesn't rest on the
    -- name alone while another same-trade company is registered at the place
    case
        when exclusion_reason is not null then 'not assessed'
        when not is_busy then 'none'
        when
            staff_gap and score >= 3
            and (identity_checked or same_trade_neighbours is null)
            then 'priority'
        when score >= 2 then 'review'
        else 'none'
    end as tier,
    -- total points of all signals; the tier also needs the company to be busy and scored
    score,
    case
        when exclusion_reason is not null then 'Not scored: ' || exclusion_reason
        when not is_busy then format(
            'Not a lead: not busy ({} reviews; busy from {})',
            reviews, cast(ceil(busy_threshold) as bigint)
        )
        when staff_gap and score >= 3 and (identity_checked or same_trade_neighbours is null)
            then format('Priority: staff cannot cover the opening hours, and {} points', score)
        when staff_gap and score >= 3 then format(
            'Review, not priority: {} points with a staffing gap, but the company was found by '
            || 'name and another company of the same trade is registered at the address',
            score
        )
        when score >= 2 then format('Review: {} points (a lead needs 2 or more)', score)
        else format('Not a lead: {} point(s), a lead needs 2', score)
    end as tier_reason,
    exclusion_reason,
    is_busy,
    list_filter([
        case when staff_gap and staff_declared = 0 then format(
            'no insured staff, but the places are open {} h a week',
            cast(round(open_hours_all) as bigint)
        ) when staff_gap then format(
            '{} insured staff can work at most {} h a week, but the places are open {} h a week '
            || '(coverage {})',
            staff_text, cast(round(staff_declared * 40) as bigint),
            cast(round(open_hours_all) as bigint), staffing_coverage
        ) end,
        case when activity_outruns_size then format(
            '{} Google reviews for {} insured staff{}: more visible activity per declared worker '
            || 'or euro than 90% of peers',
            reviews_all, staff_text,
            case
                when revenue_2024 is not null
                    then format(' and €{} revenue in 2024', cast(round(revenue_2024) as bigint))
                else ''
            end
        ) end,
        case when low_declared_wage then format(
            'average declared wage €{} is {} of the minimum wage: staff are mostly paid as part-time',
            cast(round(average_wage) as bigint), format('{}%', cast(round(wage_to_minimum * 100) as bigint))
        ) end,
        case when accounts_missing then 'annual accounts for 2024 not filed' end,
        case when not_vat_registered and revenue_2024 > 45000 then format(
            '2024 revenue €{} is above the €45,000 VAT threshold, but the company was not '
            || 'VAT-registered',
            cast(round(revenue_2024) as bigint)
        ) when not_vat_registered then
            'not VAT-registered since 2024 or earlier: its declared turnover stays under €45,000 a year'
        end
    ], lambda x: x is not null) as reasons,
    list_filter([
        case when not identity_checked
            then 'company found by its name: confirm it runs the place before acting' end,
        case when same_trade_neighbours is not null then format(
            'other companies of the same trade are registered at the address: {}',
            array_to_string(same_trade_neighbours, ', ')
        ) end,
        case when any_open_24_7
            then 'a place is listed as open 24/7 on Google; its hours are not used' end,
        case when revenue_2024 < wage_bill_estimate then format(
            '2024 revenue €{} is below its declared yearly wage bill of about €{}',
            cast(round(revenue_2024) as bigint), cast(round(wage_bill_estimate) as bigint)
        ) end,
        case when places_with_hours < places_all then 'some places have no opening hours on Google' end,
        case when registered_on > (select month from sodra) - interval 12 month
            then 'company registered less than a year ago' end,
        case when overdue_tax_arrears > 0
            then format('overdue tax debt €{}', cast(round(overdue_tax_arrears) as bigint)) end,
        case when vat_deregistered_on >= date '2024-01-01'
            then format('VAT registration ended on {}', vat_deregistered_on) end,
        case when job_ad_wage is not null and average_wage is not null then format(
            'its job ads offer €{} a month against a declared average of €{}',
            cast(round(job_ad_wage) as bigint), cast(round(average_wage) as bigint)
        ) end
    ], lambda x: x is not null) as context,
    places,
    reviews,
    busy_threshold,
    places_all,
    reviews_all,
    open_hours_all,
    staff_latest,
    staff_avg_12m,
    staff_declared,
    staffing_coverage,
    reviews_per_staff,
    average_wage,
    wage_to_minimum,
    revenue_2024,
    revenue_per_review,
    is_vat_payer,
    missing_accounts_2024,
    overdue_tax_arrears,
    job_ads,
    job_ad_wage,
    staff_gap,
    staff_gap_severe,
    activity_outruns_size,
    low_declared_wage,
    accounts_missing,
    not_vat_registered,
    identity_checked,
    same_trade_neighbours,
    place_ids,
    (select month from sodra) as sodra_month,
    (select run_id from google_run) as google_run_id,
    (select run_id from sodra) as sodra_run_id
from scored
