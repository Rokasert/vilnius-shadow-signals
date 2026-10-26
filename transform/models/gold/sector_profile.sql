-- What the official data says about each activity class in Vilnius, to choose categories
-- and, later, as peer benchmarks. "Staff" means people insured by Sodra in the latest month.
-- Every share states the group it is computed on (see the *_basis columns).
with latest_month as (
    select max(month) as month from {{ ref('employer_months') }}
),

minimum_wage as (
    select
        max(monthly_eur) filter (where year = (select year(month) from latest_month)) as latest,
        max(monthly_eur) filter (where year = 2024) as y2024
    from {{ ref('minimum_wage') }}
),

sodra_latest as (
    select e.company_code, e.insured, e.average_wage, e.municipality, e.activity_code
    from {{ ref('employer_months') }} as e
    inner join latest_month as l on e.month = l.month
    where e.company_code is not null
),

staff_2025 as (
    select company_code, avg(insured) as avg_staff
    from {{ ref('employer_months') }}
    where year(month) = 2025 and company_code is not null
    group by 1
),

vilnius_companies as (
    select
        c.company_code,
        coalesce(c.activity_class, left(s.activity_code, 4)) as activity_class,
        c.is_owner_run_form,
        c.is_name_hidden
    from {{ ref('companies') }} as c
    left join {{ ref('company_registered_addresses') }} as a on c.company_code = a.company_code
    left join sodra_latest as s on c.company_code = s.company_code
    where
        c.is_registered
        and (c.is_operating or s.insured > 0)
        and (a.is_vilnius_address or s.municipality = 'Vilniaus m. sav.')
),

revenue_2024 as (
    select company_code, arg_max(revenue, period_end) as revenue
    from {{ ref('company_financials') }}
    where fiscal_year = 2024
    group by 1
),

taxes_2025 as (
    select company_code, sum(amount_ytd) as taxes_paid
    from {{ ref('company_taxes_paid') }}
    where company_code is not null and year = 2025 and month = 12
    group by 1
),

missing_filings as (
    select distinct company_code
    from {{ ref('company_filing_gaps') }}
    where gap_type = 'missing' and missing_year >= 2022
),

vacancy_wages as (
    select company_code, median(wage_offered_monthly) as wage_offered
    from {{ ref('job_vacancies') }}
    where company_code is not null and wage_offered_monthly > 0
    group by 1
),

place_types as (
    select company_code, arg_min(division_type, division_no) as division_type
    from {{ ref('company_divisions') }}
    where is_open
    group by 1
),

company_facts as (
    select
        v.company_code,
        v.activity_class,
        v.is_owner_run_form,
        v.is_name_hidden,
        coalesce(s.insured, 0) as staff,
        s.average_wage / mw.latest as wage_to_minimum,
        coalesce(vat.is_vat_payer, false) as is_vat_payer,
        r.revenue,
        st.avg_staff as avg_staff_2025,
        r.revenue / nullif(st.avg_staff, 0) as revenue_per_staff,
        r.revenue < st.avg_staff * mw.y2024 * 12 as revenue_below_min_payroll,
        t.taxes_paid / nullif(st.avg_staff, 0) as taxes_per_staff_2025,
        coalesce(ar.overdue_arrears, 0) > 0 as has_overdue_arrears,
        mf.company_code is not null as has_missing_filings,
        jv.wage_offered,
        jv.wage_offered / nullif(s.average_wage, 0) as offer_to_declared,
        pt.division_type
    from vilnius_companies as v
    cross join minimum_wage as mw
    left join sodra_latest as s on v.company_code = s.company_code
    left join {{ ref('company_vat') }} as vat on v.company_code = vat.company_code
    left join revenue_2024 as r on v.company_code = r.company_code
    left join staff_2025 as st on v.company_code = st.company_code
    left join taxes_2025 as t on v.company_code = t.company_code
    left join {{ ref('company_tax_arrears') }} as ar on v.company_code = ar.company_code
    left join missing_filings as mf on v.company_code = mf.company_code
    left join vacancy_wages as jv on v.company_code = jv.company_code
    left join place_types as pt on v.company_code = pt.company_code
),

classes as (
    select
        activity_class,
        left(activity_class, 2) as activity_division,
        -- size and how much we can see
        count(*) as companies,
        count(*) filter (where staff > 0) as employers,
        avg((staff = 0)::int) as no_staff_share,
        avg((staff = 0 and is_owner_run_form)::int) / nullif(avg((staff = 0)::int), 0)
            as no_staff_owner_run_share,
        median(staff) filter (where staff > 0) as median_staff,
        avg((staff between 1 and 3)::int) filter (where staff > 0) as wage_hidden_share,
        count(*) filter (where staff >= 4) as wage_visible_basis,
        -- declared wages, among employers whose wage Sodra publishes (4+ staff)
        median(wage_to_minimum) as median_wage_to_minimum,
        avg((wage_to_minimum < 1)::int) filter (where wage_to_minimum is not null)
            as below_minimum_wage_share,
        -- declared revenue, among companies that filed 2024 accounts
        count(revenue) as revenue_basis,
        median(revenue) as median_revenue_2024,
        median(revenue_per_staff) filter (where revenue > 0) as median_revenue_per_staff,
        avg(revenue_below_min_payroll::int) filter (where revenue is not null and avg_staff_2025 > 0)
            as revenue_below_min_payroll_share,
        avg((revenue < 45000)::int) filter (where revenue is not null) as below_vat_threshold_share,
        avg(is_vat_payer::int) as vat_payer_share,
        -- taxes paid to VMI in 2025 per member of staff
        median(taxes_per_staff_2025) as median_taxes_per_staff_2025,
        -- compliance record
        avg(has_overdue_arrears::int) as overdue_arrears_share,
        avg(has_missing_filings::int) as missing_filings_share,
        -- job ads: offered wage compared with the same company's declared average wage
        count(offer_to_declared) as offer_basis,
        median(offer_to_declared) as median_offer_to_declared,
        -- false-positive risk and context
        avg(is_owner_run_form::int) as owner_run_share,
        -- sole proprietors' names are not published, so Google can't be linked to them by name
        avg(is_name_hidden::int) as name_hidden_share,
        mode(division_type) as typical_declared_place
    from company_facts
    where activity_class is not null
    group by activity_class
    having count(*) >= 20
),

-- VMI publishes controls and audits by EVRK 2 code; matched on the 2-digit division,
-- which mostly carries over to EVRK 2.1 (car repair, 45 -> 95, is an exception).
controls as (
    select activity_division, sum(controlled_payers) as controlled_payers
    from {{ ref('sector_tax_controls') }}
    where municipality_code = 13 and year >= 2023
    group by 1
),

audits as (
    select
        activity_division,
        sum(assessed_total) / nullif(sum(audited_payers), 0) as assessed_per_audited_payer
    from {{ ref('sector_tax_audits') }}
    where year >= 2023
    group by 1
),

division_sizes as (
    select activity_division, sum(companies) as division_companies
    from classes
    group by 1
)

select
    c.activity_class,
    n.name_lt as activity_name,
    c.* exclude (activity_class),
    100.0 * ctl.controlled_payers / nullif(d.division_companies, 0)
        as division_vmi_controls_per_100_companies,
    aud.assessed_per_audited_payer as division_vmi_assessed_per_audited_payer
from classes as c
left join {{ ref('activity_codes') }} as n on n.version = '2.1' and c.activity_class = n.code
left join controls as ctl on c.activity_division = ctl.activity_division
left join audits as aud on c.activity_division = aud.activity_division
left join division_sizes as d on c.activity_division = d.activity_division
