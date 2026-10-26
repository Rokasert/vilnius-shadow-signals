{{ config(tags=['google']) }}

-- One row per candidate category with everything needed to choose: what the registers say,
-- how complete and linkable Google is, and what the two show together.
with registry as (
    select
        cc.category,
        sum(s.companies) as registry_companies,
        sum(s.employers) as registry_employers,
        sum(s.wage_visible_basis) as companies_with_visible_wages,
        sum(s.below_minimum_wage_share * s.wage_visible_basis)
            / nullif(sum(s.wage_visible_basis), 0) as below_minimum_wage_share,
        sum(s.revenue_below_min_payroll_share * s.revenue_basis)
            / nullif(sum(s.revenue_basis), 0) as revenue_below_min_payroll_share,
        sum(s.below_vat_threshold_share * s.revenue_basis)
            / nullif(sum(s.revenue_basis), 0) as below_vat_threshold_share,
        sum(s.overdue_arrears_share * s.companies) / sum(s.companies) as overdue_arrears_share,
        sum(s.owner_run_share * s.companies) / sum(s.companies) as owner_run_share,
        sum(s.name_hidden_share * s.companies) / sum(s.companies) as name_hidden_share
    from {{ ref('category_activity_classes') }} as cc
    inner join {{ ref('sector_profile') }} as s on cc.activity_class = s.activity_class
    where cc.is_core
    group by cc.category
),

signals as (
    select
        category,
        count(*) as companies_matched,
        median(staffing_coverage) as median_staffing_coverage,
        avg((staffing_coverage < 1)::int) filter (where staffing_coverage is not null)
            as coverage_below_1_share,
        avg((staffing_coverage < 0.5)::int) filter (where staffing_coverage is not null)
            as coverage_below_half_share,
        median(reviews_per_staff) as median_reviews_per_staff,
        avg((staff = 0)::int) as matched_without_staff_share,
        count(*) filter (
            where staffing_coverage < 0.5 and reviews_per_staff_z > 1 and not is_owner_run_form
        ) as companies_with_both_signals
    from {{ ref('company_category_signals') }}
    group by category
)

select
    g.category,
    -- registers
    r.registry_companies,
    r.registry_employers,
    r.companies_with_visible_wages,
    round(r.below_minimum_wage_share, 3) as below_minimum_wage_share,
    round(r.revenue_below_min_payroll_share, 3) as revenue_below_min_payroll_share,
    round(r.below_vat_threshold_share, 3) as below_vat_threshold_share,
    round(r.overdue_arrears_share, 3) as overdue_arrears_share,
    round(r.owner_run_share, 3) as owner_run_share,
    round(r.name_hidden_share, 3) as name_hidden_share,
    -- Google
    g.places,
    g.with_website_share,
    g.with_opening_hours_share,
    g.reviews_median,
    g.reviews_p90,
    g.high_match_share + g.medium_match_share as matched_share,
    g.matched_category_consistent_share,
    g.registry_found_share,
    -- together
    s.companies_matched,
    round(s.median_staffing_coverage, 2) as median_staffing_coverage,
    round(s.coverage_below_1_share, 3) as coverage_below_1_share,
    round(s.coverage_below_half_share, 3) as coverage_below_half_share,
    round(s.median_reviews_per_staff, 1) as median_reviews_per_staff,
    round(s.matched_without_staff_share, 3) as matched_without_staff_share,
    s.companies_with_both_signals
from {{ ref('google_category_overview') }} as g
left join registry as r on g.category = r.category
left join signals as s on g.category = s.category
order by g.category
