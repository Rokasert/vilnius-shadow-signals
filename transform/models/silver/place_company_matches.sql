{{ config(tags=['google']) }}

-- The most likely company behind each Vilnius place. Each place-company pair is scored by its
-- strongest evidence, using how often that kind of evidence named the right company among
-- 453 places whose own website states the company code (see the handbook). Links an analyst
-- has checked (seed match_corrections) come first. A confident match also needs a clear lead
-- over the runner-up company.
with pairs as (
    select
        place_id,
        company_code,
        list(distinct method order by method) as methods,
        max(name_similarity) as name_similarity,
        bool_or(is_category_consistent) as is_category_consistent,
        bool_or(is_company_in_vilnius) as is_company_in_vilnius
    from {{ ref('place_company_candidates') }}
    group by place_id, company_code
),

-- a site naming more than two companies is a food court, mall or franchise group
website_companies as (
    select place_id, count(distinct company_code) as companies
    from {{ ref('website_company_codes') }}
    group by place_id
),

scored as (
    select
        p.*,
        case
            when list_contains(p.methods, 'analyst_confirmed') then 0.99
            when list_has_any(p.methods, ['website_code', 'website_vat']) then
                case
                    when coalesce(w.companies, 0) > 2 then 0.6
                    when list_contains(p.methods, 'website_code') then 0.97
                    else 0.95
                end
            when list_has_any(p.methods, ['name_exact', 'directory_link']) then least(
                0.98,
                case
                    when is_company_in_vilnius and is_category_consistent then 0.95
                    when is_company_in_vilnius then 0.85
                    when is_category_consistent then 0.65
                    else 0.35
                end
                + 0.03 * list_has_any(p.methods, ['registered_address', 'vacancy_address'])::int
            )
            when list_contains(p.methods, 'name_in_place') then
                case
                    when is_company_in_vilnius and is_category_consistent then
                        0.85
                        + 0.05 * list_has_any(p.methods, ['registered_address', 'vacancy_address'])::int
                    when is_company_in_vilnius then 0.5
                    else 0.1
                end
            when list_contains(p.methods, 'brand_exact') then
                case when is_company_in_vilnius and is_category_consistent then 0.35 else 0.05 end
            when
                list_contains(p.methods, 'registered_address')
                and list_contains(p.methods, 'vacancy_address')
                and is_category_consistent
                then 0.33
            when is_category_consistent then 0.1
            else 0.01
        end as score,
        case
            when list_contains(p.methods, 'analyst_confirmed') then 'checked by an analyst'
            when list_has_any(p.methods, ['website_code', 'website_vat']) then
                case when coalesce(w.companies, 0) > 2 then 'website (several companies)' else 'website' end
            when list_has_any(p.methods, ['name_exact', 'directory_link']) then 'same name'
            when list_contains(p.methods, 'name_in_place') then 'company name in place name'
            when list_contains(p.methods, 'brand_exact') then 'same distinctive words'
            else 'address only'
        end as evidence
    from pairs as p
    left join website_companies as w on p.place_id = w.place_id
),

ranked as (
    select
        *,
        row_number() over (partition by place_id order by score desc, company_code) as rank,
        lead(score) over (partition by place_id order by score desc, company_code)
            as runner_up_score,
        count(*) over (partition by place_id) as candidates
    from scored
)

select
    p.place_id,
    r.company_code,
    r.score,
    round(r.score - coalesce(r.runner_up_score, 0), 3) as margin,
    coalesce(r.candidates, 0) as candidates,
    r.methods,
    r.evidence,
    r.name_similarity,
    r.is_category_consistent,
    r.is_company_in_vilnius,
    case
        when r.company_code is null then 'unmatched'
        when r.score >= 0.85 and r.score - coalesce(r.runner_up_score, 0) >= 0.1 then 'high'
        when r.score >= 0.6 and r.score - coalesce(r.runner_up_score, 0) >= 0.1 then 'medium'
        else 'low'
    end as confidence
from {{ ref('places') }} as p
left join ranked as r on p.place_id = r.place_id and r.rank = 1
where p.is_in_vilnius
