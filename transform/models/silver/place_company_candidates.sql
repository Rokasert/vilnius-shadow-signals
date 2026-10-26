{{ config(tags=['google']) }}

-- Every company a Vilnius place might belong to, the method that suggested it, and the facts
-- that decide how much it counts: does the company's activity fit the place's category, and
-- does it operate in Vilnius. place_company_matches turns these into a score.
with places as (
    select
        place_id, name, name_key, name_tokens, brand_key, directory_name_key, street_key, house_key
    from {{ ref('places') }}
    where is_in_vilnius
),

latest_month as (
    select max(month) as month from {{ ref('employer_months') }}
),

words as (
    select list(word) as generic from {{ ref('generic_name_words') }}
),

-- every registered company with a published name: chains registered elsewhere run Vilnius
-- venues too (17% of the companies named on venues' own websites are seated outside Vilnius)
companies as (
    select company_code, name, name_key, is_name_hidden, activity_class
    from {{ ref('companies') }}
    where is_registered and length(name_key) >= 3
),

named_companies as (
    select * from companies where not is_name_hidden
),

name_counts as (
    select name_key, count(*) as companies from named_companies group by name_key
),

-- signs that a company operates in Vilnius: its seat, Sodra employer record, a VMI business
-- place or a job ad here
vilnius_presence as (
    select company_code from {{ ref('company_registered_addresses') }} where is_vilnius_address
    union
    select company_code
    from {{ ref('employer_months') }}
    where municipality = 'Vilniaus m. sav.' and company_code is not null
    union
    select company_code
    from {{ ref('company_divisions') }}
    where is_open and municipality ilike 'Vilniaus m%'
    union
    select company_code from {{ ref('job_vacancies') }} where company_code is not null
),

-- activity classes a company is known by: the register's and Sodra's over the last year
company_classes as (
    select company_code, activity_class from companies where activity_class is not null
    union
    select company_code, left(activity_code, 4)
    from {{ ref('employer_months') }}
    where
        company_code is not null
        and activity_code is not null
        and month >= (select month - interval 12 month from latest_month)
),

website as (
    select place_id, company_code, evidence_type as method, context as evidence
    from {{ ref('website_company_codes') }}
),

name_exact as (
    select p.place_id, c.company_code, 'name_exact' as method, c.name as evidence
    from places as p
    inner join named_companies as c on p.name_key = c.name_key
    where length(p.name_key) >= 4
),

-- the company's name inside the place's name ('MELGA Autoservisas | Šeškinė' -> UAB "MELGA"):
-- runs of 1-3 words of the place name compared with company names
place_word_runs as (
    select
        p.place_id,
        p.name_key,
        array_to_string(p.name_tokens[t.i:t.i + n.n - 1], '') as run,
        len(list_filter(
            p.name_tokens[t.i:t.i + n.n - 1], lambda x: not list_contains(w.generic, x)
        )) > 0 as has_distinctive_word
    from places as p
    cross join words as w
    cross join (select unnest(range(1, 4)) as n) as n
    cross join lateral (select unnest(range(1, len(p.name_tokens) + 1)) as i) as t
    where t.i + n.n - 1 <= len(p.name_tokens)
),

name_in_place as (
    select distinct r.place_id, c.company_code, 'name_in_place' as method, c.name as evidence
    from place_word_runs as r
    inner join named_companies as c on r.run = c.name_key
    inner join name_counts as k on c.name_key = k.name_key
    where
        r.has_distinctive_word
        and length(r.run) >= 5
        and k.companies <= 3
        and r.run <> r.name_key
),

-- the same distinctive words once generic ones are dropped ('Kirpykla Ona' ~ 'MB Ona kirpykla')
company_brands as (
    select
        c.company_code,
        c.name,
        array_to_string(list_sort(list_filter(
            string_split(
                regexp_replace(
                    lower(strip_accents(regexp_replace(
                        lower(c.name),
                        '\b(uždaroji akcinė bendrovė|akcinė bendrovė|mažoji bendrija|individuali įmonė|viešoji įstaiga)\b',
                        ' ',
                        'g'
                    ))),
                    '[^a-z0-9]+',
                    ' ',
                    'g'
                ),
                ' '
            ),
            lambda x: x <> '' and not list_contains(w.generic, x)
        )), '') as brand_key
    from named_companies as c
    cross join words as w
),

brand_exact as (
    select p.place_id, b.company_code, 'brand_exact' as method, b.name as evidence
    from places as p
    inner join company_brands as b on p.brand_key = b.brand_key
    where length(p.brand_key) >= 4
),

directory_link as (
    select p.place_id, c.company_code, 'directory_link' as method, c.name as evidence
    from places as p
    inner join named_companies as c on p.directory_name_key = c.name_key
    where length(p.directory_name_key) >= 4
),

registered_address as (
    select
        p.place_id,
        a.company_code,
        'registered_address' as method,
        a.address_text as evidence,
        count(*) over (partition by p.place_id) as companies_at_address
    from places as p
    inner join {{ ref('company_registered_addresses') }} as a
        on p.street_key = a.street_key and p.house_key = a.house_key
    inner join companies as c on a.company_code = c.company_code
    where p.street_key <> '' and p.house_key <> ''
),

vacancy_address as (
    select
        p.place_id,
        v.company_code,
        'vacancy_address' as method,
        any_value(v.workplace_address) as evidence
    from places as p
    inner join {{ ref('job_vacancies') }} as v
        on p.street_key = v.workplace_street_key and p.house_key = v.workplace_house_key
    where v.company_code is not null and p.street_key <> '' and p.house_key <> ''
    group by p.place_id, v.company_code
),

candidates as (
    select * from website
    union all by name
    select * from name_exact
    union all by name
    select * from name_in_place
    union all by name
    select * from brand_exact
    union all by name
    select * from directory_link
    union all by name
    select * from registered_address
    union all by name
    select * from vacancy_address
),

fits as (
    select distinct k.place_id, k.company_code
    from candidates as k
    inner join {{ ref('place_categories') }} as pc on k.place_id = pc.place_id
    inner join {{ ref('category_activity_classes') }} as cc on pc.category = cc.category
    inner join company_classes as cl
        on k.company_code = cl.company_code and cc.activity_class = cl.activity_class
),

described as (
    select
        k.place_id,
        k.company_code,
        k.method,
        k.evidence,
        k.companies_at_address,
        case
            when not c.is_name_hidden
                then round(jaro_winkler_similarity(p.name_key, c.name_key), 3)
        end as name_similarity,
        f.place_id is not null as is_category_consistent,
        v.company_code is not null as is_company_in_vilnius
    from candidates as k
    inner join places as p on k.place_id = p.place_id
    inner join companies as c on k.company_code = c.company_code
    left join fits as f on k.place_id = f.place_id and k.company_code = f.company_code
    left join vilnius_presence as v on k.company_code = v.company_code
)

select *
from described
-- an address alone says little in a busy building: keep it only with a name or activity fit
where not (
    method in ('registered_address', 'vacancy_address')
    and coalesce(name_similarity, 0) < 0.6
    and not is_category_consistent
)
