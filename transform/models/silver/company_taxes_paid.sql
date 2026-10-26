-- Taxes paid to VMI by Vilnius companies. `amount_ytd` is cumulative for the year:
-- December rows are full-year totals. VMI's company reference does not resolve, so rows
-- are linked by name: exact registered name first, then name key plus legal form,
-- accepting a link only when it points to exactly one registered company.
with taxes as (
    select
        _id as source_id,
        id as vmi_payer_id,
        pavadinimas as name,
        tipas as legal_form,
        {{ name_key('pavadinimas') }} as name_key,
        metai as year,
        menuo as month,
        suma as amount_ytd,
        atnaujinta as updated_on
    from {{ source('bronze', 'vmi__taxes_paid_vilnius') }}
),

registered as (
    select company_code, name, name_key, legal_form
    from {{ ref('companies') }}
    where is_registered
),

by_exact_name as (
    select name, any_value(company_code) as company_code
    from registered
    group by name
    having count(*) = 1
),

by_name_key as (
    select name_key, legal_form, any_value(company_code) as company_code
    from registered
    where name_key <> ''
    group by name_key, legal_form
    having count(*) = 1
)

select
    t.source_id,
    t.vmi_payer_id,
    coalesce(e.company_code, k.company_code) as company_code,
    case
        when e.company_code is not null then 'exact_name'
        when k.company_code is not null then 'name_key_and_form'
    end as link_method,
    t.name,
    t.legal_form,
    t.year,
    t.month,
    make_date(cast(t.year as integer), cast(t.month as integer), 1) as period_month,
    t.amount_ytd,
    t.month = 12 as is_full_year,
    t.updated_on
from taxes as t
left join by_exact_name as e on t.name = e.name
left join by_name_key as k on t.name_key = k.name_key and t.legal_form = k.legal_form
