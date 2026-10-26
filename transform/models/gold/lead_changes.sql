{{ config(tags=['google']) }}
-- depends_on: {{ ref('leads') }}

-- This month's leads against the previous month's saved list (history.leads, kept in the lake
-- as one Parquet file per month): which leads are new, which stayed, moved tier, or dropped.
{%- set history = adapter.get_relation(
    database=target.database, schema='history', identifier='leads'
) %}
with current_leads as (
    select snapshot_month, category, company_code, company_name, tier, score
    from {{ ref('leads') }}
),

current_month as (
    select max(snapshot_month) as month from {{ ref('company_assessment') }}
),

previous_leads as (
{% if history %}
    select snapshot_month, category, company_code, company_name, tier, score
    from {{ source('history', 'leads') }}
    where snapshot_month = (
        select max(snapshot_month) from {{ source('history', 'leads') }}
        where snapshot_month < (select month from current_month)
    )
{% else %}
    select
        null::varchar as snapshot_month,
        null::varchar as category,
        null::varchar as company_code,
        null::varchar as company_name,
        null::varchar as tier,
        null::integer as score
    where false
{% endif %}
)

select
    (select month from current_month) as snapshot_month,
    coalesce(c.category, p.category) as category,
    coalesce(c.company_code, p.company_code) as company_code,
    coalesce(c.company_name, p.company_name) as company_name,
    p.snapshot_month as previous_month,
    p.tier as previous_tier,
    c.tier as current_tier,
    p.score as previous_score,
    c.score as current_score,
    case
        when p.company_code is null then 'new'
        when c.company_code is null then 'dropped'
        when c.tier = p.tier then 'same tier'
        when c.tier = 'priority' then 'moved up'
        else 'moved down'
    end as change
from current_leads as c
full outer join previous_leads as p
    on c.category = p.category and c.company_code = p.company_code
