{{ config(tags=['google']) }}
-- depends_on: {{ ref('company_vat') }}
-- depends_on: {{ ref('companies') }}
-- depends_on: {{ ref('places') }}

-- Company and VAT codes found on the places' own websites, kept only when they point to a
-- registered company. Platform sites (booking, directories, social) are left out: a code there
-- belongs to the platform, not the place. Empty until a website crawl has been loaded.
{%- set crawl = adapter.get_relation(
    database=target.database, schema='bronze', identifier='web__company_codes'
) %}
{% if crawl %}
with pages as (
    select
        unnest(from_json(to_json(place_ids), '["VARCHAR"]')) as place_id,
        url,
        from_json(to_json(codes), '[{"code": "VARCHAR", "context": "VARCHAR"}]') as codes,
        from_json(to_json(vat_codes), '[{"code": "VARCHAR", "context": "VARCHAR"}]') as vat_codes
    from {{ source('bronze', 'web__company_codes') }}
),

company_codes as (
    select place_id, url, unnest(codes) as found from pages
),

vat_codes as (
    select place_id, url, unnest(vat_codes) as found from pages
),

evidence as (
    select place_id, url, 'website_code' as evidence_type, found.code as company_code,
        found.context as context
    from company_codes

    union all

    select v.place_id, v.url, 'website_vat' as evidence_type, t.company_code,
        v.found.context as context
    from vat_codes as v
    inner join {{ ref('company_vat') }} as t on t.vat_code = 'LT' || v.found.code
)

select
    e.place_id,
    e.company_code,
    e.evidence_type,
    min(e.url) as url,
    min(e.context) as context,
    count(*) as pages
from evidence as e
inner join {{ ref('companies') }} as c on e.company_code = c.company_code and c.is_registered
inner join {{ ref('places') }} as p on e.place_id = p.place_id and not p.is_platform_website
group by e.place_id, e.company_code, e.evidence_type
{% else %}
select
    null::varchar as place_id,
    null::varchar as company_code,
    null::varchar as evidence_type,
    null::varchar as url,
    null::varchar as context,
    null::bigint as pages
where false
{% endif %}
