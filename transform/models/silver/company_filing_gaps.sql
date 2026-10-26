-- Companies listed by the register as not having filed their annual financial statements
-- (one row per missing year), or as currently late (no year given). The source repeats rows.
with gaps as (
    select
        "juridinis_asmuo._id" as jar_record_id,
        'missing' as gap_type,
        cast(nepateikta_iki as varchar) as listed_until
    from {{ source('bronze', 'jar__missing_financial_statements') }}

    union all

    select
        "juridinis_asmuo._id" as jar_record_id,
        'late' as gap_type,
        cast(veluojantis_iki as varchar) as listed_until
    from {{ source('bronze', 'jar__late_financial_statements') }}
)

select distinct
    c.company_code,
    g.gap_type,
    try_cast(left(g.listed_until, 4) as integer) as missing_year
from gaps as g
inner join {{ ref('companies') }} as c on g.jar_record_id = c.jar_record_id
