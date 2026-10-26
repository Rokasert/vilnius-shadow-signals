-- Profit-and-loss statements, one row per company and reporting period. The source repeats
-- most lines (identical copies), so each line keeps its latest filing only.
with lines as (
    select
        p."juridinis_asmuo._id" as jar_record_id,
        p.laikotarpis_nuo as period_start,
        p.laikotarpis_iki as period_end,
        p.reg_date as filed_on,
        p.template_name as statement_type,
        upper(p.line_name) as line_name,
        p.reiksme as amount
    from {{ source('bronze', 'jar__profit_loss_statements') }} as p
    qualify row_number() over (
        partition by p."juridinis_asmuo._id", p.laikotarpis_nuo, p.laikotarpis_iki, p.line_type_id
        order by p.reg_date desc, p._id
    ) = 1
),

statements as (
    select
        jar_record_id,
        period_start,
        period_end,
        max(filed_on) as filed_on,
        any_value(statement_type) as statement_type,
        max(amount) filter (where line_name = 'PARDAVIMO PAJAMOS') as sales_revenue,
        max(amount) filter (where line_name = 'PAJAMOS') as other_income,
        max(amount) filter (
            where line_name in (
                'PELNAS (NUOSTOLIAI) PRIEŠ APMOKESTINIMĄ', 'VEIKLOS REZULTATAS PRIEŠ APMOKESTINIMĄ'
            )
        ) as profit_before_tax,
        max(amount) filter (
            where line_name in (
                'GRYNASIS PELNAS (NUOSTOLIAI)',
                'ATASKAITINIŲ METŲ PELNAS (NUOSTOLIAI)',
                'GRYNASIS VEIKLOS REZULTATAS'
            )
        ) as net_profit
    from lines
    group by jar_record_id, period_start, period_end
)

select
    c.company_code,
    s.period_start,
    s.period_end,
    year(s.period_end) as fiscal_year,
    s.filed_on,
    s.statement_type,
    coalesce(s.sales_revenue, s.other_income) as revenue,
    s.sales_revenue,
    s.other_income,
    s.profit_before_tax,
    s.net_profit
from statements as s
inner join {{ ref('companies') }} as c on s.jar_record_id = c.jar_record_id
