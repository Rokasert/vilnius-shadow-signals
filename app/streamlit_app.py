"""Analyst app: the lead list first, then a plain browser of every data layer."""

from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

from shadow_signals.settings import get_settings

MAX_ROWS = 10_000

LAYERS = {
    "bronze": "Raw data exactly as downloaded, one table per source dataset.",
    "silver": "Each source cleaned: renamed, typed, codes decoded, duplicates removed. Lookups are "
    "joined in (e.g. a company's legal form), and each Google place is linked to its company.",
    "gold": "Results built from silver: every linked company's assessment, the leads, summaries.",
}

TABLES = {
    # bronze
    "ar__buildings_vilnius": "Address register: Vilnius buildings (street, number, postcode).",
    "ar__municipalities": "Address register: municipalities.",
    "ar__premises": "Address register: premises (flats, units) inside buildings.",
    "ar__streets": "Address register: streets.",
    "google__places": "Google Places API: raw search responses, one row per request.",
    "jar__late_financial_statements": "Company register: companies late with annual accounts.",
    "jar__legal_entities": "Company register: every legal entity (code, name, form, status).",
    "jar__legal_forms": "Company register: legal forms (UAB, MB, IĮ…).",
    "jar__legal_statuses": "Company register: legal statuses.",
    "jar__missing_financial_statements": "Company register: annual accounts not filed.",
    "jar__profit_loss_statements": "Company register: profit-and-loss lines from annual accounts.",
    "jar__registered_addresses": "Company register: each company's registered address.",
    "lsd__evrk_2": "Statistics Lithuania: activity codes (EVRK 2).",
    "lsd__evrk_2_1": "Statistics Lithuania: activity codes (EVRK 2.1).",
    "lsd__legal_entity_activities": "Statistics Lithuania: each company's main activity.",
    "sodra__employers_monthly": "Sodra: insured staff and average wage per employer per month.",
    "uzt__job_vacancies_vilnius": "Employment Service: Vilnius job ads with offered wages.",
    "vmi__audit_assessments_by_sector": "VMI: tax audit results by sector (totals).",
    "vmi__control_actions_by_sector": "VMI: control actions by sector (totals).",
    "vmi__tax_arrears": "VMI: tax debt per company.",
    "vmi__taxes_paid_vilnius": "VMI: taxes paid by Vilnius companies.",
    "vmi__taxpayers": "VMI taxpayer register: VAT registration and declared business places.",
    "vmi__vat_returns_by_sector": "VMI: VAT returns by sector (totals).",
    "web__company_codes": "Company and VAT codes found on the Google places' own websites.",
    # silver
    "activity_codes": "Activity codes (EVRK 2 and 2.1) with names.",
    "addresses": "Vilnius addresses (buildings and premises with their street).",
    "companies": "One row per company: name, legal form, status, main activity.",
    "company_divisions": "Business places companies declared to VMI.",
    "company_filing_gaps": "Missing or late annual accounts, per company and year.",
    "company_financials": "Revenue and profit per company and reporting period.",
    "company_registered_addresses": "Each company's registered address.",
    "company_taxes_paid": "Taxes paid per company and month.",
    "company_tax_arrears": "Tax debt per company.",
    "company_vat": "VAT registration per company.",
    "employer_months": "Insured staff and average wage per employer per month (Sodra).",
    "job_vacancies": "Job ads with offered monthly wage and workplace address.",
    "places": "One row per Google place: name, address, reviews, weekly opening hours.",
    "place_categories": "Which category (search) found each Google place.",
    "place_company_candidates": "Every company a place might belong to, and the evidence.",
    "place_company_matches": "The company chosen for each place, with confidence.",
    "sector_tax_audits": "VMI tax audit results by sector.",
    "sector_tax_controls": "VMI control actions by sector.",
    "sector_vat_returns": "VMI VAT returns by sector.",
    "website_company_codes": "Company codes found on places' own websites.",
    # gold
    "company_assessment": "Every company with confidently linked places: data, signals, tier.",
    "leads": "The lead list: companies to look at first, with reasons.",
    "category_summary": "Per category: from Google places to leads, step by step.",
    "lead_changes": "This month's leads against last month's: new, dropped, moved.",
}

EXAMPLES = {
    "Which categories have the highest share of flagged businesses?": """select category,
       companies_assessed as companies_scored, leads,
       lead_share_of_assessed as share_flagged
from gold.category_summary
order by share_flagged desc""",
    "Leads with their reasons": """select rank, tier, tier_reason, company_name, category, reasons
from gold.leads order by rank""",
    "Insured staff of one company by month": """select month, insured, average_wage
from silver.employer_months
where company_code = '125442810'
order by month""",
}

# the ranked table: plain names, and what each column means
COLUMNS = {
    "Rank": "Order to look at companies in: priority leads, then review leads, then the rest by "
    "points. The next row is the next company to look at.",
    "Lead": "Priority: the clearest cases. Review: worth a look. Not a lead. Not scored: owner-run "
    "or non-profit form, whose owners or volunteers need not be insured staff.",
    "Why this tier": "The rule that decided the tier, in words.",
    "Points": "Sum of the points columns (Pts: …). A lead needs a busy company with 2+ points.",
    "Company": "Company name from the company register.",
    "Company code": "Company code (įmonės kodas).",
    "Category": "Google category the places were found in.",
    "Google reviews": "All-time Google reviews of the company's places in this category.",
    "Busy": "Yes if the company has at least the category's median number of reviews.",
    "Open h/week": "Weekly opening hours of all the company's places on Google "
    "(places listed as open 24/7 are left out).",
    "Insured staff": "People insured by Sodra: the higher of the latest month and the "
    "12-month average. A part-timer counts as one.",
    "Hours staff can cover": "Insured staff × 40 hours.",
    "Coverage": "Hours staff can cover ÷ opening hours. Below 1, the declared staff cannot keep "
    "the places open even with one person on duty.",
    "Wage % of minimum": "Average declared wage ÷ minimum wage. Sodra publishes the wage only "
    "for 4+ insured staff.",
    "Revenue 2024 €": "Revenue from the 2024 annual accounts, the latest year most companies "
    "have filed.",
    "VAT-registered": "No = not VAT-registered at any time since 2024, i.e. it declares "
    "turnover under €45,000 a year.",
    "Pts: staff can't cover hours": "2 points if coverage is below 1, 3 if below 0.5.",
    "Pts: busier than its size": "1 point if reviews per insured worker are in the top 10% of "
    "the category, or revenue per review in the bottom 10%.",
    "Pts: low wage": "1 point if the average wage is below 80% of the minimum wage.",
    "Pts: no 2024 accounts": "1 point if 2024 annual accounts were not filed.",
    "Pts: no VAT": "1 point if not VAT-registered since 2024.",
    "Why": "The signals, in words, with the numbers.",
    "Check before acting": "What could explain it, or what to confirm first.",
    "Google Maps": "The company's busiest place on Google Maps.",
}

RANKED_SQL = """
select
    row_number() over (
        order by case tier when 'priority' then 1 when 'review' then 2
                           when 'none' then 3 else 4 end,
                 score desc, reviews desc
    ) as "Rank",
    case tier when 'priority' then 'Priority' when 'review' then 'Review'
              when 'none' then 'Not a lead' else 'Not scored' end as "Lead",
    tier_reason as "Why this tier",
    score as "Points",
    company_name as "Company",
    company_code as "Company code",
    replace(category, '_', ' ') as "Category",
    reviews as "Google reviews",
    case when is_busy then 'yes' else 'no' end as "Busy",
    round(open_hours_all) as "Open h/week",
    staff_declared as "Insured staff",
    round(staff_declared * 40) as "Hours staff can cover",
    staffing_coverage as "Coverage",
    round(wage_to_minimum * 100) as "Wage % of minimum",
    revenue_2024 as "Revenue 2024 €",
    case when not_vat_registered then 'no' else 'yes' end as "VAT-registered",
    2 * staff_gap::int + staff_gap_severe::int as "Pts: staff can't cover hours",
    activity_outruns_size::int as "Pts: busier than its size",
    low_declared_wage::int as "Pts: low wage",
    accounts_missing::int as "Pts: no 2024 accounts",
    not_vat_registered::int as "Pts: no VAT",
    array_to_string(reasons, '; ') as "Why",
    array_to_string(context, '; ') as "Check before acting",
    (select google_maps_uri from silver.places as p where p.place_id = place_ids[1])
        as "Google Maps",
    category as _category,
    company_code as _code
from gold.company_assessment
order by "Rank"
"""

st.set_page_config(page_title="Vilnius shadow signals", layout="wide")
st.title("Vilnius shadow signals")
st.caption(
    "Vilnius businesses that look busy on Google but quiet in official records. "
    "These are leads to check, not accusations."
)

warehouse = Path(get_settings().warehouse_path)
if not warehouse.exists():
    st.warning("No data yet. Run the pipeline first: `shadow-signals run`.")
    st.stop()


def query(sql: str) -> pd.DataFrame:
    with duckdb.connect(str(warehouse), read_only=True) as con:
        relation = con.sql(sql)
        return relation.limit(MAX_ROWS).df() if relation is not None else pd.DataFrame()


tables = query(
    "select table_schema as layer, table_name as name from information_schema.tables "
    "where table_schema in ('bronze', 'silver', 'gold') order by table_schema, table_name"
)
leads_tab, data_tab = st.tabs(["Leads", "Data"])

with leads_tab:
    if "leads" not in set(tables.loc[tables["layer"] == "gold", "name"]):
        st.info("No lead list yet: it needs the Google data. Run `shadow-signals run`.")
    else:
        st.subheader("Where the numbers come from")
        st.dataframe(
            query(
                """with runs as (
                     select source || '/' || dataset as k, strftime(finished_at, '%Y-%m-%d') as d,
                            source_as_of
                     from meta.bronze_runs where is_latest),
                   a as (select strftime(max(sodra_month), '%Y-%m') as sodra,
                                count(*) filter (where tier <> 'not assessed') as scored
                         from gold.company_assessment),
                   y as (select count(distinct f.company_code) as filed_2025
                         from silver.company_financials as f
                         join gold.company_assessment as c using (company_code)
                         where f.fiscal_year = 2025 and c.tier <> 'not assessed')
                 select * from (values
                   ('Google Maps (Places API)', 'places, reviews, opening hours',
                    'pulled ' || (select d from runs where k = 'google/places')),
                   ('Sodra', 'insured staff, average wage (monthly)',
                    'up to ' || (select sodra from a) || '; Sodra publishes with ~2 months delay'),
                   ('Company register: annual accounts', 'revenue',
                    '2024: the latest year companies have filed; 2025 accounts so far for '
                    || (select filed_2025 from y) || ' of ' || (select scored from a)
                    || ' scored companies'),
                   ('VMI', 'VAT registration, tax debt',
                    'as of ' || (select d from runs where k = 'vmi/taxpayers')),
                   ('Company register', 'name, legal form, registered address',
                    'as of ' || (select d from runs where k = 'jar/legal_entities'))
                 ) as t("Source", "Used for", "Data")"""
            ),
            hide_index=True,
        )

        st.subheader("From Google places to leads")
        st.caption(
            "Only places Google could link to a company with high confidence can be checked: "
            "the company code on the place's own website, or the same name with a fitting "
            "activity and a presence in Vilnius. Most places carry a trading name that differs "
            "from the company's legal name, so they stay unlinked rather than guessed."
        )
        st.dataframe(
            query(
                """select replace(category, '_', ' ') as "Category",
                          registry_companies as "Companies registered in Vilnius",
                          places as "Google places found",
                          places_linked as "Places linked to a company",
                          companies_linked as "Companies behind them",
                          companies_assessed as "Scored (UAB/AB)",
                          companies_busy as "Busy",
                          leads as "Leads",
                          round(100 * lead_share_of_assessed, 1) as "% of scored flagged"
                   from gold.category_summary order by category"""
            ),
            hide_index=True,
            column_config={
                "Companies registered in Vilnius": st.column_config.NumberColumn(
                    help="Operating companies registered in Vilnius whose main activity is this "
                    "category (company register, Statistics Lithuania). For scale."
                ),
                "Google places found": st.column_config.NumberColumn(
                    help="Vilnius places Google returned for this category's searches."
                ),
                "Places linked to a company": st.column_config.NumberColumn(
                    help="Places linked to a company with high confidence."
                ),
                "Scored (UAB/AB)": st.column_config.NumberColumn(
                    help="Companies expected to have employees. Owner-run partnerships (MB), "
                    "sole traders (IĮ) and non-profits are not scored."
                ),
                "Busy": st.column_config.NumberColumn(
                    help="Scored companies with at least the category's median number of reviews."
                ),
            },
        )

        with st.expander("How does a company become a lead?"):
            st.markdown(
                """
1. **Scored**: its Google places are linked to it with high confidence, and it is a UAB or AB.
2. **Busy**: at least as many Google reviews as the typical (median) scored company of its category.
3. **Points** for what its official records show (the *Pts:* columns, summed in *Points*).
4. **Tier**:
   - *Review* = busy and 2+ points;
   - *Priority* = busy, a staffing gap, 3+ points, and a company link that doesn't rest on the
     name alone when another company of the same trade is registered at the address.

*Rank* orders every scored company: priority, then review, then the rest by points.
*Why this tier* says which rule applied. Hover over a column name to see what it means.
"""
            )

        st.subheader("Companies, ranked")
        only_leads = st.toggle("Only leads", value=False)
        table = query(RANKED_SQL)
        if only_leads:
            table = table[table["Lead"].isin(["Priority", "Review"])]
        visible = table.drop(columns=["_category", "_code"])
        config = {name: st.column_config.Column(help=text) for name, text in COLUMNS.items()}
        config["Google Maps"] = st.column_config.LinkColumn(
            help=COLUMNS["Google Maps"], display_text="open"
        )
        config["Revenue 2024 €"] = st.column_config.NumberColumn(
            help=COLUMNS["Revenue 2024 €"], format="%.0f"
        )
        st.dataframe(visible, hide_index=True, column_config=config)
        st.download_button(
            "Download this table (CSV)", visible.to_csv(index=False), "companies.csv", "text/csv"
        )

        if not table.empty:
            st.subheader("One company in detail")
            labels = {
                f"#{rank} {company} · {category_name} · {lead}": (code, category)
                for rank, company, category_name, lead, code, category in zip(
                    table["Rank"],
                    table["Company"],
                    table["Category"],
                    table["Lead"],
                    table["_code"],
                    table["_category"],
                    strict=True,
                )
            }
            code, category = labels[st.selectbox("Company", list(labels))]
            where_one = f"company_code = '{code}' and category = '{category}'"
            st.markdown("**Its Google places, and how each was linked to the company**")
            st.dataframe(
                query(
                    f"""select p.name as "Place", p.formatted_address as "Address",
                               p.review_count as "Google reviews",
                               round(p.weekly_open_hours) as "Open h/week",
                               m.evidence as "Linked to the company by",
                               p.google_maps_uri as "Google Maps"
                        from silver.places as p
                        join silver.place_company_matches as m using (place_id)
                        where p.place_id in (select unnest(place_ids)
                                             from gold.company_assessment where {where_one})
                        order by p.review_count desc"""
                ),
                hide_index=True,
                column_config={
                    "Google Maps": st.column_config.LinkColumn(display_text="open"),
                },
            )
            staff_col, accounts_col = st.columns(2)
            staff_col.markdown("**Insured staff by month (Sodra)**")
            staff_col.line_chart(
                query(
                    f"""select month, sum(insured) as insured_staff
                        from silver.employer_months where company_code = '{code}'
                        group by month order by month"""
                ),
                x="month",
                y="insured_staff",
            )
            accounts_col.markdown("**Annual accounts**")
            accounts_col.dataframe(
                query(
                    f"""select fiscal_year as "Year", revenue as "Revenue €",
                               net_profit as "Net profit €"
                        from silver.company_financials where company_code = '{code}'
                        order by period_end"""
                ),
                hide_index=True,
            )

        changes = query("select * from gold.lead_changes where previous_month is not null")
        st.subheader("Since last month")
        if changes.empty:
            st.caption(
                "This is the first month in the history. Next month's run will show which leads "
                "are new, dropped or moved (table gold.lead_changes)."
            )
        else:
            st.dataframe(query("select * from gold.lead_changes order by change"), hide_index=True)

with data_tab:
    layer = st.radio("Layer", list(LAYERS), horizontal=True)
    st.caption(LAYERS[layer])
    names = list(tables.loc[tables["layer"] == layer, "name"])
    if not names:
        st.info("This layer is empty.")
    else:
        name = st.selectbox("Table", names)
        st.caption(TABLES.get(name, ""))
        columns_col, rows_col = st.columns([1, 3])
        columns_col.dataframe(
            query(f'describe {layer}."{name}"')[["column_name", "column_type"]], hide_index=True
        )
        rows_col.dataframe(query(f'select * from {layer}."{name}" limit 200'), hide_index=True)

    with st.expander("Ask your own question (SQL)"):
        example = st.selectbox("Example", list(EXAMPLES))
        text = st.text_area("SQL (read-only)", EXAMPLES[example], height=160)
        if st.button("Run"):
            try:
                st.dataframe(query(text), hide_index=True)
            except duckdb.Error as e:
                st.error(str(e))
