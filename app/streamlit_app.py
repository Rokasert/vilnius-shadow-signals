from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

from shadow_signals.settings import get_settings

MAX_ROWS = 10_000
LAYERS = ("bronze", "silver", "gold", "reference")
EXAMPLES = {
    "All tables": "show all tables",
    "Latest bronze runs": """select source, dataset, records_total, source_as_of, warnings
from meta.bronze_runs where is_latest order by source, dataset""",
    "Sector profile, customer-facing sectors": """select
       activity_class, activity_name, companies, employers, no_staff_share, median_staff,
       wage_hidden_share, median_wage_to_minimum, below_minimum_wage_share,
       revenue_below_min_payroll_share, below_vat_threshold_share, vat_payer_share,
       overdue_arrears_share, owner_run_share, typical_declared_place
from gold.sector_profile
where activity_division in ('47', '55', '56', '86', '93', '95', '96')
order by companies desc""",
    "Vilnius restaurants, latest Sodra month": """select
       e.company_code, e.name, e.insured, e.average_wage, v.is_vat_payer
from silver.employer_months as e
left join silver.company_vat as v using (company_code)
where e.municipality = 'Vilniaus m. sav.'
  and e.activity_code = '561100'
  and e.month = (select max(month) from silver.employer_months)
order by e.insured desc""",
    "Google: busy places with few declared staff": """select
       category, company_code, company_name, places, reviews, open_hours_total, staff,
       staffing_coverage, reviews_per_staff, reviews_per_staff_z, all_matches_high
from gold.company_category_signals
where staffing_coverage < 0.5 and not is_owner_run_form
order by reviews_per_staff_z desc nulls last""",
    "Google: most-reviewed places with no company found": """select
       p.name, p.formatted_address, p.categories, p.review_count, p.website, m.confidence
from silver.places as p
join silver.place_company_matches as m using (place_id)
where m.confidence in ('low', 'unmatched')
order by p.review_count desc""",
    "Google: every candidate company for one place": """select c.*, co.name, co.activity_class
from silver.place_company_candidates as c
join silver.companies as co using (company_code)
where c.place_id = (select place_id from silver.places order by review_count desc limit 1)
order by c.company_code, c.method""",
}

st.set_page_config(page_title="Vilnius shadow signals", layout="wide")
st.title("Vilnius shadow signals")

warehouse = Path(get_settings().warehouse_path)
if not warehouse.exists():
    st.warning("No warehouse yet. Run the pipeline first.")
    st.stop()


def query(sql: str) -> pd.DataFrame:
    with duckdb.connect(str(warehouse), read_only=True) as con:
        relation = con.sql(sql)
        return relation.limit(MAX_ROWS).df() if relation is not None else pd.DataFrame()


def tables() -> pd.DataFrame:
    layers = ", ".join(f"'{layer}'" for layer in LAYERS)
    return query(
        "select table_schema as layer, table_name as name from information_schema.tables "
        f"where table_schema in ({layers}) order by table_schema, table_name"
    )


def readable(value: float) -> str:
    if pd.isna(value):
        return "–"
    return f"{value:,.0f}" if abs(value) >= 10 or value == int(value) else f"{value:.3f}"


available = tables()
datasets_tab, categories_tab, sql_tab = st.tabs(["Datasets", "Categories", "SQL"])

with datasets_tab:
    if available.empty:
        st.info("No data loaded yet.")
    else:
        with st.expander("Latest bronze downloads"):
            st.dataframe(
                query(
                    """select source, dataset, snapshot_month, run_id, records_total,
                              round(bytes / 1e6, 1) as mb_raw, source_as_of, warnings, finished_at
                       from meta.bronze_runs where is_latest order by source, dataset"""
                ),
                hide_index=True,
            )
        layer = st.radio(
            "Layer", [x for x in LAYERS if x in set(available["layer"])], horizontal=True
        )
        table = st.selectbox("Table", available.loc[available["layer"] == layer, "name"])
        columns, preview = st.columns([1, 3])
        columns.dataframe(
            query(f'describe {layer}."{table}"')[["column_name", "column_type"]], hide_index=True
        )
        preview.dataframe(query(f'select * from {layer}."{table}" limit 200'), hide_index=True)

with categories_tab:
    if "category_decision" not in set(available.loc[available["layer"] == "gold", "name"]):
        st.info(
            "No Google data yet. Preview the pull with "
            "`shadow-signals run --mode live --google live --dry-run`, then run it without "
            "`--dry-run`."
        )
    else:
        st.subheader("Category comparison")
        st.caption(
            "Registers first (Vilnius companies in the category's activity classes), then what "
            "Google shows, then the two joined. Shares are 0-1."
        )
        decision = query("select * from gold.category_decision").set_index("category")
        st.dataframe(decision.T.map(readable), width="stretch")

        st.subheader("Distributions")
        st.caption(
            "How Google's numbers and the joined declared numbers are spread within each "
            "category. Company metrics only cover high and medium confidence matches."
        )
        distributions = query("select * from gold.google_distributions")
        metric_col, category_col = st.columns([1, 2])
        metric = metric_col.selectbox("Metric", sorted(distributions["metric"].unique()))
        categories = sorted(distributions["category"].unique())
        chosen = category_col.multiselect("Categories", categories, default=categories)
        selected = distributions[
            (distributions["metric"] == metric) & distributions["category"].isin(chosen)
        ]
        st.bar_chart(
            selected.pivot(index="bucket", columns="category", values="share"),
            stack=False,
            y_label="share of the category",
        )
        with st.expander("Counts behind the chart"):
            st.dataframe(selected.pivot(index="bucket", columns="category", values="n"))

        st.subheader("Join checks")
        st.caption(
            "One row per category and per method behind the chosen company. Low category "
            "consistency or many website disagreements mean a method links the wrong companies."
        )
        st.dataframe(
            query("select * from gold.match_quality order by category, method"), hide_index=True
        )

        st.subheader("Check matches by hand")
        st.caption(
            "A fixed sample per confidence level. Mark each row, read the precision below and "
            "download the labels; nothing is saved to the warehouse."
        )
        sample = query("select * from gold.match_review_sample order by confidence, pick")
        sample["is_correct"] = sample["is_correct"].astype("object")
        labelled = st.data_editor(
            sample,
            hide_index=True,
            disabled=[c for c in sample.columns if c != "is_correct"],
            column_config={
                "google_maps_uri": st.column_config.LinkColumn("google maps"),
                "website": st.column_config.LinkColumn(),
                "is_correct": st.column_config.SelectboxColumn(options=[True, False]),
            },
        )
        checked = labelled[labelled["is_correct"].notna()]
        if not checked.empty:
            precision = checked.groupby("confidence")["is_correct"].agg(
                checked="count", correct=lambda s: int(s.eq(True).sum())
            )
            precision["precision"] = (precision["correct"] / precision["checked"]).round(2)
            st.dataframe(precision)
        st.download_button(
            "Download labels (CSV)", labelled.to_csv(index=False), "match_review.csv", "text/csv"
        )

with sql_tab:
    example = st.selectbox("Examples", list(EXAMPLES))
    text = st.text_area("Query (read-only)", EXAMPLES[example], height=220)
    if st.button("Run", type="primary"):
        try:
            result = query(text)
            st.caption(f"{len(result):,} row(s), first {MAX_ROWS:,} at most")
            st.dataframe(result, hide_index=True)
        except duckdb.Error as e:
            st.error(str(e))
