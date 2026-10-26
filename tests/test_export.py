import json
from pathlib import Path

import duckdb

from shadow_signals.export import export_outputs
from shadow_signals.warehouse import history_leads_path, load_bronze


def build_gold(path):
    with duckdb.connect(path) as con:
        con.execute("create schema gold; create schema meta")
        con.execute("""create table gold.company_assessment as select
            '2026-10' as snapshot_month, 'bars' as category, '111111111' as company_code,
            'UAB Kava' as company_name, 'review' as tier, 2 as score, 500 as reviews,
            ['few staff'] as reasons, ['found by name'] as context,
            ['UAB Kita (3 insured staff)'] as same_trade_neighbours, ['p1'] as place_ids,
            date '2026-08-01' as sodra_month, 'g1' as google_run_id, 's1' as sodra_run_id""")
        con.execute("""create table gold.leads as select 1 as rank, snapshot_month, tier, score,
            category, company_code, company_name, reasons, context, ['Kava'] as place_names,
            ['Gedimino pr. 1'] as place_addresses, ['https://maps.google.com/?cid=1']
            as google_maps_links, ['same name'] as match_evidence, place_ids
            from gold.company_assessment""")
        con.execute("create table gold.category_summary as select 'bars' as category, 1 as leads")
        con.execute("""create table gold.lead_changes as select 'new' as change,
            'bars' as category, 'UAB Kava' as company_name""")
        con.execute("""create table meta.bronze_runs as select 'google' as source,
            'places' as dataset, 'g1' as run_id, now() as finished_at, true as is_latest""")


def test_export_writes_outputs_and_the_next_load_reads_them_as_history(settings, ctx, tmp_path):
    settings = settings.model_copy(update={"output_dir": str(tmp_path / "output")})
    build_gold(settings.warehouse_path)

    month = export_outputs(settings, ctx.lake)

    assert month == "2026-10"
    out = Path(settings.output_dir) / month
    assert {p.name for p in out.iterdir()} == {
        "leads.csv",
        "category_summary.csv",
        "lead_changes.csv",
        "company_assessment.csv",
        "run_info.json",
    }
    assert "few staff" in (out / "leads.csv").read_text(encoding="utf-8")
    assert json.loads((out / "run_info.json").read_text())["google_run_id"] == "g1"
    assert ctx.lake.fs.exists(history_leads_path(ctx.lake, month))

    load_bronze(settings, ctx.lake)
    with duckdb.connect(settings.warehouse_path, read_only=True) as con:
        history = con.execute(
            "select snapshot_month, company_code, tier from history.leads"
        ).fetchall()
    assert history == [("2026-10", "111111111", "review")]


def test_export_skips_a_warehouse_without_leads(settings, ctx):
    duckdb.connect(settings.warehouse_path).close()
    assert export_outputs(settings, ctx.lake) is None
