"""Monthly outputs. The lead list of each month is kept in the lake as Parquet (the history the
next run compares with), and written as CSV files the analyst can open or share."""

import json
import logging
import posixpath
import tempfile
from pathlib import Path

import duckdb

from shadow_signals.lake import Lake, utc_now
from shadow_signals.settings import Settings
from shadow_signals.warehouse import HISTORY_LEADS_COLUMNS, history_leads_path, warehouse_path

log = logging.getLogger(__name__)

CSV_TABLES = {
    "leads.csv": """select * replace (
            array_to_string(reasons, '; ') as reasons,
            array_to_string(context, '; ') as context,
            array_to_string(place_names, '; ') as place_names,
            array_to_string(place_addresses, '; ') as place_addresses,
            array_to_string(google_maps_links, ' ') as google_maps_links,
            array_to_string(match_evidence, '; ') as match_evidence,
            array_to_string(place_ids, ' ') as place_ids)
        from gold.leads order by rank""",
    "category_summary.csv": "select * from gold.category_summary order by category",
    "lead_changes.csv": "select * from gold.lead_changes order by change, category, company_name",
    "company_assessment.csv": """select * replace (
            array_to_string(reasons, '; ') as reasons,
            array_to_string(context, '; ') as context,
            array_to_string(same_trade_neighbours, '; ') as same_trade_neighbours,
            array_to_string(place_ids, ' ') as place_ids)
        from gold.company_assessment order by category, tier, score desc, reviews desc""",
}


def export_outputs(settings: Settings, lake: Lake) -> str | None:
    """Write this month's lead history and CSV outputs; returns the month, or None if the
    warehouse has no lead list (no Google data)."""
    # same connection settings as dbt's: within `run`, dbt's connection to the file is still open
    with duckdb.connect(str(warehouse_path(settings))) as con:
        exists = con.execute(
            "select count(*) from information_schema.tables "
            "where table_schema = 'gold' and table_name = 'leads'"
        ).fetchone()[0]
        if not exists:
            log.warning("no gold.leads in the warehouse; nothing to export")
            return None
        month = con.execute("select max(snapshot_month) from gold.company_assessment").fetchone()[0]

        with tempfile.TemporaryDirectory() as tmp:
            local = Path(tmp) / "leads.parquet"
            columns = ", ".join(HISTORY_LEADS_COLUMNS)
            con.execute(
                f"copy (select {columns} from gold.leads) to '{local.as_posix()}' (format parquet)"
            )
            lake.write_bytes(history_leads_path(lake, month), local.read_bytes())

        out_dir = Path(settings.output_dir) / month
        out_dir.mkdir(parents=True, exist_ok=True)
        for name, sql in CSV_TABLES.items():
            path = out_dir / name
            con.execute(f"copy ({sql}) to '{path.as_posix()}' (header, delimiter ',')")
            # a byte-order mark makes Excel read the Lithuanian letters as UTF-8
            path.write_bytes(b"\xef\xbb\xbf" + path.read_bytes())

        info = con.execute(
            """select max(sodra_month)::varchar, max(google_run_id), max(sodra_run_id)
               from gold.company_assessment"""
        ).fetchone()
        runs = con.execute(
            "select source || '/' || dataset, run_id, finished_at::varchar "
            "from meta.bronze_runs where is_latest order by 1"
        ).fetchall()
    (out_dir / "run_info.json").write_text(
        json.dumps(
            {
                "snapshot_month": month,
                "exported_at": str(utc_now()),
                "sodra_month": info[0],
                "google_run_id": info[1],
                "sodra_run_id": info[2],
                "bronze_runs": {key: {"run_id": r, "finished_at": f} for key, r, f in runs},
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    log.info(
        "exported %s: history %s, files in %s",
        month,
        posixpath.dirname(history_leads_path(lake, month)),
        out_dir,
    )
    return month
