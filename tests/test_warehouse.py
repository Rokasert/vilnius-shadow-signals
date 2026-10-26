import duckdb

from shadow_signals.ingest.common import finish_csv
from shadow_signals.warehouse import bronze_tables, load_bronze


def land_csv(lake, source, dataset, text, delimiter=","):
    with lake.bronze_run(source, dataset, "2026-10", "1") as run:
        run.manifest.format = {"type": "csv", "delimiter": delimiter, "compression": "gzip"}
        entry = run.write_stream("part-0001.csv.gz", [text.encode()], compress=True)
        finish_csv(run, entry, delimiter)
    return run.manifest


def test_load_bronze_creates_typed_tables_with_lineage(settings, ctx):
    land_csv(
        ctx.lake, "jar", "legal_entities", "ja_kodas,name\n110000291,Pirmas\n110000334,Antras\n"
    )
    manifest = land_csv(ctx.lake, "sodra", "employers", "code;insured\n1;3\n", delimiter=";")

    tables = load_bronze(settings, ctx.lake)

    assert tables == ['bronze."jar__legal_entities"', 'bronze."sodra__employers"']
    with duckdb.connect(settings.warehouse_path, read_only=True) as con:
        rows = con.execute(
            "select code, insured, _file, _snapshot_month, _run_id from bronze.sodra__employers"
        ).fetchall()
        types = dict(
            con.execute(
                "select column_name, data_type from information_schema.columns "
                "where table_name = 'sodra__employers'"
            ).fetchall()
        )
        runs = con.execute(
            "select source, dataset, records_total, is_latest from meta.bronze_runs order by 1"
        ).fetchall()

    assert rows == [(1, 3, "part-0001.csv.gz", "2026-10", manifest.run_id)]
    assert types["insured"] == "BIGINT"
    assert runs == [("jar", "legal_entities", 2, True), ("sodra", "employers", 1, True)]


def test_bronze_tables_empty_before_any_load(settings, ctx):
    assert bronze_tables(settings) == set()
    land_csv(ctx.lake, "jar", "legal_entities", "ja_kodas\n110000291\n")
    load_bronze(settings, ctx.lake)
    assert bronze_tables(settings) == {"jar__legal_entities"}


def test_load_bronze_reads_nested_json_lines(settings, ctx):
    lines = [
        b'{"category": "bars", "response": {"places": [{"id": "a", "rating": 4.5}]}}\n',
        b'{"category": "bars", "response": {}}\n',
    ]
    with ctx.lake.bronze_run("google", "places", "2026-10", "1") as run:
        run.manifest.format = {"type": "jsonl", "compression": "gzip"}
        run.write_stream("responses.jsonl.gz", lines, compress=True).records = 2

    load_bronze(settings, ctx.lake)

    with duckdb.connect(settings.warehouse_path, read_only=True) as con:
        rows = con.execute(
            "select category, json_extract_string(to_json(response), '$.places[0].id'), _file "
            "from bronze.google__places order by 2 nulls last"
        ).fetchall()
    assert rows == [("bars", "a", "responses.jsonl.gz"), ("bars", None, "responses.jsonl.gz")]


def test_load_bronze_aligns_files_by_column_name(settings, ctx):
    with ctx.lake.bronze_run("sodra", "employers", "2026-10", "1") as run:
        run.manifest.format = {"type": "csv", "delimiter": ";", "compression": "gzip"}
        old = run.write_stream("2025.csv.gz", [b"code;month;insured\n1;202512;3\n"], compress=True)
        finish_csv(run, old, ";")
        new = run.write_stream(
            "2026.csv.gz", [b"code;evrk;month;insured\n1;56.11.00;202601;4\n"], compress=True
        )
        finish_csv(run, new, ";")

    load_bronze(settings, ctx.lake)

    with duckdb.connect(settings.warehouse_path, read_only=True) as con:
        rows = con.execute(
            "select month, evrk, insured from bronze.sodra__employers order by month"
        ).fetchall()
    assert rows == [(202512, None, 3), (202601, "56.11.00", 4)]
    assert run.manifest.warnings == ["2026.csv.gz: columns differ from the first file in this run"]
