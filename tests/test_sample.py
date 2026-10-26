import gzip
import json

import duckdb

from shadow_signals.lake import Lake
from shadow_signals.sample import filter_csv_run, filter_jsonl_run


def test_filter_csv_run_keeps_matching_rows_and_records_the_source(tmp_path):
    lake, out = Lake(str(tmp_path / "lake")), Lake(str(tmp_path / "sample"))
    with lake.bronze_run("vmi", "taxpayers", "2026-10", "1") as run:
        run.manifest.format = {"type": "csv", "delimiter": ";", "compression": "gzip"}
        data = b"ja_kodas;pavadinimas\n011111111;Kava\n222222222;Arbata\n"
        run.write_stream("part-0001.csv.gz", [data], compress=True).records = 2

    with duckdb.connect() as con:
        sample = filter_csv_run(con, lake, run.manifest, out, "ja_kodas = '011111111'")

    assert sample.run_id == run.manifest.run_id and sample.records_total == 1
    assert sample.request["sample_of"]["records_total"] == 2
    written = gzip.decompress(out.read_bytes(f"{out.run_dir(sample)}/part-0001.csv.gz")).decode()
    # raw text survives: the leading zero is kept, the delimiter is unchanged
    assert written.splitlines() == ["ja_kodas;pavadinimas", "011111111;Kava"]
    assert [m.key for m in out.latest_manifests()] == ["vmi/taxpayers"]


def test_filter_jsonl_run_keeps_raw_lines(tmp_path):
    lake, out = Lake(str(tmp_path / "lake")), Lake(str(tmp_path / "sample"))
    lines = [{"place_ids": ["p1"], "codes": []}, {"place_ids": ["p2"], "codes": []}]
    with lake.bronze_run("web", "company_codes", "2026-10", "1") as run:
        run.manifest.format = {"type": "jsonl", "compression": "gzip"}
        payload = [json.dumps(line).encode() + b"\n" for line in lines]
        run.write_stream("pages.jsonl.gz", payload, compress=True).records = 2

    sample = filter_jsonl_run(lake, run.manifest, out, lambda r: "p2" in r["place_ids"])

    assert sample.records_total == 1
    written = gzip.decompress(out.read_bytes(f"{out.run_dir(sample)}/pages.jsonl.gz"))
    assert written == payload[1]
