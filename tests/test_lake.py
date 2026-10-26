import gzip
import hashlib
import json

import pytest

from shadow_signals.lake import Lake, schema_fingerprint


def test_bronze_run_writes_parts_then_manifest(tmp_path):
    lake = Lake(str(tmp_path / "lake"))
    payload = b'[{"a": 1}, {"a": 2}]'

    with lake.bronze_run("demo", "items", "2026-10", extractor_version="1") as run:
        entry = run.write_part("part-0001.json", payload, records=2)
        run.warn("example warning")

    (manifest,) = lake.manifests("demo", "items", "2026-10")
    assert manifest.run_id == run.manifest.run_id
    assert manifest.records_total == 2
    assert manifest.finished_at is not None
    assert manifest.warnings == ["example warning"]
    assert entry.bytes == len(payload)
    assert entry.sha256 == hashlib.sha256(payload).hexdigest()
    assert lake.read_bytes(run.path("part-0001.json")) == payload
    assert "snapshot_month=2026-10" in run.dir


def test_write_stream_compresses_but_fingerprints_the_original(tmp_path):
    lake = Lake(str(tmp_path / "lake"))
    chunks = [b"a,b\n", b"1,2\n"]

    with lake.bronze_run("demo", "items", "2026-10", "1") as run:
        entry = run.write_stream("part-0001.csv.gz", chunks, compress=True)

    assert entry.compression == "gzip"
    assert entry.bytes == 8
    assert entry.sha256 == hashlib.sha256(b"a,b\n1,2\n").hexdigest()
    assert gzip.decompress(lake.read_bytes(run.path(entry.path))) == b"a,b\n1,2\n"


def test_failed_run_leaves_no_manifest(tmp_path):
    lake = Lake(str(tmp_path / "lake"))

    with pytest.raises(RuntimeError), lake.bronze_run("demo", "items", "2026-10", "1") as run:
        run.write_part("part-0001.json", b"[]", records=0)
        raise RuntimeError("source went away")

    assert lake.manifests("demo") == []
    assert lake.latest_manifest("demo", "items", "2026-10") is None


def test_records_total_unknown_when_any_part_uncounted(tmp_path):
    lake = Lake(str(tmp_path / "lake"))

    with lake.bronze_run("demo", "items", "2026-10", "1") as run:
        run.write_part("part-0001.json", b"[]", records=0)
        run.write_part("part-0002.bin", b"\x00")

    assert lake.latest_manifest("demo", "items", "2026-10").records_total is None


def test_latest_manifests_picks_newest_month_per_dataset(tmp_path):
    lake = Lake(str(tmp_path / "lake"))
    for month in ("2026-09", "2026-10"):
        with lake.bronze_run("demo", "items", month, "1") as run:
            run.write_part("part-0001.json", b"[]", records=0)
    with lake.bronze_run("demo", "other", "2026-09", "1") as run:
        run.write_part("part-0001.json", b"[]", records=0)

    latest = {m.key: m.snapshot_month for m in lake.latest_manifests()}

    assert latest == {"demo/items": "2026-10", "demo/other": "2026-09"}


def test_copy_run_from_other_lake_is_idempotent(tmp_path):
    sample, lake = Lake(str(tmp_path / "sample")), Lake(str(tmp_path / "lake"))
    with sample.bronze_run("demo", "items", "2026-10", "1") as run:
        run.write_part("part-0001.json", b"[1]", records=1)

    assert lake.copy_run_from(sample, run.manifest) is True
    assert lake.copy_run_from(sample, run.manifest) is False
    assert lake.latest_manifests()[0].run_id == run.manifest.run_id


def test_write_json_round_trip(tmp_path):
    lake = Lake(str(tmp_path / "lake"))
    path = lake.path("_runs", "run_id=x.json")

    lake.write_json(path, {"status": "succeeded"})

    assert json.loads(lake.read_bytes(path)) == {"status": "succeeded"}


def test_schema_fingerprint_ignores_field_order():
    assert schema_fingerprint(["b", "a"]) == schema_fingerprint(["a", "b"])
    assert schema_fingerprint(["a"]) != schema_fingerprint(["a", "b"])
