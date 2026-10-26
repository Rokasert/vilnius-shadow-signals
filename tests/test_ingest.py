import gzip
import io
import zipfile

import httpx
import pytest

from shadow_signals import ingest
from shadow_signals.ingest import sodra, spinta
from shadow_signals.ingest.spinta import SpintaDataset

SPINTA_CSV = (
    "_type,_id,ja_kodas,ja_pavadinimas\n"
    'x,1,110000291,"UAB ""Pirmas"""\n'
    'x,2,110000334,"Antras, su kableliu\nir nauja eilute"\n'
)
SODRA_CSV = (
    "﻿Draudėjo kodas (code);Pavadinimas (name);Mėnuo (month);"
    "Apdraustųjų skaičius (numInsured)\n"
    '1;"UAB ""Kava""";202507;3\n'
    '1;"UAB ""Kava""";202508;4\n'
)


def zipped(name: str, text: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as zf:
        zf.writestr(name, text.encode())
    return buffer.getvalue()


def test_spinta_dataset_url_quotes_query():
    ds = SpintaDataset(
        "vmi", "taxes", "vmi/ja_mokesciai/Moketojas", 'savivaldybe._id="x y"&metai>=2025'
    )
    assert ds.url("https://get.data.gov.lt/") == (
        "https://get.data.gov.lt/datasets/gov/vmi/ja_mokesciai/Moketojas/:format/csv"
        "?savivaldybe._id=%22x%20y%22&metai>=2025"
    )


def test_spinta_fetch_lands_gzipped_csv_with_manifest(ctx, make_http):
    seen = []

    def handler(request):
        seen.append(str(request.url))
        return httpx.Response(200, text=SPINTA_CSV)

    ds = SpintaDataset("jar", "legal_entities", "rc/jar/iregistruoti/JuridinisAsmuo")
    with make_http(handler) as http:
        manifest = spinta.fetch(ctx, http, ds)

    assert seen == [
        "https://spinta.test/datasets/gov/rc/jar/iregistruoti/JuridinisAsmuo/:format/csv"
    ]
    assert manifest.records_total == 2
    assert manifest.columns == ["_type", "_id", "ja_kodas", "ja_pavadinimas"]
    assert manifest.schema_fingerprint
    assert manifest.http == {"status": 200, "attempts": 1}
    run_dir = ctx.lake.run_dir(manifest)
    stored = ctx.lake.read_bytes(f"{run_dir}/part-0001.csv.gz")
    assert gzip.decompress(stored).decode() == SPINTA_CSV


def test_spinta_fetch_warns_on_empty_table(ctx, make_http):
    with make_http(lambda request: httpx.Response(200, text="_type,_id\n")) as http:
        manifest = spinta.fetch(ctx, http, SpintaDataset("ar", "points", "rc/ar/x/X"))

    assert manifest.records_total == 0
    assert manifest.warnings == ["part-0001.csv.gz: no records"]


def test_sodra_fetch_tolerates_missing_current_year(ctx, make_http):
    def handler(request):
        if "/2025/" in str(request.url):
            return httpx.Response(
                200,
                content=zipped("monthly-2025.csv", SODRA_CSV),
                headers={"last-modified": "Tue, 27 Jan 2026 11:22:59 GMT"},
            )
        return httpx.Response(404)

    with make_http(handler) as http:
        manifest = sodra.fetch(ctx, http)

    assert [f.path for f in manifest.files] == ["monthly-2025.csv.gz"]
    assert manifest.records_total == 2
    assert manifest.source_as_of == "2025-08"
    assert manifest.warnings == [
        "https://sodra.test/imones/downloads/2026/monthly-2026.csv.zip: not published yet (404)"
    ]
    assert manifest.request["urls"][0]["last_modified"] == "Tue, 27 Jan 2026 11:22:59 GMT"


def test_sodra_fetch_fails_when_nothing_downloads(ctx, make_http):
    with (
        make_http(lambda request: httpx.Response(404)) as http,
        pytest.raises(httpx.HTTPStatusError),
    ):
        sodra.fetch(ctx, http)
    assert ctx.lake.manifests() == []


def test_select_matches_sources_and_datasets(monkeypatch):
    monkeypatch.setattr(ingest, "EXTRACTORS", {"jar/a": None, "jar/b": None, "vmi/c": None})

    assert ingest.select(None) == ["jar/a", "jar/b", "vmi/c"]
    assert ingest.select(["jar"]) == ["jar/a", "jar/b"]
    assert ingest.select(["vmi/c", "jar/b"]) == ["jar/b", "vmi/c"]
    with pytest.raises(KeyError):
        ingest.select(["ja"])
