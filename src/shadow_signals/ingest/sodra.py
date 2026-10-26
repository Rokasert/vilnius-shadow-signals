"""Sodra open employer data: one zip per year with a row per employer per month
(insured persons, average wage, social insurance contributions, EVRK code)."""

import hashlib
import io
import zipfile

import duckdb
import httpx

from shadow_signals.context import RunContext
from shadow_signals.http_client import HttpClient
from shadow_signals.ingest.common import CHUNK_SIZE, finish_csv
from shadow_signals.lake import BronzeRun, Manifest

EXTRACTOR_VERSION = "1"
SOURCE, DATASET = "sodra", "employers_monthly"
DELIMITER = ";"
MONTH_COLUMN = "Mėnuo (month)"


def year_url(base_url: str, year: int) -> str:
    return f"{base_url.rstrip('/')}/imones/downloads/{year}/monthly-{year}.csv.zip"


def fetch(ctx: RunContext, http: HttpClient) -> Manifest:
    year = int(ctx.snapshot_month[:4])
    with ctx.lake.bronze_run(SOURCE, DATASET, ctx.snapshot_month, EXTRACTOR_VERSION) as run:
        run.manifest.format = {"type": "csv", "delimiter": DELIMITER, "compression": "gzip"}
        run.manifest.request = {"urls": []}
        for y in (year - 1, year):
            _fetch_year(run, http, year_url(ctx.settings.sodra_base_url, y), is_current=y == year)
        if not run.manifest.files:
            raise RuntimeError("no Sodra yearly file could be downloaded")
        run.manifest.source_as_of = _latest_month(run)
    return run.manifest


def _fetch_year(run: BronzeRun, http: HttpClient, url: str, is_current: bool) -> None:
    try:
        streamed = http.stream(url, lambda response: response.read())
    except httpx.HTTPStatusError as e:
        if is_current and e.response.status_code == 404:
            run.warn(f"{url}: not published yet (404)")
            return
        raise
    archive = streamed.value
    with zipfile.ZipFile(io.BytesIO(archive)) as zf:
        (member,) = [m for m in zf.namelist() if m.lower().endswith(".csv")]
        with zf.open(member) as src:
            name = member.rsplit("/", 1)[-1] + ".gz"
            entry = run.write_stream(name, iter(lambda: src.read(CHUNK_SIZE), b""), compress=True)
    run.manifest.request["urls"].append(
        {
            "url": url,
            "zip_sha256": hashlib.sha256(archive).hexdigest(),
            "zip_bytes": len(archive),
            "last_modified": streamed.headers.get("last-modified"),
            "member": member,
            "attempts": streamed.attempts,
        }
    )
    finish_csv(run, entry, DELIMITER)


def _latest_month(run: BronzeRun) -> str | None:
    paths = [run.lake.uri(run.path(f.path)) for f in run.manifest.files]
    query = f"""select max("{MONTH_COLUMN}") from read_csv(?, delim = ?, header = true,
                all_varchar = true)"""
    with duckdb.connect() as con:
        latest = con.execute(query, [paths, DELIMITER]).fetchone()[0]
    return f"{latest[:4]}-{latest[4:6]}" if latest else None
