import csv
import gzip
import io

import duckdb

from shadow_signals.lake import BronzeRun, FileEntry, schema_fingerprint

CHUNK_SIZE = 1 << 20


def csv_header(run: BronzeRun, entry: FileEntry, delimiter: str) -> list[str]:
    with run.lake.fs.open(run.path(entry.path), "rb") as raw:
        stream = gzip.GzipFile(fileobj=raw) if entry.compression == "gzip" else raw
        line = io.TextIOWrapper(stream, encoding="utf-8-sig", newline="").readline()
    return next(csv.reader([line], delimiter=delimiter)) if line else []


def count_csv_records(run: BronzeRun, entry: FileEntry, delimiter: str) -> int:
    """Parse the whole file rather than counting lines: quoted values can contain newlines."""
    uri = run.lake.uri(run.path(entry.path))
    query = "select count(*) from read_csv(?, delim = ?, header = true, all_varchar = true)"
    with duckdb.connect() as con:
        return con.execute(query, [uri, delimiter]).fetchone()[0]


def finish_csv(run: BronzeRun, entry: FileEntry, delimiter: str) -> None:
    """Record columns, schema fingerprint and record count for a landed CSV file."""
    columns = csv_header(run, entry, delimiter)
    entry.records = count_csv_records(run, entry, delimiter) if columns else 0
    if run.manifest.columns and columns != run.manifest.columns:
        run.warn(f"{entry.path}: columns differ from the first file in this run")
    run.manifest.columns = run.manifest.columns or columns
    run.manifest.schema_fingerprint = schema_fingerprint(run.manifest.columns)
    if entry.records == 0:
        run.warn(f"{entry.path}: no records")
