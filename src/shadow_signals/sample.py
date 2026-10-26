"""The replay sample: the newest bronze run of every dataset, cut down to the rows the pipeline
needs for the categories in scope, so `docker compose up` works on a clean machine without keys.

Rows are chosen from the built warehouse: every Google place found by a search in the
category_searches seed, every company that could be matched to one of those places (and every
company sharing such a company's name, so name matching gives the same result), and every
company whose activity belongs to a category. The official records kept are those of these
companies, their addresses and their filings. Small classifier tables are kept whole. Each
sample manifest records the run it was cut from."""

import gzip
import hashlib
import json
import logging
import posixpath
from collections.abc import Callable

import duckdb

from shadow_signals.lake import MANIFEST_NAME, FileEntry, Lake, Manifest
from shadow_signals.settings import Settings
from shadow_signals.warehouse import warehouse_path

log = logging.getLogger(__name__)

KEEP_WHOLE = {
    "ar/municipalities",
    "jar/legal_forms",
    "jar/legal_statuses",
    "lsd/evrk_2",
    "lsd/evrk_2_1",
    "vmi/audit_assessments_by_sector",
    "vmi/control_actions_by_sector",
    "vmi/vat_returns_by_sector",
}

# profit-and-loss lines silver.company_financials reads
FINANCIAL_LINES = (
    "PARDAVIMO PAJAMOS",
    "PAJAMOS",
    "PELNAS (NUOSTOLIAI) PRIEŠ APMOKESTINIMĄ",
    "VEIKLOS REZULTATAS PRIEŠ APMOKESTINIMĄ",
    "GRYNASIS PELNAS (NUOSTOLIAI)",
    "ATASKAITINIŲ METŲ PELNAS (NUOSTOLIAI)",
    "GRYNASIS VEIKLOS REZULTATAS",
)
_LINES = ", ".join("'" + line.replace("'", "''") + "'" for line in FINANCIAL_LINES)

# which raw rows to keep, as a condition on the raw (text) columns
CSV_FILTERS = {
    "jar/legal_entities": "ja_kodas in (select id from keep_companies)",
    "jar/registered_addresses": '"juridinis_asmuo._id" in (select id from keep_jar_ids)',
    # the latest filing of each line only, as silver.company_financials keeps
    "jar/profit_loss_statements": (
        '"juridinis_asmuo._id" in (select id from keep_jar_ids) '
        f"and upper(line_name) in ({_LINES}) "
        'qualify row_number() over (partition by "juridinis_asmuo._id", laikotarpis_nuo, '
        "laikotarpis_iki, line_type_id order by reg_date desc, _id) = 1"
    ),
    "jar/missing_financial_statements": '"juridinis_asmuo._id" in (select id from keep_jar_ids)',
    "jar/late_financial_statements": '"juridinis_asmuo._id" in (select id from keep_jar_ids)',
    "ar/premises": '"aob_kodas._id" in (select id from keep_addresses)',
    "ar/buildings_vilnius": (
        '"aob_kodas._id" in (select id from keep_addresses) '
        "or _id in (select id from keep_buildings)"
    ),
    "ar/streets": "_id in (select id from keep_streets)",
    "lsd/legal_entity_activities": "kodas in (select id from keep_companies)",
    "sodra/employers_monthly": (
        '"Juridinių asmenų registro kodas (jarCode)" in (select id from keep_companies)'
    ),
    "uzt/job_vacancies_vilnius": "jar_kodas in (select id from keep_companies)",
    "vmi/taxpayers": "ja_kodas in (select id from keep_companies)",
    "vmi/tax_arrears": "ja_kodas in (select id from keep_companies)",
    "vmi/taxes_paid_vilnius": "_id in (select id from keep_tax_rows)",
}


def build_sample(settings: Settings, lake: Lake, out: Lake) -> list[Manifest]:
    if not (lake.is_local and out.is_local):
        raise RuntimeError("the sample is built from and into local folders")
    with duckdb.connect() as con:
        con.execute(f"attach '{warehouse_path(settings).as_posix()}' as wh (read_only)")
        _keep_sets(con)
        place_ids = {r[0] for r in con.execute("select id from keep_places").fetchall()}
        searches = {r[0] for r in con.execute("select search from keep_searches").fetchall()}

        def in_scope_search(record: dict) -> bool:
            return (record.get("google_type") or record.get("text_query")) in searches

        def about_kept_place(record: dict) -> bool:
            return bool(place_ids & set(record.get("place_ids") or []))

        written = []
        for manifest in lake.latest_manifests():
            key = manifest.key
            if key == "google/places":
                written.append(filter_jsonl_run(lake, manifest, out, in_scope_search))
            elif key == "web/company_codes":
                written.append(filter_jsonl_run(lake, manifest, out, about_kept_place))
            elif key in KEEP_WHOLE:
                written.append(filter_csv_run(con, lake, manifest, out, "true"))
            elif key in CSV_FILTERS:
                written.append(filter_csv_run(con, lake, manifest, out, CSV_FILTERS[key]))
            else:
                log.warning("sample: no rule for %s, left out", key)
                continue
            log.info(
                "sample %s: %s of %s records",
                key,
                written[-1].records_total,
                manifest.records_total,
            )
    return written


def _keep_sets(con: duckdb.DuckDBPyConnection) -> None:
    """Ids to keep, as text so they compare with raw columns."""
    con.execute("""
        create temp table keep_searches as
        select coalesce(google_type, text_query) as search from wh.reference.category_searches;

        create temp table keep_places as select place_id as id from wh.silver.places;

        -- companies with their seat or a Sodra employer record in Vilnius
        create temp table vilnius_companies as
        select company_code from wh.silver.company_registered_addresses where is_vilnius_address
        union
        select company_code from wh.silver.employer_months
        where municipality = 'Vilniaus m. sav.' and company_code is not null;

        create temp table candidate_companies as
        select distinct company_code from wh.silver.place_company_candidates
        union
        select c.company_code
        from wh.silver.companies as c
        inner join wh.reference.category_activity_classes as cc using (activity_class)
        where c.company_code in (select company_code from vilnius_companies)
        union
        select e.company_code
        from wh.silver.employer_months as e
        inner join wh.reference.category_activity_classes as cc
            on left(e.activity_code, 4) = cc.activity_class
        where e.company_code in (select company_code from vilnius_companies);

        -- companies sharing a kept company's name, so name matching finds the same candidates
        create temp table keep_companies as
        select distinct c.company_code as id
        from wh.silver.companies as c
        where not c.is_name_hidden and c.name_key in (
            select name_key from wh.silver.companies
            where company_code in (select company_code from candidate_companies)
        )
        union select company_code from candidate_companies;

        create temp table keep_jar_ids as
        select distinct cast(_id as varchar) as id from wh.bronze.jar__legal_entities
        where cast(ja_kodas as varchar) in (select id from keep_companies);

        create temp table keep_addresses as
        select distinct cast("adresas._id" as varchar) as id
        from wh.bronze.jar__registered_addresses
        where cast("juridinis_asmuo._id" as varchar) in (select id from keep_jar_ids);

        create temp table keep_buildings as
        select distinct cast("pastatas._id" as varchar) as id from wh.bronze.ar__premises
        where cast("aob_kodas._id" as varchar) in (select id from keep_addresses);

        create temp table keep_streets as
        select distinct cast("gatve._id" as varchar) as id from wh.bronze.ar__buildings_vilnius
        where cast("aob_kodas._id" as varchar) in (select id from keep_addresses)
            or cast(_id as varchar) in (select id from keep_buildings);

        create temp table keep_tax_rows as
        select distinct cast(source_id as varchar) as id from wh.silver.company_taxes_paid
        where company_code in (select id from keep_companies);
    """)


def _sample_manifest(manifest: Manifest, files: list[FileEntry]) -> Manifest:
    sample = manifest.model_copy(deep=True)
    sample.files = files
    sample.records_total = sum(f.records or 0 for f in files)
    sample.request = {
        **manifest.request,
        "sample_of": {
            "records_total": manifest.records_total,
            "files": [f.model_dump() for f in manifest.files],
        },
    }
    sample.warnings = [*manifest.warnings, "replay sample: only the rows the pipeline needs"]
    return sample


def _write_gzip(out: Lake, path: str, data: bytes) -> None:
    out.write_bytes(path, gzip.compress(data, mtime=0))


def filter_csv_run(
    con: duckdb.DuckDBPyConnection, lake: Lake, manifest: Manifest, out: Lake, condition: str
) -> Manifest:
    source_dir = lake.run_dir(manifest)
    target_dir = out.run_dir(manifest)
    files = ", ".join(
        "'" + lake.uri(posixpath.join(source_dir, f.path)).replace("'", "''") + "'"
        for f in manifest.files
    )
    delimiter = manifest.format.get("delimiter", ",")
    name = "part-0001.csv.gz"
    target = posixpath.join(target_dir, name)
    out.fs.makedirs(target_dir, exist_ok=True)
    # hive_partitioning off: the run folders (snapshot_month=…/run_id=…) are not columns
    con.execute(
        f"""copy (
              select * from read_csv([{files}], delim = '{delimiter}', header = true,
                                     all_varchar = true, union_by_name = true,
                                     hive_partitioning = false)
              where {condition}
            ) to '{out.uri(target)}' (header, delimiter '{delimiter}', compression gzip)"""
    )
    raw = gzip.decompress(out.read_bytes(target))
    records = con.execute(
        f"select count(*) from read_csv('{out.uri(target)}', delim = '{delimiter}', "
        "header = true, all_varchar = true, hive_partitioning = false)"
    ).fetchone()[0]
    entry = FileEntry(
        path=name,
        bytes=len(raw),
        sha256=hashlib.sha256(raw).hexdigest(),
        records=records,
        compression="gzip",
    )
    sample = _sample_manifest(manifest, [entry])
    sample.format = {**manifest.format, "compression": "gzip"}
    out.write_bytes(
        posixpath.join(target_dir, MANIFEST_NAME), sample.model_dump_json(indent=2).encode()
    )
    return sample


def filter_jsonl_run(
    lake: Lake, manifest: Manifest, out: Lake, keep: Callable[[dict], bool]
) -> Manifest:
    source_dir = lake.run_dir(manifest)
    target_dir = out.run_dir(manifest)
    entries = []
    for f in manifest.files:
        lines = gzip.decompress(lake.read_bytes(posixpath.join(source_dir, f.path))).splitlines()
        kept = [line + b"\n" for line in lines if line.strip() and keep(json.loads(line))]
        data = b"".join(kept)
        _write_gzip(out, posixpath.join(target_dir, f.path), data)
        entries.append(
            FileEntry(
                path=f.path,
                bytes=len(data),
                sha256=hashlib.sha256(data).hexdigest(),
                records=len(kept),
                compression="gzip",
            )
        )
    sample = _sample_manifest(manifest, entries)
    out.write_bytes(
        posixpath.join(target_dir, MANIFEST_NAME), sample.model_dump_json(indent=2).encode()
    )
    return sample
