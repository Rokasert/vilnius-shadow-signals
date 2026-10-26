import logging
import os
import posixpath
from pathlib import Path

import duckdb
from dbt.cli.main import dbtRunner

from shadow_signals.lake import Lake, Manifest
from shadow_signals.settings import Settings

log = logging.getLogger(__name__)


def warehouse_path(settings: Settings) -> Path:
    path = Path(settings.warehouse_path).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def run_dbt(settings: Settings, lake: Lake, *args: str) -> None:
    os.environ["WAREHOUSE_PATH"] = warehouse_path(settings).as_posix()
    os.environ["LAKE_ROOT"] = lake.root
    os.environ.setdefault("DBT_SEND_ANONYMOUS_USAGE_STATS", "False")

    project_dir = settings.dbt_project_dir
    result = dbtRunner().invoke(
        [*args, "--project-dir", project_dir, "--profiles-dir", project_dir]
    )
    if not result.success:
        raise RuntimeError(f"dbt {' '.join(args)} failed") from result.exception


def bronze_tables(settings: Settings) -> set[str]:
    path = warehouse_path(settings)
    if not path.exists():
        return set()
    with duckdb.connect(str(path), read_only=True) as con:
        rows = con.execute(
            "select table_name from information_schema.tables where table_schema = 'bronze'"
        ).fetchall()
    return {r[0] for r in rows}


def load_bronze(settings: Settings, lake: Lake) -> list[str]:
    """Copy the newest complete run of every bronze dataset into DuckDB (schema `bronze`),
    plus run metadata (schema `meta`). The lake stays the source of truth; this is a cache
    that can be rebuilt at any time."""
    latest = lake.latest_manifests()
    with duckdb.connect(str(warehouse_path(settings))) as con:
        con.execute("create schema if not exists bronze")
        con.execute("create schema if not exists meta")
        tables = [_load_dataset(con, lake, m) for m in latest if m.files]
        _load_run_metadata(con, lake, latest)
    return tables


def _load_dataset(con: duckdb.DuckDBPyConnection, lake: Lake, manifest: Manifest) -> str:
    table = f'bronze."{manifest.source}__{manifest.dataset}"'
    run_dir = lake.run_dir(manifest)
    files = ", ".join(_sql_str(lake.uri(posixpath.join(run_dir, f.path))) for f in manifest.files)
    lineage = (
        f"replace(filename, {_sql_str(lake.uri(run_dir) + '/')}, '') as _file, "
        f"{_sql_str(manifest.snapshot_month)} as _snapshot_month, "
        f"{_sql_str(manifest.run_id)} as _run_id"
    )
    common = "filename = true, hive_partitioning = false"
    if manifest.format.get("type") == "jsonl":
        # nested API responses: let DuckDB infer structs; if that fails keep each line as JSON
        readers = [
            f"read_json([{files}], format = 'newline_delimited', union_by_name = true, "
            f"sample_size = -1, {common})",
            f"read_json_objects([{files}], format = 'newline_delimited', {common})",
        ]
    else:
        delimiter = _sql_str(manifest.format.get("delimiter", ","))
        readers = [
            f"read_csv([{files}], delim = {delimiter}, header = true, union_by_name = true, "
            f"{common}, {options})"
            for options in ("sample_size = -1", "all_varchar = true")
        ]
    for reader in readers:
        try:
            con.execute(
                f"create or replace table {table} as "
                f"select * exclude (filename), {lineage} from {reader}"
            )
            break
        except duckdb.Error as e:
            log.warning("load %s failed (%s); retrying with looser types", table, e)
    else:
        raise RuntimeError(f"could not load {manifest.key}")
    log.info("loaded %s", table)
    return table


def _load_run_metadata(con: duckdb.DuckDBPyConnection, lake: Lake, latest: list[Manifest]) -> None:
    latest_ids = {(m.key, m.run_id) for m in latest}
    con.execute(
        """create or replace table meta.bronze_runs (
             source varchar, dataset varchar, snapshot_month varchar, run_id varchar,
             is_latest boolean, started_at timestamptz, finished_at timestamptz,
             records_total bigint, files integer, bytes bigint, columns integer,
             schema_fingerprint varchar, source_as_of varchar, warnings varchar[],
             request_url varchar)"""
    )
    rows = [
        (
            m.source,
            m.dataset,
            m.snapshot_month,
            m.run_id,
            (m.key, m.run_id) in latest_ids,
            m.started_at,
            m.finished_at,
            m.records_total,
            len(m.files),
            sum(f.bytes for f in m.files),
            len(m.columns),
            m.schema_fingerprint,
            m.source_as_of,
            m.warnings,
            m.request.get("url"),
        )
        for m in lake.manifests()
    ]
    if rows:
        con.executemany(
            f"insert into meta.bronze_runs values ({', '.join(['?'] * len(rows[0]))})", rows
        )
    runs = lake.fs.glob(lake.path("_runs", "*.json"))
    if runs:
        uris = ", ".join(_sql_str(lake.uri(p)) for p in runs)
        con.execute(
            f"create or replace table meta.pipeline_runs as "
            f"select * from read_json([{uris}], union_by_name = true)"
        )


def _sql_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"
