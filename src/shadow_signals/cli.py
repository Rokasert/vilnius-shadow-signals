import logging
import re
import sys
from enum import StrEnum
from typing import Annotated

import duckdb
import typer

from shadow_signals import __version__
from shadow_signals import ingest as ingestion
from shadow_signals.context import RunContext
from shadow_signals.ingest import places
from shadow_signals.lake import Lake, Manifest, new_run_id, utc_now
from shadow_signals.sample import build_sample
from shadow_signals.settings import get_settings
from shadow_signals.warehouse import bronze_tables, load_bronze, run_dbt, warehouse_path

log = logging.getLogger("shadow_signals")

app = typer.Typer(no_args_is_help=True, add_completion=False)

MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


class Mode(StrEnum):
    replay = "replay"
    live = "live"


class Google(StrEnum):
    reuse = "reuse"
    live = "live"


MonthOption = Annotated[
    str | None, typer.Option("--month", help="Snapshot month, YYYY-MM. Default: current month.")
]
ModeOption = Annotated[Mode | None, typer.Option("--mode", help="Default: RUN_MODE setting.")]
SourceOption = Annotated[
    list[str] | None,
    typer.Option("--source", help="Source (`jar`) or dataset (`jar/legal_entities`). Repeatable."),
]
GoogleOption = Annotated[
    Google | None,
    typer.Option(
        "--google",
        help="reuse (default): no Google calls, use the newest saved pull. "
        "live: call the Places API, then crawl the places' websites.",
    ),
]
DryRunOption = Annotated[
    bool, typer.Option("--dry-run", help="With --google live: print the plan and stop.")
]
YesOption = Annotated[
    bool, typer.Option("--yes", "-y", help="Don't ask before calling the Places API.")
]
MaxCallsOption = Annotated[
    int | None,
    typer.Option(
        "--max-calls", help="Cap on Places API calls for this run (default PLACES_MAX_CALLS)."
    ),
]


def parse_month(value: str | None) -> str:
    if value is None:
        return utc_now().strftime("%Y-%m")
    if not MONTH_RE.match(value):
        raise typer.BadParameter(f"expected YYYY-MM, got {value!r}")
    return value


def build_context(month: str | None, mode: Mode | None, google: Google | None = None) -> RunContext:
    settings = get_settings()
    return RunContext(
        snapshot_month=parse_month(month),
        mode=mode.value if mode else settings.run_mode,
        settings=settings,
        lake=Lake(settings.lake_uri),
        google_mode=google.value if google else settings.google_mode,
    )


@app.callback()
def main(verbose: Annotated[bool, typer.Option("--verbose", "-v")] = False) -> None:
    for stream in (sys.stdout, sys.stderr):
        stream.reconfigure(errors="replace")
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        force=True,
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)


def _confirm_google(ctx: RunContext, max_calls: int | None, dry_run: bool, yes: bool) -> None:
    settings = ctx.settings
    cap = max_calls or settings.places_max_calls
    p = places.plan(settings, cap)
    typer.echo("Google Places plan")
    typer.echo(f"  searches ({len(p['searches'])}): {', '.join(p['searches'])}")
    typer.echo(
        f"  area: Vilnius in {p['start_tiles']} tiles; "
        f"busy tiles split up to depth {p['max_depth']}"
    )
    typer.echo(
        f"  first pass: {p['first_pass_min_calls']}-{p['first_pass_max_calls']} calls; "
        "splitting adds more"
    )
    typer.echo(f"  hard cap: {cap} calls for this run")
    typer.echo(
        f"  cost: the first {p['free_calls_per_month']:,} calls each month are free; beyond that "
        f"$35 per 1,000, so at most ${p['max_cost_usd_if_free_tier_used_up']} for this run"
    )
    typer.echo("  afterwards the places' websites are crawled (free; robots.txt respected)")
    if dry_run:
        raise typer.Exit(0)
    if settings.google_maps_api_key is None:
        raise typer.BadParameter("GOOGLE_MAPS_API_KEY is not set in .env")
    if not yes:
        typer.confirm("Call the Google Places API now?", abort=True)


def _ingest(
    ctx: RunContext,
    sources: list[str] | None,
    dry_run: bool = False,
    yes: bool = False,
    max_calls: int | None = None,
) -> dict[str, Manifest | Exception]:
    try:
        keys = ingestion.select(sources)
    except KeyError as e:
        raise typer.BadParameter(f"unknown source(s): {e.args[0]}") from None
    google_live = ctx.mode == "live" and ctx.google_mode == "live"
    if google_live and ingestion.PLACES_KEY in keys:
        _confirm_google(ctx, max_calls, dry_run, yes)
    elif dry_run:
        typer.echo("--dry-run only plans a --google live run in live mode; nothing to do.")
        raise typer.Exit(0)
    log.info(
        "ingest %d dataset(s), month=%s, mode=%s, google=%s",
        len(keys),
        ctx.snapshot_month,
        ctx.mode,
        ctx.google_mode,
    )
    if ctx.mode == "live":
        return ingestion.ingest_live(ctx, keys, max_google_calls=max_calls)
    return ingestion.ingest_replay(ctx, keys)


def _load(ctx: RunContext) -> None:
    tables = load_bronze(ctx.settings, ctx.lake)
    log.info("warehouse: %d bronze table(s) loaded", len(tables))


def _transform(ctx: RunContext) -> None:
    tables = bronze_tables(ctx.settings)
    if not tables:
        log.warning("no bronze data in the warehouse; skipping dbt build")
        return
    # every model is a table rebuilt from bronze anyway; full refresh also lets seeds change shape
    args = ["build", "--full-refresh"]
    if "google__places" not in tables:
        log.warning("no Google data in the warehouse; building without the Google models")
        args += ["--exclude", "tag:google"]
    log.info("dbt %s", " ".join(args))
    run_dbt(ctx.settings, ctx.lake, *args)


@app.command()
def ingest(
    month: MonthOption = None,
    mode: ModeOption = None,
    source: SourceOption = None,
    google: GoogleOption = None,
    dry_run: DryRunOption = False,
    yes: YesOption = False,
    max_calls: MaxCallsOption = None,
) -> None:
    """Land raw source data in the bronze layer."""
    results = _ingest(build_context(month, mode, google), source, dry_run, yes, max_calls)
    if any(isinstance(r, Exception) for r in results.values()):
        raise typer.Exit(1)


@app.command()
def load() -> None:
    """Copy the newest bronze runs into the DuckDB warehouse for exploration."""
    _load(build_context(None, None))


@app.command()
def transform(month: MonthOption = None, mode: ModeOption = None) -> None:
    """Build silver and gold with dbt."""
    _transform(build_context(month, mode))


@app.command()
def run(
    month: MonthOption = None,
    mode: ModeOption = None,
    source: SourceOption = None,
    google: GoogleOption = None,
    dry_run: DryRunOption = False,
    yes: YesOption = False,
    max_calls: MaxCallsOption = None,
) -> None:
    """Ingest, load into the warehouse, then transform."""
    ctx = build_context(month, mode, google)
    run_id = new_run_id()
    record = {
        "run_id": run_id,
        "snapshot_month": ctx.snapshot_month,
        "mode": ctx.mode,
        "google_mode": ctx.google_mode,
        "version": __version__,
        "started_at": utc_now(),
        "status": "failed",
    }
    results = _ingest(ctx, source, dry_run, yes, max_calls)
    try:
        record["sources"] = {
            key: {"status": "failed", "error": str(r)}
            if isinstance(r, Exception)
            else {"status": "ok", "run_id": r.run_id, "records": r.records_total}
            for key, r in results.items()
        }
        _load(ctx)
        _transform(ctx)
        failed = [k for k, r in results.items() if isinstance(r, Exception)]
        record["status"] = "partial" if failed else "succeeded"
    finally:
        record["finished_at"] = utc_now()
        ctx.lake.write_json(ctx.lake.path("_runs", f"run_id={run_id}.json"), record)
        log.info("run %s %s", run_id, record["status"])
    if record["status"] != "succeeded":
        raise typer.Exit(1)


@app.command()
def sample() -> None:
    """Rebuild the replay sample (SAMPLE_URI) from the newest runs, using the built warehouse
    to keep only the rows the pipeline needs."""
    ctx = build_context(None, None)
    out = Lake(ctx.settings.sample_uri)
    if out.fs.exists(out.path("bronze")):
        out.fs.rm(out.path("bronze"), recursive=True)
    written = build_sample(ctx.settings, ctx.lake, out)
    log.info("sample: %d dataset(s) written to %s", len(written), ctx.settings.sample_uri)


@app.command()
def sources() -> None:
    """List the datasets the pipeline can ingest."""
    for key in ingestion.EXTRACTORS:
        typer.echo(key)


@app.command()
def sql(
    query: Annotated[str, typer.Argument(help="SQL to run against the warehouse (read-only).")],
    limit: Annotated[int, typer.Option(help="Maximum rows to print.")] = 50,
) -> None:
    """Query the warehouse, e.g. `shadow-signals sql "show all tables"`."""
    with duckdb.connect(str(warehouse_path(get_settings())), read_only=True) as con:
        typer.echo(con.sql(query).limit(limit))
