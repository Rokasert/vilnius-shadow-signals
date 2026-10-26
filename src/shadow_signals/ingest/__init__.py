import logging
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

from shadow_signals.context import RunContext
from shadow_signals.http_client import HttpClient
from shadow_signals.ingest import places, sodra, spinta, websites
from shadow_signals.lake import Lake, Manifest

log = logging.getLogger(__name__)

Extractor = Callable[[RunContext, HttpClient], Manifest]

PLACES_KEY = f"{places.SOURCE}/{places.DATASET}"
WEBSITES_KEY = f"{websites.SOURCE}/{websites.DATASET}"
# Sources that cost money or touch third-party websites: only refreshed with --google live,
# in this order (the crawl reads the websites of the newest Places run).
GOOGLE_KEYS = (PLACES_KEY, WEBSITES_KEY)

EXTRACTORS: dict[str, Extractor] = {
    f"{sodra.SOURCE}/{sodra.DATASET}": sodra.fetch,
    **spinta.extractors(),
    PLACES_KEY: places.fetch,
    WEBSITES_KEY: websites.fetch,
}


def select(names: list[str] | None) -> list[str]:
    """Match full keys (`jar/legal_entities`) or whole sources (`jar`)."""
    if not names:
        return list(EXTRACTORS)
    unknown = [n for n in names if not any(k == n or k.startswith(f"{n}/") for k in EXTRACTORS)]
    if unknown:
        raise KeyError(", ".join(unknown))
    return [k for k in EXTRACTORS if any(k == n or k.startswith(f"{n}/") for n in names)]


def ingest_live(
    ctx: RunContext, keys: list[str], max_google_calls: int | None = None
) -> dict[str, Manifest | Exception]:
    settings = ctx.settings
    results: dict[str, Manifest | Exception] = {}

    def run_one(key: str, http: HttpClient, **kwargs) -> None:
        log.info("ingest %s", key)
        try:
            manifest = EXTRACTORS[key](ctx, http, **kwargs)
            results[key] = manifest
            log.info(
                "ingest %s: %s records, %s warning(s)",
                key,
                manifest.records_total,
                len(manifest.warnings),
            )
        except Exception as e:  # one failing source must not stop the others
            log.exception("ingest %s failed", key)
            results[key] = e

    official = [k for k in keys if k not in GOOGLE_KEYS]
    with (
        HttpClient(
            user_agent=settings.http_user_agent,
            timeout_s=settings.http_timeout_s,
            max_attempts=settings.http_max_attempts,
            min_interval_s=settings.http_min_interval_s,
        ) as http,
        ThreadPoolExecutor(settings.ingest_workers) as pool,
    ):
        list(pool.map(lambda key: run_one(key, http), official))

    google = [k for k in GOOGLE_KEYS if k in keys]
    if google and ctx.google_mode != "live":
        for key in google:
            saved = [m for m in ctx.lake.latest_manifests() if m.key == key]
            if saved:
                log.info("%s: reusing saved run %s (no calls made)", key, saved[0].run_id)
            else:
                log.warning("%s: no saved run; use --google live to fetch one", key)
        return results
    with HttpClient(
        user_agent=settings.http_user_agent,
        timeout_s=60,
        max_attempts=3,
        min_interval_s=settings.places_min_interval_s,
    ) as http:
        for key in google:
            if key == PLACES_KEY:
                run_one(key, http, max_calls=max_google_calls)
            elif not isinstance(results.get(PLACES_KEY), Exception):
                run_one(key, http)
    return results


def ingest_replay(ctx: RunContext, keys: list[str]) -> dict[str, Manifest | Exception]:
    """Copy saved runs from the sample lake, so the pipeline works without network or keys."""
    sample = Lake(ctx.settings.sample_uri)
    results: dict[str, Manifest | Exception] = {}
    for manifest in sample.latest_manifests():
        if manifest.key in keys:
            copied = ctx.lake.copy_run_from(sample, manifest)
            log.info(
                "replay %s run %s (%s)",
                manifest.key,
                manifest.run_id,
                "copied" if copied else "present",
            )
            results[manifest.key] = manifest
    if not results:
        log.warning("replay: no saved runs in %s", ctx.settings.sample_uri)
    return results
