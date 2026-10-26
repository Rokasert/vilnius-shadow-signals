"""Google Places API (New) Text Search over map tiles covering Vilnius.

One search returns at most 60 places, so a tile that returns 60 is split into four and
searched again. Every response is kept exactly as received, with the request that
produced it. The API key travels only in a request header and is never stored."""

import csv
import json
import logging
from collections import deque
from dataclasses import dataclass
from pathlib import Path

from shadow_signals.context import RunContext
from shadow_signals.http_client import HttpClient
from shadow_signals.lake import Manifest
from shadow_signals.settings import Settings

log = logging.getLogger(__name__)

SOURCE, DATASET = "google", "places"
EXTRACTOR_VERSION = "1"
PAGES_PER_SEARCH = 3
MAX_RESULTS_PER_SEARCH = 60
PRICE_PER_1000_CALLS_USD = 35.0
FREE_CALLS_PER_MONTH = 1000
# Every field below is billed at most at the Text Search Enterprise tier; `reviews` (Enterprise +
# Atmosphere) is deliberately left out.
FIELD_MASK = ",".join(
    [
        "places.id",
        "places.displayName",
        "places.formattedAddress",
        "places.shortFormattedAddress",
        "places.location",
        "places.types",
        "places.primaryType",
        "places.businessStatus",
        "places.googleMapsUri",
        "places.websiteUri",
        "places.nationalPhoneNumber",
        "places.rating",
        "places.userRatingCount",
        "places.regularOpeningHours",
        "places.priceLevel",
        "nextPageToken",
    ]
)


@dataclass(frozen=True)
class Search:
    category: str
    google_type: str | None
    text_query: str

    @property
    def label(self) -> str:
        return f"{self.category}/{self.google_type or self.text_query}"


@dataclass(frozen=True)
class Tile:
    low_lat: float
    low_lng: float
    high_lat: float
    high_lng: float
    depth: int = 0

    def split(self) -> list["Tile"]:
        mid_lat = (self.low_lat + self.high_lat) / 2
        mid_lng = (self.low_lng + self.high_lng) / 2
        return [
            Tile(lat0, lng0, lat1, lng1, self.depth + 1)
            for lat0, lat1 in ((self.low_lat, mid_lat), (mid_lat, self.high_lat))
            for lng0, lng1 in ((self.low_lng, mid_lng), (mid_lng, self.high_lng))
        ]

    def restriction(self) -> dict:
        return {
            "rectangle": {
                "low": {"latitude": self.low_lat, "longitude": self.low_lng},
                "high": {"latitude": self.high_lat, "longitude": self.high_lng},
            }
        }


class CallBudget:
    def __init__(self, limit: int) -> None:
        self.limit = limit
        self.used = 0

    def take(self) -> bool:
        if self.used >= self.limit:
            return False
        self.used += 1
        return True


def load_searches(settings: Settings) -> list[Search]:
    path = Path(settings.dbt_project_dir) / "seeds" / "category_searches.csv"
    with path.open(encoding="utf-8") as f:
        return [
            Search(row["category"], row["google_type"] or None, row["text_query"])
            for row in csv.DictReader(f)
        ]


def initial_tiles(settings: Settings) -> list[Tile]:
    low_lat, low_lng, high_lat, high_lng = settings.places_area
    n = settings.places_grid
    step_lat, step_lng = (high_lat - low_lat) / n, (high_lng - low_lng) / n
    return [
        Tile(
            low_lat + i * step_lat,
            low_lng + j * step_lng,
            low_lat + (i + 1) * step_lat,
            low_lng + (j + 1) * step_lng,
        )
        for i in range(n)
        for j in range(n)
    ]


def plan(settings: Settings, max_calls: int) -> dict:
    searches = load_searches(settings)
    tiles = len(initial_tiles(settings))
    return {
        "searches": [s.label for s in searches],
        "start_tiles": tiles,
        "max_depth": settings.places_max_depth,
        "first_pass_min_calls": tiles * len(searches),
        "first_pass_max_calls": tiles * len(searches) * PAGES_PER_SEARCH,
        "call_cap": max_calls,
        "max_cost_usd_if_free_tier_used_up": round(max_calls * PRICE_PER_1000_CALLS_USD / 1000, 2),
        "free_calls_per_month": FREE_CALLS_PER_MONTH,
    }


def search_body(search: Search, tile: Tile, page_token: str | None) -> dict:
    body: dict = {
        "textQuery": search.text_query,
        "languageCode": "lt",
        "regionCode": "LT",
        "pageSize": 20,
        "locationRestriction": tile.restriction(),
    }
    if search.google_type:
        body["includedType"] = search.google_type
        body["strictTypeFiltering"] = True
    if page_token:
        body["pageToken"] = page_token
    return body


def fetch(ctx: RunContext, http: HttpClient, max_calls: int | None = None) -> Manifest:
    settings = ctx.settings
    if settings.google_maps_api_key is None:
        raise RuntimeError("GOOGLE_MAPS_API_KEY is not set in .env")
    url = f"{settings.places_base_url.rstrip('/')}/v1/places:searchText"
    headers = {
        "X-Goog-Api-Key": settings.google_maps_api_key.get_secret_value(),
        "X-Goog-FieldMask": FIELD_MASK,
    }
    budget = CallBudget(max_calls or settings.places_max_calls)
    lines: list[bytes] = []
    place_ids: set[str] = set()
    incomplete: list[str] = []

    with ctx.lake.bronze_run(SOURCE, DATASET, ctx.snapshot_month, EXTRACTOR_VERSION) as run:
        run.manifest.format = {"type": "jsonl", "compression": "gzip"}
        for search in load_searches(settings):
            tiles = deque(initial_tiles(settings))
            while tiles:
                tile = tiles.popleft()
                found = _search_tile(http, url, headers, search, tile, budget, lines, place_ids)
                if found is None:
                    incomplete.append(search.label)
                    break
                if found >= MAX_RESULTS_PER_SEARCH:
                    if tile.depth < settings.places_max_depth:
                        tiles.extend(tile.split())
                    else:
                        run.warn(f"{search.label}: a tile still returned 60 places at max depth")
            log.info(
                "%s: %d calls so far, %d unique places", search.label, budget.used, len(place_ids)
            )
        if incomplete:
            run.warn(
                f"call cap of {budget.limit} reached; incomplete searches: {', '.join(incomplete)}"
            )
        entry = run.write_stream("responses.jsonl.gz", lines, compress=True)
        entry.records = len(lines)
        run.manifest.request = {
            "url": url,
            "field_mask": FIELD_MASK,
            "searches": [s.label for s in load_searches(settings)],
            "area": settings.places_area,
            "grid": settings.places_grid,
            "max_depth": settings.places_max_depth,
            "calls": budget.used,
            "unique_places": len(place_ids),
        }
    return run.manifest


def _search_tile(
    http: HttpClient,
    url: str,
    headers: dict,
    search: Search,
    tile: Tile,
    budget: CallBudget,
    lines: list[bytes],
    place_ids: set[str],
) -> int | None:
    """Fetch up to three pages for one search in one tile; None when the budget ran out."""
    found = 0
    page_token = None
    for page in range(1, PAGES_PER_SEARCH + 1):
        if not budget.take():
            return None
        body = search_body(search, tile, page_token)
        response = http.request("POST", url, json=body, headers=headers).response.json()
        places = response.get("places", [])
        found += len(places)
        place_ids.update(p["id"] for p in places if "id" in p)
        record = {
            "category": search.category,
            "google_type": search.google_type,
            "text_query": search.text_query,
            "depth": tile.depth,
            "page": page,
            "request": body,
            "response": response,
        }
        lines.append(json.dumps(record, ensure_ascii=False).encode() + b"\n")
        page_token = response.get("nextPageToken")
        if not page_token:
            break
    return found
