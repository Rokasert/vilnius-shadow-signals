import gzip
import json

import httpx
import pytest
from pydantic import SecretStr

from shadow_signals.ingest import places, websites
from shadow_signals.ingest.places import Search, Tile


@pytest.fixture
def google_settings(settings, tmp_path):
    seeds = tmp_path / "transform" / "seeds"
    seeds.mkdir(parents=True)
    (seeds / "category_searches.csv").write_text(
        "category,google_type,text_query\nbars,bar,baras\ndriving_schools,,vairavimo mokykla\n",
        encoding="utf-8",
    )
    return settings.model_copy(
        update={
            "dbt_project_dir": str(tmp_path / "transform"),
            "google_maps_api_key": SecretStr("test-key"),
            "places_grid": 1,
            "places_max_depth": 1,
            "places_base_url": "https://places.test",
            "crawl_delay_s": 0,
        }
    )


@pytest.fixture
def google_ctx(ctx, google_settings):
    return ctx.__class__(
        snapshot_month=ctx.snapshot_month,
        mode="live",
        settings=google_settings,
        lake=ctx.lake,
        google_mode="live",
    )


def place(i, website=None):
    p = {"id": f"p{i}", "displayName": {"text": f"Place {i}"}}
    if website:
        p["websiteUri"] = website
    return p


def test_tile_split_covers_parent():
    parent = Tile(54.0, 25.0, 55.0, 26.0)
    children = parent.split()
    assert {c.depth for c in children} == {1}
    assert min(c.low_lat for c in children) == 54.0 and max(c.high_lat for c in children) == 55.0
    assert min(c.low_lng for c in children) == 25.0 and max(c.high_lng for c in children) == 26.0


def test_search_body_filters_by_type_only_when_given():
    tile = Tile(54.0, 25.0, 55.0, 26.0)
    typed = places.search_body(Search("bars", "bar", "baras"), tile, None)
    untyped = places.search_body(Search("driving_schools", None, "vairavimo mokykla"), tile, "t")
    assert typed["includedType"] == "bar" and typed["strictTypeFiltering"] is True
    assert "includedType" not in untyped and untyped["pageToken"] == "t"


def test_places_fetch_splits_full_tiles_and_never_stores_the_key(google_ctx, make_http):
    calls = []

    def handler(request):
        body = json.loads(request.content)
        calls.append((body["textQuery"], request.headers["x-goog-api-key"]))
        depth0 = body["locationRestriction"]["rectangle"]["low"]["latitude"] == 54.567
        if body["textQuery"] == "baras" and depth0 and len(calls) <= 3:
            page = len(calls)
            response = {"places": [place(page * 100 + i) for i in range(20)]}
            if page < 3:
                response["nextPageToken"] = f"t{page}"
            return httpx.Response(200, json=response)
        return httpx.Response(200, json={"places": [place(len(calls))]})

    with make_http(handler) as http:
        manifest = places.fetch(google_ctx, http)

    # 3 pages for the full start tile, then 4 children; 1 call for the second search
    assert len(calls) == 3 + 4 + 1
    assert {key for _, key in calls} == {"test-key"}
    assert manifest.request["calls"] == 8
    stored = gzip.decompress(
        google_ctx.lake.read_bytes(f"{google_ctx.lake.run_dir(manifest)}/responses.jsonl.gz")
    )
    assert b"test-key" not in stored
    assert len(stored.splitlines()) == manifest.records_total == 8


def test_places_fetch_stops_at_call_cap_and_keeps_what_it_has(google_ctx, make_http):
    def handler(request):
        return httpx.Response(200, json={"places": [place(1)], "nextPageToken": "more"})

    with make_http(handler) as http:
        manifest = places.fetch(google_ctx, http, max_calls=2)

    assert manifest.request["calls"] == 2
    assert manifest.records_total == 2
    assert "call cap of 2 reached" in manifest.warnings[0]


def test_places_fetch_requires_a_key(google_ctx, make_http):
    settings = google_ctx.settings.model_copy(update={"google_maps_api_key": None})
    ctx = google_ctx.__class__(**{**google_ctx.__dict__, "settings": settings})
    with make_http(lambda r: httpx.Response(200)) as http, pytest.raises(RuntimeError):
        places.fetch(ctx, http)


def test_find_codes_needs_a_keyword_for_company_codes():
    text = (
        "Tel. 860012345. UAB Kava, įmonės kodas 304051696, PVM mokėtojo kodas LT100009579210. "
        "Užsakymo nr. 123456789"
    )
    codes, vat = websites.find_codes(text)
    assert [c["code"] for c in codes] == ["304051696"]
    assert [v["code"] for v in vat] == ["100009579210"]


def test_candidate_links_keeps_same_site_contact_pages():
    html = (
        '<a href="/kontaktai">Kontaktai</a><a href="https://other.lt/rekvizitai">x</a>'
        '<a href="/meniu">Meniu</a><a href="/apie-mus">Apie mus</a>'
    )
    links = websites.candidate_links(html, "https://kava.lt/", 3)
    assert links == ["https://kava.lt/kontaktai", "https://kava.lt/apie-mus"]


def test_crawl_site_respects_robots_and_finds_codes(google_settings):
    pages = {
        "https://kava.lt/robots.txt": (200, "text/plain", "User-agent: *\nDisallow: /privatumo"),
        "https://kava.lt/": (
            200,
            "text/html",
            '<title>Kava</title><a href="/rekvizitai">Rekvizitai</a>'
            '<a href="/privatumo-politika">Privatumo politika</a>',
        ),
        "https://kava.lt/rekvizitai": (200, "text/html", "<p>Įmonės kodas: 304051696</p>"),
    }

    def handler(request):
        status, content_type, body = pages.get(str(request.url), (404, "text/html", ""))
        return httpx.Response(status, text=body, headers={"content-type": content_type})

    client = httpx.Client(transport=httpx.MockTransport(handler))
    site = {"url": "https://kava.lt/", "place_ids": {"p1"}}
    records = websites.crawl_site(client, "https://kava.lt", site, google_settings)

    by_url = {r["url"]: r for r in records}
    assert by_url["https://kava.lt/"]["title"] == "Kava"
    assert [c["code"] for c in by_url["https://kava.lt/rekvizitai"]["codes"]] == ["304051696"]
    assert by_url["https://kava.lt/privatumo-politika"]["robots_allowed"] is False
    assert all(r["place_ids"] == ["p1"] for r in records)


def test_is_platform_matches_domains_not_substrings():
    assert websites.is_platform("www.facebook.com")
    assert websites.is_platform("salonas.mytreatwell.lt")
    assert websites.is_platform("rekvizitai.vz.lt")
    assert websites.is_platform("farmer-ocean.tablein.com")
    assert not websites.is_platform("fedex.com")
    assert not websites.is_platform("autoinfo.lt")


def test_sites_from_places_groups_places_by_site_and_skips_social(google_ctx):
    response = {
        "places": [
            place(1, "https://www.kava.lt/meniu"),
            place(2, "https://www.kava.lt/"),
            place(3, "https://facebook.com/kava"),
            place(4),
        ]
    }
    line = json.dumps({"response": response}).encode() + b"\n"
    with google_ctx.lake.bronze_run("google", "places", "2026-10", "1") as run:
        run.manifest.format = {"type": "jsonl", "compression": "gzip"}
        run.write_stream("responses.jsonl.gz", [line], compress=True)

    sites = websites.sites_from_places(google_ctx.lake)

    assert list(sites) == ["https://www.kava.lt"]
    assert sites["https://www.kava.lt"]["place_ids"] == {"p1", "p2"}
