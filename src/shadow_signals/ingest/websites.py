"""Company and VAT codes printed on the websites of the places found on Google.

Lithuanian business websites often show the company code ("įmonės kodas") on a contact or
details ("rekvizitai") page. The crawler reads the homepage and up to a few linked pages of
that kind, respects robots.txt, waits between requests to the same site, and stores only the
codes it finds with a short text snippet around each, never the pages themselves."""

import gzip
import json
import logging
import posixpath
import re
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor
from html import unescape
from urllib.parse import urljoin, urlsplit
from urllib.robotparser import RobotFileParser

import httpx

from shadow_signals.context import RunContext
from shadow_signals.http_client import HttpClient
from shadow_signals.lake import Lake, Manifest, utc_now

log = logging.getLogger(__name__)

SOURCE, DATASET = "web", "company_codes"
EXTRACTOR_VERSION = "1"
MAX_PAGE_BYTES = 2_000_000
# platforms (social, booking, delivery, directories): a code found there belongs to the
# platform, and one page of theirs stands for every place linking to them
SKIP_HOSTS = (
    "facebook.com",
    "instagram.com",
    "linktr.ee",
    "wolt.com",
    "bolt.eu",
    "google.com",
    "goo.gl",
    "page.link",
    "booking.com",
    "tripadvisor",
    "youtube.com",
    "tiktok.com",
    "linkedin.com",
    "twitter.com",
    "x.com",
    "treatwell",
    "trea.tw",
    "booksy.com",
    "fresha.com",
    "tablein.com",
    "foodout.lt",
    "nevaziuoja.lt",
    "grozionamai.lt",
    "motointegrator.com",
    "rekvizitai.vz.lt",
    "118.lt",
    "imones.lt",
    "info.lt",
    "kasvyksta.lt",
)
LINK_HINTS = (
    "kontakt", "rekvizit", "apie", "about", "contact", "privatum", "privacy",
    "taisykl", "terms", "salyg", "imone", "company", "duomen",
)  # fmt: skip
CODE_KEYWORDS = ("kod", "im. k", "i. k", "imon", "company", "reg", "juridin", "rekviz")
CODE_RE = re.compile(r"(?<!\d)(\d{9})(?!\d)")
VAT_RE = re.compile(r"\bLT\s?(\d{12}|\d{9})(?!\d)")
LINK_RE = re.compile(r"""(?is)<a\s[^>]*href=["']([^"'#]+)["'][^>]*>(.*?)</a>""")
TITLE_RE = re.compile(r"(?is)<title[^>]*>(.*?)</title>")


def plain(text: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFKD", text.lower()) if not unicodedata.combining(c)
    )


def page_text(html: str) -> str:
    html = re.sub(r"(?is)<(script|style|noscript)[^>]*>.*?</\1>", " ", html)
    return re.sub(r"\s+", " ", unescape(re.sub(r"(?s)<[^>]+>", " ", html))).strip()


def find_codes(text: str) -> tuple[list[dict], list[dict]]:
    """A company code must follow a keyword such as "įmonės kodas" with no other number in
    between (phone and order numbers are 9 digits too). VAT codes carry their own 'LT' prefix."""
    codes = []
    for m in CODE_RE.finditer(text):
        before = plain(text[max(0, m.start() - 40) : m.start()])
        keyword_at = max(before.rfind(k) for k in CODE_KEYWORDS)
        if keyword_at >= 0 and not re.search(r"\d", before[keyword_at:]):
            context = text[max(0, m.start() - 80) : m.end() + 20].strip()
            codes.append({"code": m.group(1), "context": context})
    vat = [
        {"code": m.group(1), "context": text[max(0, m.start() - 40) : m.end() + 20].strip()}
        for m in VAT_RE.finditer(text)
    ]
    return codes, vat


def is_platform(host: str) -> bool:
    """A listed domain or one of its subdomains; entries without a dot match anywhere."""
    host = host.lower().removeprefix("www.")
    return any(
        host == skip or host.endswith("." + skip) or ("." not in skip and skip in host)
        for skip in SKIP_HOSTS
    )


def candidate_links(html: str, base_url: str, limit: int) -> list[str]:
    host = urlsplit(base_url).netloc
    links: list[str] = []
    for href, label in LINK_RE.findall(html):
        url = urljoin(base_url, href.strip())
        if urlsplit(url).netloc != host or url in links or url.rstrip("/") == base_url.rstrip("/"):
            continue
        if any(h in plain(href + " " + label) for h in LINK_HINTS):
            links.append(url)
        if len(links) >= limit:
            break
    return links


def sites_from_places(lake: Lake) -> dict[str, dict]:
    """Websites of the newest Places run, one entry per site root, with the places using it."""
    places = [m for m in lake.latest_manifests() if m.key == "google/places"]
    if not places:
        raise RuntimeError("no saved Google Places run to take websites from")
    manifest = places[0]
    sites: dict[str, dict] = {}
    for entry in manifest.files:
        raw = lake.read_bytes(posixpath.join(lake.run_dir(manifest), entry.path))
        for line in gzip.decompress(raw).splitlines():
            for place in json.loads(line)["response"].get("places", []):
                website = place.get("websiteUri")
                if not website or not place.get("id"):
                    continue
                parts = urlsplit(website)
                host = parts.netloc.lower()
                if not host or is_platform(host):
                    continue
                site = sites.setdefault(
                    f"{parts.scheme}://{host}", {"url": website, "place_ids": set()}
                )
                site["place_ids"].add(place["id"])
    return sites


def crawl_site(client: httpx.Client, root: str, site: dict, settings) -> list[dict]:
    place_ids = sorted(site["place_ids"])
    robots = RobotFileParser()
    try:
        response = client.get(f"{root}/robots.txt")
        robots.parse(response.text.splitlines() if response.status_code == 200 else [])
        robots.disallow_all = response.status_code in (401, 403)
    except httpx.HTTPError:
        robots.parse([])
    agent = settings.http_user_agent

    records: list[dict] = []
    queue = [site["url"]]
    visited: set[str] = set()
    while queue and len(visited) < settings.crawl_max_pages:
        url = queue.pop(0)
        if url in visited:
            continue
        visited.add(url)
        record = {
            "place_ids": place_ids,
            "site": root,
            "url": url,
            "fetched_at": str(utc_now()),
            "status": None,
            "robots_allowed": True,
            "error": None,
            "title": None,
            "codes": [],
            "vat_codes": [],
        }
        if not robots.can_fetch(agent, url):
            records.append({**record, "robots_allowed": False})
            continue
        time.sleep(settings.crawl_delay_s)
        try:
            with client.stream("GET", url) as response:
                body = b""
                for chunk in response.iter_bytes():
                    body += chunk
                    if len(body) > MAX_PAGE_BYTES:
                        break
                final_url = str(response.url)
                record["status"] = response.status_code
                is_html = "html" in response.headers.get("content-type", "")
                encoding = response.charset_encoding or "utf-8"
        except httpx.HTTPError as e:
            records.append({**record, "error": type(e).__name__})
            continue
        html = body.decode(encoding, errors="replace") if is_html else ""
        codes, vat = find_codes(page_text(html))
        title = TITLE_RE.search(html)
        record["title"] = page_text(title.group(1))[:200] if title else None
        records.append({**record, "codes": codes, "vat_codes": vat})
        if len(visited) == 1 and is_html:
            queue.extend(candidate_links(html, final_url, settings.crawl_max_pages - 1))
    return records


def fetch(ctx: RunContext, http: HttpClient) -> Manifest:
    settings = ctx.settings
    sites = sites_from_places(ctx.lake)
    log.info("crawling %d websites", len(sites))
    client = httpx.Client(
        headers={"User-Agent": settings.http_user_agent},
        timeout=settings.crawl_timeout_s,
        follow_redirects=True,
    )
    with client, ThreadPoolExecutor(settings.crawl_workers) as pool:
        results = list(
            pool.map(lambda item: crawl_site(client, item[0], item[1], settings), sites.items())
        )
    records = [r for site_records in results for r in site_records]

    with ctx.lake.bronze_run(SOURCE, DATASET, ctx.snapshot_month, EXTRACTOR_VERSION) as run:
        run.manifest.format = {"type": "jsonl", "compression": "gzip"}
        lines = [json.dumps(r, ensure_ascii=False).encode() + b"\n" for r in records]
        entry = run.write_stream("pages.jsonl.gz", lines, compress=True)
        entry.records = len(records)
        run.manifest.request = {
            "sites": len(sites),
            "pages": len(records),
            "pages_with_company_codes": sum(1 for r in records if r["codes"]),
            "pages_blocked_by_robots": sum(1 for r in records if not r["robots_allowed"]),
            "skipped_hosts": SKIP_HOSTS,
        }
    return run.manifest
