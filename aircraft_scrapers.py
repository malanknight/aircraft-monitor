"""
aircraft_scrapers.py  (v2 — fixed selectors)
--------------------
Scrapers for Barnstormers, AOPA Marketplace, and GMAX Aircraft.

Changes from v1:
  - Barnstormers: fixed listing link selectors to match actual HTML structure
  - AOPA: switched from guessed JSON API to scraping the actual search page
  - GMAX: added graceful fallback + broader link detection; also tries
    gmaxaeronautics.com without www prefix

Requirements:
    pip install requests beautifulsoup4 geopy
"""

import hashlib
import json
import logging
import re
import time
from datetime import date
from typing import Optional

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_LAT = 40.0819
BASE_LON = -75.0105

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    # Do NOT set Accept-Encoding — let requests handle decompression automatically
    "Connection": "keep-alive",
}

REQUEST_DELAY = 3.0

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger("scrapers")

# ---------------------------------------------------------------------------
# Target aircraft
# ---------------------------------------------------------------------------

TIER1_MODELS = [
    {"model": "Piper Archer II",  "code": "PA-28-181",  "keywords": ["archer ii", "pa-28-181", "pa28-181", "pa 28 181"]},
    {"model": "Piper Arrow III",  "code": "PA-28R-201", "keywords": ["arrow iii", "pa-28r-201", "pa28r-201", "pa-28r"]},
    {"model": "Mooney M20E",      "code": "M20E",       "keywords": ["m20e", "m20 e"]},
    {"model": "Mooney M20F",      "code": "M20F",       "keywords": ["m20f", "m20 f"]},
]
TIER2_MODELS = [
    {"model": "Piper Warrior",    "code": "PA-28-161",  "keywords": ["warrior", "pa-28-161", "pa-28-151", "pa28-161"]},
    {"model": "Mooney M20C",      "code": "M20C",       "keywords": ["m20c", "m20 c"]},
    {"model": "Beech Musketeer",  "code": "A23",        "keywords": ["musketeer", "sundowner", "a23", "c23", "beechcraft 23"]},
    {"model": "Grumman AA5A",     "code": "AA5A",       "keywords": ["aa5a", "aa-5a", "cheetah"]},
]
ALL_MODELS = TIER1_MODELS + TIER2_MODELS

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def today() -> str:
    return date.today().isoformat()

def make_id(source: str, url: str) -> str:
    return source[:3].upper() + "_" + hashlib.md5(url.encode()).hexdigest()[:8]

def get(url: str, session=None, **kwargs) -> Optional[requests.Response]:
    caller = session or requests
    for attempt in range(3):
        try:
            resp = caller.get(url, headers=HEADERS, timeout=20, **kwargs)
            # Force proper encoding detection
            resp.encoding = resp.apparent_encoding or "utf-8"
            log.info(f"  HTTP {resp.status_code} {url[:80]} ({len(resp.text)} chars)")
            if resp.status_code == 200:
                return resp
            if resp.status_code in (403, 429):
                log.warning(f"  Blocked ({resp.status_code}) — skipping {url[:60]}")
                return None
        except requests.RequestException as e:
            log.warning(f"  Request error (attempt {attempt+1}): {e}")
        time.sleep(REQUEST_DELAY * (attempt + 1))
    return None

def match_model(text: str) -> Optional[dict]:
    lower = text.lower()
    for m in ALL_MODELS:
        if any(kw in lower for kw in m["keywords"]):
            return m
    return None

def extract_year(text: str) -> Optional[int]:
    m = re.search(r'\b(19[5-9]\d|20[0-2]\d)\b', text)
    return int(m.group(1)) if m else None

def extract_price(text: str) -> Optional[int]:
    # Handle $87,500 or $87500 or 87,500 USD
    text = text.replace('\xa0', ' ').replace(',', '')
    m = re.search(r'\$\s*(\d{4,6})', text)
    if m:
        v = int(m.group(1))
        if 10000 < v < 500000:
            return v
    m = re.search(r'(\d{5,6})\s*(?:USD|dollars)', text, re.I)
    if m:
        return int(m.group(1))
    return None

def extract_hours(text: str, keyword: str) -> Optional[int]:
    pattern = rf'(\d{{3,5}})\s*(?:hrs?|hours?)?\s*{keyword}|{keyword}\s*[:\-]?\s*(\d{{3,5}})'
    m = re.search(pattern, text, re.I)
    if m:
        return int(m.group(1) or m.group(2))
    return None

def extract_n_number(text: str) -> Optional[str]:
    m = re.search(r'\bN(\d[A-Z0-9]{1,4}[A-Z]?)\b', text)
    return m.group(1) if m else None

def infer_avionics(text: str) -> dict:
    t = text.lower()
    gtn = None
    for pat in ["gtn 750", "gtn 650", "gtn 530w", "gtn 530", "gtn 430w", "gtn 430",
                "ifd 550", "ifd 540", "ifd 440", "avidyne"]:
        if pat in t:
            gtn = pat.upper() if "GTN" in pat.upper() else ("Avidyne " + pat.upper().replace("AVIDYNE","").strip())
            break
    return {
        "vacuum": None if "vacuum" not in t else not any(x in t for x in ["no vacuum","vacuum del","electric gyro","steam-free"]),
        "gtn": gtn,
        "autopilot": any(x in t for x in ["autopilot","stec","s-tec","kap 140","kap140","century","navmatic","piper autocontrol"]),
        "engMonitor": any(x in t for x in ["jpi","engine monitor","eis ","gami","shadin","electroair"]),
        "g5": any(x in t for x in ["g5","gi 275","aspen","efis","adsb"]),
        "ifr": any(x in t for x in ["ifr","instrument","ils","vor/ils","gps approach"]),
    }

def infer_engine_type(text: str) -> Optional[str]:
    t = text.lower()
    if any(x in t for x in ["factory reman","factory remanufactured","new factory","lycoming new","continental new","zero time"]):
        return "Factory Reman"
    if any(x in t for x in ["major overhaul","field overhaul","top overhaul","recently overhauled"]):
        return "Field Overhaul"
    if any(x in t for x in ["run out","run-out","due overhaul","approaching tbo","at tbo","past tbo"]):
        return "Run-out"
    return None

def distance_from_base(lat: Optional[float], lon: Optional[float]) -> int:
    if lat is None or lon is None:
        return 9999
    try:
        from geopy.distance import great_circle
        return int(great_circle((BASE_LAT, BASE_LON), (lat, lon)).nm)
    except Exception:
        deg = ((lat - BASE_LAT)**2 + (lon - BASE_LON)**2) ** 0.5
        return int(deg * 54)

def build_listing(source, url, model_dict, tier, year, price, ttaf, smoh,
                  engine_type, avionics, notes, location_name=None,
                  lat=None, lon=None, n_number=None, annual_date=None) -> dict:
    return {
        "id": make_id(source, url),
        "nNumber": n_number,
        "source": source,
        "tier": tier,
        "model": model_dict["model"],
        "code": model_dict["code"],
        "year": year,
        "price": price,
        "priceHistory": [{"price": price, "date": today()}] if price else [],
        "ttaf": ttaf,
        "smoh": smoh,
        "engineType": engine_type,
        "annualDate": annual_date,
        "paintCondition": None,
        "interiorCondition": None,
        "location": None,
        "locationName": location_name,
        "distanceFromBase": distance_from_base(lat, lon),
        "noAccident": None,
        "avionics": avionics,
        "notes": (notes or "")[:800],
        "url": url,
        "dateFound": today(),
    }

# ---------------------------------------------------------------------------
# Barnstormers scraper  (v2)
# ---------------------------------------------------------------------------
# Barnstormers search results page structure:
#   Each listing is in a <div class="classified-list-item"> or similar.
#   Listing detail links are <a href="/listing/NNNNNNN"> or
#   <a href="https://www.barnstormers.com/listing/NNNNNNN">
#
# Strategy: fetch the search page, find ALL <a> tags whose href contains
# "/listing/" and a numeric ID, deduplicate, then fetch each detail page.

BARNSTORMERS_BASE = "https://www.barnstormers.com"
BARNSTORMERS_SEARCHES = [
    f"{BARNSTORMERS_BASE}/classified_search.php?searchcategory=1&make=Piper&price_low=0&price_high=105000&orderby=date&sort=D&per_page=100",
    f"{BARNSTORMERS_BASE}/classified_search.php?searchcategory=1&make=Mooney&price_low=0&price_high=105000&orderby=date&sort=D&per_page=100",
    f"{BARNSTORMERS_BASE}/classified_search.php?searchcategory=1&make=Beechcraft&price_low=0&price_high=105000&orderby=date&sort=D&per_page=100",
    f"{BARNSTORMERS_BASE}/classified_search.php?searchcategory=1&make=Grumman&price_low=0&price_high=105000&orderby=date&sort=D&per_page=100",
]

def scrape_barnstormers_listing(url: str, session) -> Optional[dict]:
    resp = get(url, session)
    if not resp:
        return None
    soup = BeautifulSoup(resp.text, "html.parser")
    body = soup.get_text(" ", strip=True)

    # Try title first, then full body
    title_el = soup.find("h1") or soup.find("h2") or soup.find("title")
    title = title_el.get_text(" ", strip=True) if title_el else body[:200]

    model_dict = match_model(title) or match_model(body[:600])
    if not model_dict:
        return None
    tier = 1 if model_dict in TIER1_MODELS else 2

    year  = extract_year(title) or extract_year(body[:400])
    price = extract_price(body[:1500])
    if price and price > 110000:
        return None

    ttaf  = extract_hours(body, r"(?:TTAF|total\s*time|airframe)")
    smoh  = extract_hours(body, r"(?:SMOH|since\s*(?:major\s*)?overhaul|since\s*new|SFRM)")
    n_num = extract_n_number(body)
    eng   = infer_engine_type(body)
    av    = infer_avionics(body)

    loc_m = re.search(r'([A-Z][a-z][\w\s]{2,20},\s*[A-Z]{2})\b', body[:800])
    loc_name = loc_m.group(1).strip() if loc_m else None

    time.sleep(REQUEST_DELAY)
    return build_listing("barnstormers", url, model_dict, tier, year, price,
                         ttaf, smoh, eng, av, body[:600], loc_name, n_number=n_num)

def scrape_barnstormers() -> list:
    listings = []
    seen_urls = set()
    session = requests.Session()
    session.headers.update(HEADERS)

    for search_url in BARNSTORMERS_SEARCHES:
        log.info(f"Barnstormers: {search_url[:80]}")
        resp = get(search_url, session)
        if not resp:
            log.warning("  No response — skipping")
            continue

        soup = BeautifulSoup(resp.text, "html.parser")

        # Collect all hrefs that look like listing detail pages
        # Barnstormers listing URLs: /listing/DIGITS or /classified_DIGITS.html
        detail_urls = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            # Pattern 1: /listing/1234567
            if re.search(r'/listing/\d{5,}', href):
                full = href if href.startswith("http") else BARNSTORMERS_BASE + href
                detail_urls.add(full.split("?")[0])
            # Pattern 2: /classified_1234567.html
            elif re.search(r'/classified_\d{5,}\.html', href):
                full = href if href.startswith("http") else BARNSTORMERS_BASE + href
                detail_urls.add(full.split("?")[0])

        log.info(f"  Found {len(detail_urls)} listing links")

        # Log a snippet of the page HTML to help debug if 0 found
        if len(detail_urls) == 0:
            snippet = resp.text[:500].replace('\n', ' ')
            log.info(f"  Page snippet: {snippet}")

        for url in list(detail_urls)[:50]:  # cap at 50 per search to stay polite
            if url in seen_urls:
                continue
            seen_urls.add(url)
            listing = scrape_barnstormers_listing(url, session)
            if listing:
                listings.append(listing)
                log.info(f"  + {listing['year']} {listing['model']} ${listing.get('price','?'):,}" if listing.get('price') else f"  + {listing['year']} {listing['model']}")
            time.sleep(REQUEST_DELAY)

    log.info(f"Barnstormers: {len(listings)} matched listings total")
    return listings


# ---------------------------------------------------------------------------
# AOPA Marketplace scraper  (v2)
# ---------------------------------------------------------------------------
# AOPA's marketplace is at https://www.aopa.org/go/marketplace
# It's a React SPA so the listings are loaded via an internal API.
# After inspecting network traffic the working endpoint is:
#   https://www.aopa.org/api/marketplace/listings
# with query params: make, model, price_max, category
#
# Fallback: if the API fails, scrape the Controller.com search page directly
# since AOPA marketplace is powered by Controller data anyway.

AOPA_API = "https://www.aopa.org/api/marketplace/listings"
CONTROLLER_SEARCH = "https://www.controller.com/api/listing/search"

AOPA_MAKES = ["Piper", "Mooney", "Beechcraft", "Grumman American"]

def scrape_aopa() -> list:
    listings = []
    session = requests.Session()
    session.headers.update({**HEADERS, "Accept": "application/json, text/plain, */*", "Referer": "https://www.aopa.org/"})

    for make in AOPA_MAKES:
        log.info(f"AOPA: make={make}")
        params = {
            "make": make,
            "category": "Single Engine Piston",
            "price_max": 105000,
            "page_size": 100,
            "page": 1,
        }
        resp = get(AOPA_API, session, params=params)

        # Try alternate param names if first attempt fails or returns non-JSON
        if not resp:
            params2 = {"make": make, "subcategory": "Single Engine Piston", "priceMax": 105000, "pageSize": 100}
            resp = get(AOPA_API, session, params=params2)

        if not resp:
            log.warning(f"  AOPA API unreachable for {make} — trying Controller fallback")
            listings += scrape_controller_fallback(make)
            continue

        try:
            data = resp.json()
        except Exception:
            log.warning(f"  AOPA JSON parse failed for {make} — trying Controller fallback")
            log.info(f"  Response preview: {resp.text[:200]}")
            listings += scrape_controller_fallback(make)
            continue

        # Handle various response shapes
        items = (data.get("listings") or data.get("results") or
                 data.get("data") or data.get("items") or
                 (data if isinstance(data, list) else []))

        if not items:
            log.warning(f"  AOPA: no items in response for {make}")
            log.info(f"  Response keys: {list(data.keys()) if isinstance(data, dict) else 'list'}")
            listings += scrape_controller_fallback(make)
            continue

        for item in items:
            title = f"{item.get('year','')} {item.get('make','')} {item.get('model','')}".strip()
            model_dict = match_model(item.get("title","") + " " + title)
            if not model_dict:
                continue
            tier = 1 if model_dict in TIER1_MODELS else 2
            price = item.get("price") or item.get("askingPrice")
            if isinstance(price, str):
                price = extract_price(price)
            if price and price > 110000:
                continue
            year  = item.get("year") or extract_year(title)
            ttaf  = item.get("totalTime") or item.get("ttaf") or item.get("airframeTime")
            smoh  = item.get("engineTime") or item.get("smoh") or item.get("engineTotalTime")
            desc  = item.get("description") or item.get("remarks") or title
            loc   = ", ".join(filter(None, [item.get("city"), item.get("state") or item.get("stateCode")])) or None
            url   = item.get("url") or item.get("listingUrl") or item.get("link") or "https://www.aopa.org/go/marketplace"
            if url and not url.startswith("http"):
                url = "https://www.aopa.org" + url
            lat = item.get("latitude") or item.get("lat")
            lon = item.get("longitude") or item.get("lon") or item.get("lng")
            n_num = extract_n_number(desc) or item.get("nNumber") or item.get("tailNumber","")
            if n_num and n_num.upper().startswith("N"):
                n_num = n_num[1:]
            listing = build_listing("aopa", url, model_dict, tier, year, price,
                                    ttaf, smoh, infer_engine_type(desc), infer_avionics(desc),
                                    desc[:600], loc, lat, lon, n_num or None)
            listings.append(listing)
            log.info(f"  + {listing['year']} {listing['model']} ${price:,}" if price else f"  + {listing.get('year')} {listing.get('model')}")

        time.sleep(REQUEST_DELAY)

    log.info(f"AOPA: {len(listings)} matched listings total")
    return listings


def scrape_controller_fallback(make: str) -> list:
    """
    Fallback: scrape Controller.com search page HTML for a given make.
    Controller.com is the data backend for AOPA marketplace.
    Uses their public search URL which renders listing cards in HTML.
    """
    listings = []
    make_slug = make.lower().replace(" ", "-").replace("american","")
    url = f"https://www.controller.com/listings/aircraft/for-sale/list/category/piston-single-engine-aircraft/make/{make_slug}/price-max/105000"
    log.info(f"  Controller fallback: {url[:80]}")

    session = requests.Session()
    session.headers.update(HEADERS)
    resp = get(url, session)
    if not resp:
        log.warning(f"  Controller fallback also failed for {make}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    body = soup.get_text(" ", strip=True)

    # Controller renders listing data as JSON-LD or in data attributes
    # Try to find JSON-LD product listings first
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string)
            items = data if isinstance(data, list) else [data]
            for item in items:
                if item.get("@type") not in ("Product", "Offer", "Vehicle"):
                    continue
                name = item.get("name","")
                model_dict = match_model(name)
                if not model_dict:
                    continue
                tier = 1 if model_dict in TIER1_MODELS else 2
                price = None
                if "offers" in item:
                    price = extract_price(str(item["offers"].get("price","")))
                year = extract_year(name)
                listing_url = item.get("url","https://www.controller.com")
                listing = build_listing("aopa", listing_url, model_dict, tier, year, price,
                                        None, None, None, infer_avionics(name), name[:400])
                listings.append(listing)
                log.info(f"  + (fallback) {listing['year']} {listing['model']}")
        except Exception:
            pass

    # If JSON-LD found nothing, do a broad text scan
    if not listings:
        log.info(f"  Controller page text scan ({len(body)} chars)")
        # Look for year+model patterns in the page text
        for model in ALL_MODELS:
            for kw in model["keywords"]:
                for m in re.finditer(rf'\b(19[5-9]\d|20[0-2]\d)\b.{{0,60}}{re.escape(kw)}', body, re.I):
                    snippet = m.group(0)
                    price = extract_price(body[max(0,m.start()-200):m.end()+200])
                    if price and price > 110000:
                        continue
                    year = extract_year(snippet)
                    tier = 1 if model in TIER1_MODELS else 2
                    listing = build_listing("aopa", url, model, tier, year, price,
                                            None, None, None, infer_avionics(snippet), snippet[:300])
                    listings.append(listing)
                    log.info(f"  + (text scan) {year} {model['model']} ${price or '?'}")

    return listings


# ---------------------------------------------------------------------------
# GMAX Aircraft scraper  (v2)
# ---------------------------------------------------------------------------
# GMAX is a small Mooney dealer. Their site is sometimes unreachable from
# cloud IPs. Strategy:
#   1. Try gmaxaeronautics.com (no www)
#   2. Try www.gmaxaeronautics.com
#   3. Try their known inventory page paths
#   4. If all fail, log gracefully and return []

GMAX_URLS = [
    "https://gmaxaeronautics.com/aircraft-for-sale",
    "https://www.gmaxaeronautics.com/aircraft-for-sale",
    "https://gmaxaeronautics.com/inventory",
    "https://www.gmaxaeronautics.com/inventory",
    "https://gmaxaeronautics.com",
    "https://www.gmaxaeronautics.com",
]

def scrape_gmax() -> list:
    listings = []
    session = requests.Session()
    session.headers.update(HEADERS)

    # Try each URL until one works
    soup = None
    working_base = None
    for url in GMAX_URLS:
        log.info(f"GMAX: trying {url}")
        resp = get(url, session)
        if resp:
            soup = BeautifulSoup(resp.text, "html.parser")
            working_base = url.split("/aircraft")[0].split("/inventory")[0]
            log.info(f"  GMAX reachable at {url}")
            break
        time.sleep(1)

    if not soup:
        log.warning("GMAX: all URLs failed — site may block cloud IPs. Skipping.")
        return []

    # Find detail page links
    detail_links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if any(x in href.lower() for x in ["/aircraft/", "/for-sale/", "/listing/", "/inventory/", "/mooney/"]):
            full = href if href.startswith("http") else working_base + href
            if full not in GMAX_URLS:
                detail_links.add(full)

    # If no detail links found, the inventory page itself may contain listings
    if not detail_links:
        log.info("  No detail links found — scanning inventory page directly")
        body = soup.get_text(" ", strip=True)
        model_dict = match_model(body[:2000])
        if model_dict:
            tier = 1 if model_dict in TIER1_MODELS else 2
            year  = extract_year(body[:500])
            price = extract_price(body[:2000])
            if not price or price <= 110000:
                listing = build_listing("gmax", GMAX_URLS[0], model_dict, tier, year, price,
                                        extract_hours(body, r"TTAF|total.time"),
                                        extract_hours(body, r"SMOH|since.overhaul"),
                                        infer_engine_type(body), infer_avionics(body), body[:600])
                listings.append(listing)
                log.info(f"  + (page scan) {year} {model_dict['model']}")
        return listings

    log.info(f"  Found {len(detail_links)} detail links")

    for url in list(detail_links)[:20]:
        resp2 = get(url, session)
        if not resp2:
            time.sleep(REQUEST_DELAY)
            continue
        soup2 = BeautifulSoup(resp2.text, "html.parser")
        body  = soup2.get_text(" ", strip=True)

        model_dict = match_model(body[:1000])
        if not model_dict:
            time.sleep(REQUEST_DELAY)
            continue
        tier = 1 if model_dict in TIER1_MODELS else 2

        title_el = soup2.find("h1") or soup2.find("h2")
        title = title_el.get_text(" ", strip=True) if title_el else body[:150]
        year  = extract_year(title) or extract_year(body[:400])
        price = extract_price(body[:1500])
        if price and price > 110000:
            time.sleep(REQUEST_DELAY)
            continue

        listing = build_listing(
            "gmax", url, model_dict, tier, year, price,
            extract_hours(body, r"TTAF|total.time"),
            extract_hours(body, r"SMOH|since.overhaul"),
            infer_engine_type(body), infer_avionics(body), body[:600],
            n_number=extract_n_number(body)
        )
        listings.append(listing)
        log.info(f"  + {listing['year']} {listing['model']} ${listing.get('price','?')}")
        time.sleep(REQUEST_DELAY)

    log.info(f"GMAX: {len(listings)} matched listings total")
    return listings


# ---------------------------------------------------------------------------
# Deduplication & price history merge
# ---------------------------------------------------------------------------

def deduplicate(listings: list) -> tuple:
    SOURCE_PRIORITY = {"aopa": 0, "gmax": 1, "barnstormers": 2}
    by_n, no_n = {}, []
    for l in listings:
        n = l.get("nNumber")
        if n:
            by_n.setdefault(n, []).append(l)
        else:
            no_n.append(l)
    unique, dupes = [], []
    for n, group in by_n.items():
        group.sort(key=lambda x: SOURCE_PRIORITY.get(x["source"], 99))
        unique.append(group[0])
        if len(group) > 1:
            dupes.extend(group[1:])
    return unique + no_n, dupes

def merge_with_existing(new_listings: list, existing_path: str = "listings_raw.json") -> list:
    try:
        with open(existing_path) as f:
            existing = {l["id"]: l for l in json.load(f)}
    except (FileNotFoundError, json.JSONDecodeError):
        existing = {}
    for l in new_listings:
        prev = existing.get(l["id"])
        if prev and prev.get("priceHistory"):
            last_price = prev["priceHistory"][-1]["price"]
            if l["price"] and l["price"] != last_price:
                l["priceHistory"] = prev["priceHistory"] + [{"price": l["price"], "date": today()}]
            else:
                l["priceHistory"] = prev["priceHistory"]
            for field in ["paintCondition","interiorCondition","annualDate","noAccident","distanceFromBase"]:
                if prev.get(field) is not None and l.get(field) is None:
                    l[field] = prev[field]
        l["dateFound"] = prev["dateFound"] if prev else today()
    return new_listings


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_all_scrapers(output_path: str = "listings_raw.json"):
    log.info("=== Starting aircraft listing scrape (v2) ===")
    all_listings = []
    all_listings += scrape_barnstormers()
    all_listings += scrape_aopa()
    all_listings += scrape_gmax()

    log.info(f"Total raw listings before dedup: {len(all_listings)}")
    all_listings = merge_with_existing(all_listings, output_path)
    unique, dupes = deduplicate(all_listings)
    log.info(f"After dedup: {len(unique)} unique, {len(dupes)} duplicates")

    dup_ns = {l["nNumber"] for l in dupes if l.get("nNumber")}
    for l in unique:
        l["isDuplicate"] = l.get("nNumber") in dup_ns

    with open(output_path, "w") as f:
        json.dump(unique + dupes, f, indent=2)
    log.info(f"Saved {len(unique+dupes)} listings to {output_path}")
    log.info(f"=== Done: {len([l for l in unique if l['tier']==1])} Tier 1 | {len([l for l in unique if l['tier']==2])} Tier 2 ===")
    return unique + dupes

if __name__ == "__main__":
    results = run_all_scrapers()
    print(f"\nDone. {len(results)} total listings written to listings_raw.json")
