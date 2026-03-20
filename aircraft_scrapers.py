"""
aircraft_scrapers.py
--------------------
Scrapers for Barnstormers, AOPA Marketplace, and GMAX Aircraft.
Each scraper returns a list of listing dicts normalized to the dashboard schema.

Output schema per listing:
{
    "id":               str,        # source_prefix + hash of url
    "nNumber":          str|None,   # FAA N-number if found in listing
    "source":           str,        # "barnstormers" | "aopa" | "gmax"
    "tier":             int,        # 1 or 2, determined by model matching
    "model":            str,        # human-readable model name
    "code":             str,        # ICAO/common code e.g. "PA-28-181"
    "year":             int|None,
    "price":            int|None,
    "priceHistory":     [{"price": int, "date": "YYYY-MM-DD"}],
    "ttaf":             int|None,   # total time airframe
    "smoh":             int|None,   # since major overhaul
    "engineType":       str|None,   # "Factory Reman" | "Field Overhaul" | "Run-out" | None
    "annualDate":       str|None,   # "YYYY-MM-DD"
    "paintCondition":   int|None,   # 1-3
    "interiorCondition":int|None,   # 1-3
    "location":         str|None,   # ICAO identifier
    "locationName":     str|None,   # human-readable city/state
    "distanceFromBase": int,        # nm from KPNE, computed
    "noAccident":       bool|None,  # None = unknown
    "avionics": {
        "vacuum":       bool|None,
        "gtn":          str|None,
        "autopilot":    bool|None,
        "engMonitor":   bool|None,
        "g5":           bool|None,
        "ifr":          bool|None,
    },
    "notes":            str,
    "url":              str,
    "dateFound":        str,        # "YYYY-MM-DD"
}

Usage:
    python aircraft_scrapers.py
    # Writes results to listings_raw.json

Requirements:
    pip install requests beautifulsoup4 geopy
"""

import hashlib
import json
import logging
import re
import time
from datetime import date, datetime
from typing import Optional

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_AIRPORT = "KPNE"   # Philadelphia Northeast — change if your base moves
BASE_LAT     = 40.0819
BASE_LON     = -75.0105

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

REQUEST_DELAY = 2.5   # seconds between requests — be polite

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger("scrapers")

# ---------------------------------------------------------------------------
# Target aircraft — used to assign tier and normalize model names
# ---------------------------------------------------------------------------

TIER1_MODELS = [
    {"model": "Piper Archer II",  "code": "PA-28-181",   "keywords": ["archer ii", "pa-28-181", "pa28-181"]},
    {"model": "Piper Arrow III",  "code": "PA-28R-201",  "keywords": ["arrow iii", "pa-28r-201", "pa28r-201"]},
    {"model": "Mooney M20E",      "code": "M20E",        "keywords": ["m20e"]},
    {"model": "Mooney M20F",      "code": "M20F",        "keywords": ["m20f"]},
]
TIER2_MODELS = [
    {"model": "Piper Warrior",       "code": "PA-28-161", "keywords": ["warrior", "pa-28-161", "pa-28-151", "pa28-161"]},
    {"model": "Mooney M20C",         "code": "M20C",      "keywords": ["m20c"]},
    {"model": "Beech Musketeer",     "code": "A23",       "keywords": ["musketeer", "a23", "sundowner", "c23"]},
    {"model": "Grumman AA5A",        "code": "AA5A",      "keywords": ["aa5a", "cheetah"]},
]
ALL_MODELS = TIER1_MODELS + TIER2_MODELS

# Search terms to use when querying each source
SEARCH_MAKES = ["Piper", "Mooney", "Beechcraft", "Grumman"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def today() -> str:
    return date.today().isoformat()


def make_id(source: str, url: str) -> str:
    return source[:3].upper() + "_" + hashlib.md5(url.encode()).hexdigest()[:8]


def get(url: str, **kwargs) -> Optional[requests.Response]:
    """Polite GET with retry."""
    for attempt in range(3):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15, **kwargs)
            if resp.status_code == 200:
                return resp
            log.warning(f"HTTP {resp.status_code} for {url}")
        except requests.RequestException as e:
            log.warning(f"Request error (attempt {attempt+1}): {e}")
        time.sleep(REQUEST_DELAY * (attempt + 1))
    return None


def match_model(text: str) -> Optional[dict]:
    """Return model dict if text matches any target, else None."""
    lower = text.lower()
    for m in ALL_MODELS:
        if any(kw in lower for kw in m["keywords"]):
            return m
    return None


def extract_year(text: str) -> Optional[int]:
    m = re.search(r'\b(19[5-9]\d|20[0-2]\d)\b', text)
    return int(m.group(1)) if m else None


def extract_price(text: str) -> Optional[int]:
    m = re.search(r'\$\s*([\d,]+)', text.replace('\xa0', ' '))
    if m:
        return int(m.group(1).replace(',', ''))
    m = re.search(r'([\d,]+)\s*(?:USD|dollars)', text, re.I)
    if m:
        return int(m.group(1).replace(',', ''))
    return None


def extract_hours(text: str, keyword: str) -> Optional[int]:
    """Extract a number near a keyword like TTAF, SMOH, TT."""
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
    for pattern in ["gtn 750", "gtn 650", "gtn 530", "gtn 430", "ifd 550", "ifd 440", "ifd 540", "avidyne"]:
        if pattern in t:
            gtn = pattern.upper().replace("IFD", "Avidyne IFD").replace("AVIDYNE", "Avidyne")
            # Normalize
            if "GTN" in gtn: gtn = gtn.replace("GTN ", "GTN ")
            break
    if not gtn:
        for pattern in ["kx 175", "kx 165", "sl30", "nav/com"]:
            if pattern in t:
                gtn = None
                break
    return {
        "vacuum":      None if "vacuum" not in t else ("no vacuum" not in t and "vacuum deleted" not in t and "electric" not in t),
        "gtn":         gtn,
        "autopilot":   any(x in t for x in ["autopilot", "stec", "s-tec", "kap 140", "kap140", "century", "navmatic"]),
        "engMonitor":  any(x in t for x in ["jpi", "engine monitor", "eis", "gami", "shadin"]),
        "g5":          any(x in t for x in ["g5", "gi 275", "aspen", "efis"]),
        "ifr":         any(x in t for x in ["ifr", "instrument", "ils", "vor", "gps"]),
    }


def infer_engine_type(text: str) -> Optional[str]:
    t = text.lower()
    if any(x in t for x in ["factory reman", "factory remanufactured", "lycoming new", "continental new"]):
        return "Factory Reman"
    if any(x in t for x in ["field overhaul", "major overhaul", "top overhaul"]):
        return "Field Overhaul"
    if any(x in t for x in ["run out", "run-out", "due overhaul", "approaching tbo", "tbo"]):
        return "Run-out"
    return None


def distance_from_base(lat: Optional[float], lon: Optional[float]) -> int:
    """Approximate nm from KPNE. Returns 9999 if coords unknown."""
    if lat is None or lon is None:
        return 9999
    try:
        from geopy.distance import great_circle
        return int(great_circle((BASE_LAT, BASE_LON), (lat, lon)).nm)
    except ImportError:
        # Rough degree-based fallback if geopy not installed
        deg = ((lat - BASE_LAT)**2 + (lon - BASE_LON)**2) ** 0.5
        return int(deg * 54)   # ~54nm per degree at mid-latitudes


def build_listing(source, url, model_dict, tier, year, price, ttaf, smoh,
                  engine_type, avionics, notes, location_name=None,
                  lat=None, lon=None, n_number=None, annual_date=None) -> dict:
    return {
        "id":               make_id(source, url),
        "nNumber":          n_number,
        "source":           source,
        "tier":             tier,
        "model":            model_dict["model"],
        "code":             model_dict["code"],
        "year":             year,
        "price":            price,
        "priceHistory":     [{"price": price, "date": today()}] if price else [],
        "ttaf":             ttaf,
        "smoh":             smoh,
        "engineType":       engine_type,
        "annualDate":       annual_date,
        "paintCondition":   None,
        "interiorCondition":None,
        "location":         None,
        "locationName":     location_name,
        "distanceFromBase": distance_from_base(lat, lon),
        "noAccident":       None,
        "avionics":         avionics,
        "notes":            notes[:800] if notes else "",
        "url":              url,
        "dateFound":        today(),
    }


# ---------------------------------------------------------------------------
# Barnstormers scraper
# ---------------------------------------------------------------------------
# Barnstormers uses plain HTML pages with a simple URL structure.
# Search URL: /classified_search.php?searchcategory=1&make=MAKE&price_low=0&price_high=105000
# Each result links to /listing/XXXXXX

BARNSTORMERS_BASE = "https://www.barnstormers.com"

BARNSTORMERS_SEARCHES = [
    f"{BARNSTORMERS_BASE}/classified_search.php?searchcategory=1&make=Piper&price_low=0&price_high=105000&orderby=date&sort=D",
    f"{BARNSTORMERS_BASE}/classified_search.php?searchcategory=1&make=Mooney&price_low=0&price_high=105000&orderby=date&sort=D",
    f"{BARNSTORMERS_BASE}/classified_search.php?searchcategory=1&make=Beechcraft&price_low=0&price_high=105000&orderby=date&sort=D",
    f"{BARNSTORMERS_BASE}/classified_search.php?searchcategory=1&make=Grumman&price_low=0&price_high=105000&orderby=date&sort=D",
]


def scrape_barnstormers_listing(url: str) -> Optional[dict]:
    resp = get(url)
    if not resp:
        return None
    soup = BeautifulSoup(resp.text, "html.parser")

    # Full text of the listing
    body = soup.get_text(" ", strip=True)

    # Identify model
    title_el = soup.find("h1") or soup.find("h2")
    title = title_el.get_text(" ", strip=True) if title_el else body[:120]
    model_dict = match_model(title) or match_model(body[:500])
    if not model_dict:
        return None
    tier = 1 if model_dict in TIER1_MODELS else 2

    year  = extract_year(title) or extract_year(body[:300])
    price = extract_price(body[:1000])
    if price and price > 110000:
        return None   # outside budget even with some buffer

    ttaf  = extract_hours(body, r"(?:TTAF|total\s*time|airframe)")
    smoh  = extract_hours(body, r"(?:SMOH|since\s*(?:major\s*)?overhaul|since\s*new)")
    n_num = extract_n_number(body)
    eng   = infer_engine_type(body)
    av    = infer_avionics(body)

    # Location — Barnstormers usually has "City, ST" near the top
    loc_m = re.search(r'([A-Z][a-z]+(?:\s[A-Z][a-z]+)*,\s*[A-Z]{2})\b', body[:600])
    loc_name = loc_m.group(1) if loc_m else None

    time.sleep(REQUEST_DELAY)
    return build_listing("barnstormers", url, model_dict, tier, year, price,
                         ttaf, smoh, eng, av, body[:600], loc_name, n_number=n_num)


def scrape_barnstormers() -> list:
    listings = []
    seen_urls = set()

    for search_url in BARNSTORMERS_SEARCHES:
        log.info(f"Barnstormers: {search_url}")
        resp = get(search_url)
        if not resp:
            continue
        soup = BeautifulSoup(resp.text, "html.parser")

        # Listing links are <a href="/listing/XXXXXX">
        links = soup.select("a[href*='/listing/']")
        urls = list({BARNSTORMERS_BASE + a["href"] if a["href"].startswith("/") else a["href"]
                     for a in links if "/listing/" in a.get("href", "")})
        log.info(f"  Found {len(urls)} listing links")

        for url in urls:
            if url in seen_urls:
                continue
            seen_urls.add(url)
            listing = scrape_barnstormers_listing(url)
            if listing:
                listings.append(listing)
                log.info(f"  + {listing['year']} {listing['model']} ${listing['price']:,}")
            time.sleep(REQUEST_DELAY)

    log.info(f"Barnstormers: {len(listings)} matched listings")
    return listings


# ---------------------------------------------------------------------------
# AOPA Marketplace scraper
# ---------------------------------------------------------------------------
# AOPA Marketplace is powered by Controller.com's data under the hood,
# but exposes a simpler JSON endpoint we can query directly.
# Endpoint: https://www.aopa.org/go/api/marketplace/search
# Returns JSON with a "listings" array.

AOPA_API = "https://www.aopa.org/go/api/marketplace/search"

AOPA_PARAMS_BASE = {
    "category": "Aircraft",
    "subcategory": "Single Engine Piston",
    "priceMax": 105000,
    "pageSize": 100,
    "page": 1,
}
AOPA_MAKES = ["Piper", "Mooney", "Beechcraft", "Grumman"]


def scrape_aopa() -> list:
    listings = []

    for make in AOPA_MAKES:
        params = {**AOPA_PARAMS_BASE, "make": make}
        log.info(f"AOPA: make={make}")

        page = 1
        while True:
            params["page"] = page
            resp = get(AOPA_API, params=params)
            if not resp:
                break
            try:
                data = resp.json()
            except Exception:
                log.warning("AOPA: JSON parse failed")
                break

            items = data.get("listings") or data.get("results") or data.get("data") or []
            if not items:
                break

            for item in items:
                # Normalize field names — AOPA/Controller field names vary
                title = (item.get("title") or item.get("name") or
                         f"{item.get('year','')} {item.get('make','')} {item.get('model','')}").strip()
                model_dict = match_model(title)
                if not model_dict:
                    continue
                tier = 1 if model_dict in TIER1_MODELS else 2

                price = item.get("price") or item.get("askingPrice")
                if isinstance(price, str):
                    price = extract_price(price)
                if price and price > 110000:
                    continue

                year  = item.get("year") or extract_year(title)
                ttaf  = item.get("totalTime") or item.get("ttaf")
                smoh  = item.get("engineTime") or item.get("smoh")
                loc   = item.get("city") or item.get("location") or ""
                state = item.get("state") or item.get("stateCode") or ""
                loc_name = f"{loc}, {state}".strip(", ") or None
                url   = item.get("url") or item.get("listingUrl") or item.get("link") or AOPA_API
                if not url.startswith("http"):
                    url = "https://www.aopa.org" + url
                desc  = item.get("description") or item.get("notes") or title
                av    = infer_avionics(desc)
                eng   = infer_engine_type(desc)
                n_num = extract_n_number(desc) or item.get("nNumber") or item.get("tailNumber")
                if n_num and n_num.startswith("N"):
                    n_num = n_num[1:]

                lat = item.get("latitude") or item.get("lat")
                lon = item.get("longitude") or item.get("lon") or item.get("lng")

                listing = build_listing("aopa", url, model_dict, tier, year, price,
                                        ttaf, smoh, eng, av, desc[:600], loc_name,
                                        lat, lon, n_num)
                listings.append(listing)
                log.info(f"  + {listing['year']} {listing['model']} ${listing['price']:,}")

            if len(items) < params["pageSize"]:
                break
            page += 1
            time.sleep(REQUEST_DELAY)

    log.info(f"AOPA: {len(listings)} matched listings")
    return listings


# ---------------------------------------------------------------------------
# GMAX Aircraft scraper
# ---------------------------------------------------------------------------
# GMAX is a Mooney specialty dealer with a small, manually-updated inventory.
# Simple HTML, no bot protection. We scrape their inventory page directly.

GMAX_BASE      = "https://www.gmaxaeronautics.com"
GMAX_INVENTORY = f"{GMAX_BASE}/aircraft-for-sale"


def scrape_gmax() -> list:
    listings = []
    log.info(f"GMAX: {GMAX_INVENTORY}")
    resp = get(GMAX_INVENTORY)
    if not resp:
        log.warning("GMAX: failed to fetch inventory page")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")

    # GMAX uses a grid of aircraft cards — each links to a detail page
    # Try common patterns for listing links
    detail_links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        # Detail pages typically have /aircraft/ or /listing/ or /for-sale/ in path
        if any(x in href for x in ["/aircraft/", "/for-sale/", "/listing/", "/inventory/"]):
            full = href if href.startswith("http") else GMAX_BASE + href
            if full != GMAX_INVENTORY:
                detail_links.add(full)

    log.info(f"  Found {len(detail_links)} detail links")

    for url in detail_links:
        resp2 = get(url)
        if not resp2:
            continue
        soup2 = BeautifulSoup(resp2.text, "html.parser")
        body  = soup2.get_text(" ", strip=True)

        model_dict = match_model(body[:800])
        if not model_dict:
            time.sleep(REQUEST_DELAY)
            continue
        tier = 1 if model_dict in TIER1_MODELS else 2

        title_el = soup2.find("h1") or soup2.find("h2")
        title = title_el.get_text(" ", strip=True) if title_el else body[:120]

        year  = extract_year(title) or extract_year(body[:300])
        price = extract_price(body[:1200])
        if price and price > 110000:
            time.sleep(REQUEST_DELAY)
            continue

        ttaf  = extract_hours(body, r"(?:TTAF|total\s*time|airframe)")
        smoh  = extract_hours(body, r"(?:SMOH|since\s*(?:major\s*)?overhaul)")
        n_num = extract_n_number(body)
        eng   = infer_engine_type(body)
        av    = infer_avionics(body)

        loc_m = re.search(r'([A-Z][a-z]+(?:\s[A-Z][a-z]+)*,\s*[A-Z]{2})\b', body[:800])
        loc_name = loc_m.group(1) if loc_m else None

        listing = build_listing("gmax", url, model_dict, tier, year, price,
                                ttaf, smoh, eng, av, body[:600], loc_name, n_number=n_num)
        listings.append(listing)
        log.info(f"  + {listing['year']} {listing['model']} ${(listing['price'] or 0):,}")
        time.sleep(REQUEST_DELAY)

    log.info(f"GMAX: {len(listings)} matched listings")
    return listings


# ---------------------------------------------------------------------------
# Deduplication by N-number
# ---------------------------------------------------------------------------

def deduplicate(listings: list) -> tuple[list, list]:
    """
    Returns (unique_listings, duplicate_groups).
    Duplicates are flagged but not removed — the dashboard handles display.
    Within each N-number group, sort by source priority so the canonical
    listing comes first.
    SOURCE_PRIORITY: aopa > gmax > barnstormers (lower = preferred)
    """
    SOURCE_PRIORITY = {"aopa": 0, "gmax": 1, "barnstormers": 2}
    by_n = {}
    no_n = []

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
            log.info(f"Duplicate N{n}: found on {[g['source'] for g in group]}")

    return unique + no_n, dupes


# ---------------------------------------------------------------------------
# Price history merge
# ---------------------------------------------------------------------------

def merge_with_existing(new_listings: list, existing_path: str = "listings_raw.json") -> list:
    """
    Load previously saved listings and preserve price history.
    New price entries are appended only if price changed.
    """
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
                # Price changed — append new entry
                l["priceHistory"] = prev["priceHistory"] + [{"price": l["price"], "date": today()}]
            else:
                # Price unchanged — keep full history
                l["priceHistory"] = prev["priceHistory"]
            # Preserve manually-entered fields
            for field in ["paintCondition", "interiorCondition", "annualDate", "noAccident", "distanceFromBase"]:
                if prev.get(field) is not None and l.get(field) is None:
                    l[field] = prev[field]
        l["dateFound"] = prev["dateFound"] if prev else today()

    return new_listings


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_all_scrapers(output_path: str = "listings_raw.json"):
    log.info("=== Starting aircraft listing scrape ===")
    all_listings = []

    all_listings += scrape_barnstormers()
    all_listings += scrape_aopa()
    all_listings += scrape_gmax()

    log.info(f"Total raw listings before dedup: {len(all_listings)}")

    # Merge price history from previous run
    all_listings = merge_with_existing(all_listings, output_path)

    # Deduplicate
    unique, dupes = deduplicate(all_listings)
    log.info(f"After dedup: {len(unique)} unique, {len(dupes)} duplicates flagged")

    # Mark duplicates in the unique list too (for dashboard display)
    dup_n_numbers = {l["nNumber"] for l in dupes if l.get("nNumber")}
    for l in unique:
        l["isDuplicate"] = l.get("nNumber") in dup_n_numbers

    # Save
    with open(output_path, "w") as f:
        json.dump(unique + dupes, f, indent=2)
    log.info(f"Saved {len(unique + dupes)} listings to {output_path}")

    # Summary
    tier1 = [l for l in unique if l["tier"] == 1]
    high_score_approx = [l for l in unique if (l.get("smoh") or 0) >= 400 and (l.get("price") or 999999) <= 100000]
    log.info(f"=== Done: {len(tier1)} Tier 1 | {len(high_score_approx)} potentially high-scoring ===")

    return unique + dupes


if __name__ == "__main__":
    results = run_all_scrapers()
    print(f"\nDone. {len(results)} total listings written to listings_raw.json")
