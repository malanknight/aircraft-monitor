"""
aircraft_scrapers.py  (v3 — RSS only)
--------------------------------------
Scrapes Barnstormers via their RSS feeds.
AOPA and GMAX removed — Controller and Trade-A-Plane are handled
by email_parser.py via Gmail alerts.

Barnstormers RSS feed URL format:
  https://www.barnstormers.com/classified_search.php?
    searchcategory=1&make=Piper&price_high=105000&format=rss

RSS is plain XML — no JavaScript rendering, no compression issues,
no bot detection. Works reliably from any server.

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
from xml.etree import ElementTree as ET

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_LAT = 40.0819
BASE_LON = -75.0105

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; personal-aircraft-monitor/1.0)",
    "Accept": "application/rss+xml, application/xml, text/xml, */*",
}

REQUEST_DELAY = 2.0

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
# Barnstormers RSS feeds
# ---------------------------------------------------------------------------
# One feed per make. Barnstormers supports ?format=rss on their search URLs.
# Each feed returns up to ~25 most recent listings for that make/price range.

BARNSTORMERS_RSS_FEEDS = [
    # Correct RSS endpoint: ad_manager/listing.php?main=MAKE&RSS=1
    "https://www.barnstormers.com/ad_manager/listing.php?main=Piper&RSS=1",
    "https://www.barnstormers.com/ad_manager/listing.php?main=Mooney&RSS=1",
    "https://www.barnstormers.com/ad_manager/listing.php?main=Beechcraft&RSS=1",
    "https://www.barnstormers.com/ad_manager/listing.php?main=Grumman&RSS=1",
    # Model-specific feeds for key targets
    "https://www.barnstormers.com/ad_manager/listing.php?main=Piper&model=Archer&RSS=1",
    "https://www.barnstormers.com/ad_manager/listing.php?main=Piper&model=Arrow&RSS=1",
    "https://www.barnstormers.com/ad_manager/listing.php?main=Mooney&model=M20&RSS=1",
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def today() -> str:
    return date.today().isoformat()

def make_id(source: str, url: str) -> str:
    return source[:3].upper() + "_" + hashlib.md5(url.encode()).hexdigest()[:8]

def get(url: str) -> Optional[requests.Response]:
    for attempt in range(3):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=20)
            log.info(f"  HTTP {resp.status_code} ({len(resp.content)} bytes) {url[:80]}")
            if resp.status_code == 200:
                return resp
            if resp.status_code in (403, 429):
                log.warning(f"  Blocked ({resp.status_code})")
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
    text = text.replace(',', '').replace('\xa0', ' ')
    m = re.search(r'\$\s*(\d{4,6})', text)
    if m:
        v = int(m.group(1))
        if 10000 < v < 500000:
            return v
    m = re.search(r'(\d{5,6})\s*(?:USD|dollars|obo)', text, re.I)
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
            gtn = pat.upper() if pat.startswith("gtn") else "Avidyne " + pat.upper().replace("AVIDYNE","").strip()
            break
    return {
        "vacuum": None if "vacuum" not in t else not any(x in t for x in ["no vacuum", "vacuum del", "electric gyro"]),
        "gtn": gtn,
        "autopilot": any(x in t for x in ["autopilot", "stec", "s-tec", "kap 140", "kap140", "century", "navmatic"]),
        "engMonitor": any(x in t for x in ["jpi", "engine monitor", "eis", "gami", "shadin"]),
        "g5": any(x in t for x in ["g5", "gi 275", "aspen", "efis", "adsb"]),
        "ifr": any(x in t for x in ["ifr", "instrument", "ils", "vor/ils", "gps approach"]),
    }

def infer_engine_type(text: str) -> Optional[str]:
    t = text.lower()
    if any(x in t for x in ["factory reman", "factory remanufactured", "new factory", "zero time"]):
        return "Factory Reman"
    if any(x in t for x in ["major overhaul", "field overhaul", "top overhaul", "recently overhauled"]):
        return "Field Overhaul"
    if any(x in t for x in ["run out", "run-out", "due overhaul", "approaching tbo", "at tbo", "past tbo"]):
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
                  lat=None, lon=None, n_number=None) -> dict:
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
        "annualDate": None,
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
# Barnstormers RSS scraper
# ---------------------------------------------------------------------------

def parse_rss_item(item_el) -> dict:
    """Extract fields from a single RSS <item> element."""
    def tag(name):
        el = item_el.find(name)
        return el.text.strip() if el is not None and el.text else ""

    title       = tag("title")
    link        = tag("link")
    description = tag("description")

    # Strip HTML from description
    if "<" in description:
        description = BeautifulSoup(description, "html.parser").get_text(" ", strip=True)

    full_text = f"{title} {description}"
    return {"title": title, "link": link, "description": description, "full_text": full_text}

def scrape_barnstormers() -> list:
    listings = []
    seen_ids = set()

    for feed_url in BARNSTORMERS_RSS_FEEDS:
        log.info(f"Barnstormers RSS: {feed_url[:80]}")
        resp = get(feed_url)
        if not resp:
            continue

        # Parse RSS XML
        try:
            root = ET.fromstring(resp.content)
        except ET.ParseError as e:
            log.warning(f"  RSS parse error: {e}")
            log.info(f"  Content preview: {resp.text[:200]}")
            continue

        # Items can be at root/channel/item or root/item
        items = root.findall(".//item")
        log.info(f"  Found {len(items)} RSS items")

        if len(items) == 0:
            log.info(f"  Feed preview: {resp.text[:300]}")
            continue

        matched = 0
        for item_el in items:
            parsed = parse_rss_item(item_el)
            title    = parsed["title"]
            link     = parsed["link"]
            full     = parsed["full_text"]

            # Skip if we've already processed this listing URL
            listing_id = make_id("barnstormers", link)
            if listing_id in seen_ids:
                continue
            seen_ids.add(listing_id)

            # Match against target models
            model_dict = match_model(title) or match_model(full[:400])
            if not model_dict:
                continue

            tier  = 1 if model_dict in TIER1_MODELS else 2
            year  = extract_year(title) or extract_year(full[:200])
            price = extract_price(title) or extract_price(full[:600])

            if price and price > 110000:
                continue

            ttaf  = extract_hours(full, r"(?:TTAF|total\s*time|airframe)")
            smoh  = extract_hours(full, r"(?:SMOH|since\s*(?:major\s*)?overhaul|SFRM)")
            n_num = extract_n_number(full)
            eng   = infer_engine_type(full)
            av    = infer_avionics(full)

            # Location — look for "City, ST" pattern
            loc_m = re.search(r'([A-Z][a-z][\w\s]{2,20},\s*[A-Z]{2})\b', full)
            loc_name = loc_m.group(1).strip() if loc_m else None

            listing = build_listing(
                "barnstormers", link, model_dict, tier, year, price,
                ttaf, smoh, eng, av, parsed["description"][:600], loc_name,
                n_number=n_num
            )
            listings.append(listing)
            matched += 1
            price_str = f"${price:,}" if price else "price unknown"
            log.info(f"  + {year} {model_dict['model']} {price_str} — {loc_name or 'location unknown'}")

        log.info(f"  {matched} matched from this feed")
        time.sleep(REQUEST_DELAY)

    log.info(f"Barnstormers: {len(listings)} total matched listings")
    return listings

# ---------------------------------------------------------------------------
# Deduplication & price history merge
# ---------------------------------------------------------------------------

def deduplicate(listings: list) -> tuple:
    by_n, no_n = {}, []
    for l in listings:
        n = l.get("nNumber")
        if n:
            by_n.setdefault(n, []).append(l)
        else:
            no_n.append(l)
    unique, dupes = [], []
    for n, group in by_n.items():
        unique.append(group[0])
        if len(group) > 1:
            dupes.extend(group[1:])
            log.info(f"  Duplicate N{n} on {[g['source'] for g in group]}")
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
            last_price = prev["priceHistory"][-1]["price"] if prev["priceHistory"] else None
            if l["price"] and l["price"] != last_price:
                l["priceHistory"] = prev["priceHistory"] + [{"price": l["price"], "date": today()}]
            else:
                l["priceHistory"] = prev["priceHistory"]
            for field in ["paintCondition", "interiorCondition", "annualDate", "noAccident", "distanceFromBase"]:
                if prev.get(field) is not None and l.get(field) is None:
                    l[field] = prev[field]
        l["dateFound"] = prev["dateFound"] if prev else today()
    return new_listings

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_all_scrapers(output_path: str = "listings_raw.json"):
    log.info("=== Starting aircraft listing scrape (v3 — RSS) ===")

    all_listings = scrape_barnstormers()

    log.info(f"Total raw listings: {len(all_listings)}")
    all_listings = merge_with_existing(all_listings, output_path)
    unique, dupes = deduplicate(all_listings)

    dup_ns = {l["nNumber"] for l in dupes if l.get("nNumber")}
    for l in unique:
        l["isDuplicate"] = l.get("nNumber") in dup_ns

    with open(output_path, "w") as f:
        json.dump(unique + dupes, f, indent=2)

    tier1 = [l for l in unique if l["tier"] == 1]
    tier2 = [l for l in unique if l["tier"] == 2]
    log.info(f"Saved {len(unique + dupes)} listings — {len(tier1)} Tier 1, {len(tier2)} Tier 2")
    log.info("=== Done ===")
    return unique + dupes

if __name__ == "__main__":
    results = run_all_scrapers()
    print(f"\nDone. {len(results)} listings written to listings_raw.json")
