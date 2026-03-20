"""
email_parser.py
---------------
Reads aircraft alert emails from a Gmail label and extracts structured
listing data using the same schema as aircraft_scrapers.py.

Works with Controller.com and Trade-A-Plane saved-search alert emails.
Uses the Gmail API (same credentials as your MCP connection) via the
google-auth + google-api-python-client libraries.

Setup:
    1. Enable Gmail API in Google Cloud Console (same project as your MCP)
    2. Download credentials.json to this directory
    3. pip install google-auth google-auth-oauthlib google-api-python-client beautifulsoup4

Usage:
    python email_parser.py
    # Appends new listings to listings_raw.json
    # Marks processed emails with a "aircraft-processed" label so they
    # are never parsed twice.

Gmail label to watch: "aircraft-alerts"  (change WATCH_LABEL below)
"""

import base64
import json
import logging
import os
import re
from datetime import date, datetime
from typing import Optional

# Reuse helpers from scrapers
from aircraft_scrapers import (
    ALL_MODELS, TIER1_MODELS, match_model, extract_year, extract_price,
    extract_hours, extract_n_number, infer_avionics, infer_engine_type,
    build_listing, merge_with_existing, deduplicate, today
)

log = logging.getLogger("email_parser")
logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

WATCH_LABEL      = "aircraft-alerts"       # Gmail label containing alert emails
PROCESSED_LABEL  = "aircraft-processed"    # Applied after parsing to avoid reprocessing
OUTPUT_PATH      = "listings_raw.json"
CREDENTIALS_FILE = "credentials.json"
TOKEN_FILE       = "token.json"
SCOPES           = ["https://www.googleapis.com/auth/gmail.modify"]


# ---------------------------------------------------------------------------
# Gmail auth
# ---------------------------------------------------------------------------

def get_gmail_service():
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build

    creds = None
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_FILE, "w") as f:
            f.write(creds.to_json())

    return build("gmail", "v1", credentials=creds)


def get_or_create_label(service, name: str) -> str:
    """Return label ID for `name`, creating it if it doesn't exist."""
    labels = service.users().labels().list(userId="me").execute().get("labels", [])
    for l in labels:
        if l["name"].lower() == name.lower():
            return l["id"]
    created = service.users().labels().create(
        userId="me", body={"name": name, "labelListVisibility": "labelShow", "messageListVisibility": "show"}
    ).execute()
    return created["id"]


def fetch_unprocessed_emails(service, watch_label_id: str, processed_label_id: str) -> list:
    """Return messages in watch_label that do NOT have processed_label."""
    query = f"label:{WATCH_LABEL} -label:{PROCESSED_LABEL}"
    result = service.users().messages().list(userId="me", q=query, maxResults=50).execute()
    return result.get("messages", [])


def get_email_body(service, msg_id: str) -> tuple[str, str, str]:
    """Returns (subject, from_addr, plain_text_body)."""
    msg = service.users().messages().get(userId="me", id=msg_id, format="full").execute()
    headers = {h["name"].lower(): h["value"] for h in msg["payload"].get("headers", [])}
    subject = headers.get("subject", "")
    from_addr = headers.get("from", "")

    def extract_text(payload) -> str:
        if payload.get("mimeType") == "text/plain":
            data = payload.get("body", {}).get("data", "")
            return base64.urlsafe_b64decode(data + "==").decode("utf-8", errors="replace") if data else ""
        if payload.get("mimeType") == "text/html":
            data = payload.get("body", {}).get("data", "")
            if data:
                from bs4 import BeautifulSoup
                html = base64.urlsafe_b64decode(data + "==").decode("utf-8", errors="replace")
                return BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
        # Recurse into multipart
        text = ""
        for part in payload.get("parts", []):
            text += extract_text(part)
        return text

    body = extract_text(msg["payload"])
    return subject, from_addr, body


def mark_processed(service, msg_id: str, processed_label_id: str):
    service.users().messages().modify(
        userId="me", id=msg_id,
        body={"addLabelIds": [processed_label_id]}
    ).execute()


# ---------------------------------------------------------------------------
# Source detection
# ---------------------------------------------------------------------------

def detect_source(from_addr: str, subject: str) -> Optional[str]:
    combined = (from_addr + " " + subject).lower()
    if "controller" in combined:
        return "controller"
    if "trade-a-plane" in combined or "tradeaplane" in combined or "tap" in combined:
        return "tradeaplane"
    return None


# ---------------------------------------------------------------------------
# Controller.com email parser
# ---------------------------------------------------------------------------
#
# Controller alert email format (as of 2024-2026):
#
# Subject: "New Listing Alert: 1982 Piper PA-28-181 Archer II - $87,500"
#    or:   "Controller.com: New aircraft matching your saved search"
#
# Body (plain text) contains one or more listing blocks like:
#
#   1982 Piper PA-28-181 Archer II
#   Asking Price: $87,500
#   Total Time: 3,420 Hours
#   Engine Time: 720 SMOH
#   Location: Philadelphia, PA
#   [optional description paragraph]
#   View Listing: https://www.controller.com/listings/aircraft/...
#
# Multiple listings may appear in one digest email.

def parse_controller_email(subject: str, body: str) -> list:
    listings = []

    # Split on "View Listing" or double-newline blocks to isolate each aircraft
    blocks = re.split(r'(?:View Listing|See Details|View Details)\s*[:\-]?\s*https?://\S+', body, flags=re.I)
    urls   = re.findall(r'(?:View Listing|See Details|View Details)\s*[:\-]?\s*(https?://\S+)', body, re.I)

    # If we can't split nicely, treat the whole body as one block
    if len(blocks) <= 1:
        blocks = [body]
        urls   = re.findall(r'(https?://www\.controller\.com/listings/\S+)', body)

    for i, block in enumerate(blocks):
        if not block.strip():
            continue
        model_dict = match_model(block)
        if not model_dict:
            # Try subject for single-listing alerts
            model_dict = match_model(subject)
        if not model_dict:
            continue

        tier  = 1 if model_dict in TIER1_MODELS else 2
        year  = extract_year(block) or extract_year(subject)
        price = extract_price(block) or extract_price(subject)
        if price and price > 110000:
            continue

        ttaf  = extract_hours(block, r"(?:TTAF|total\s*time|airframe\s*time)")
        smoh  = extract_hours(block, r"(?:SMOH|engine\s*time|since\s*overhaul)")
        n_num = extract_n_number(block)
        eng   = infer_engine_type(block)
        av    = infer_avionics(block)

        loc_m = re.search(r'Location\s*[:\-]?\s*([A-Za-z\s]+,\s*[A-Z]{2})', block, re.I)
        if not loc_m:
            loc_m = re.search(r'([A-Z][a-z]+(?:\s[A-Z][a-z]+)*,\s*[A-Z]{2})\b', block)
        loc_name = loc_m.group(1).strip() if loc_m else None

        url = urls[i] if i < len(urls) else "https://www.controller.com"

        listing = build_listing("controller", url, model_dict, tier, year, price,
                                ttaf, smoh, eng, av, block[:600].strip(), loc_name,
                                n_number=n_num)
        listings.append(listing)
        log.info(f"  Controller: {year} {model_dict['model']} ${price:,}" if price else f"  Controller: {year} {model_dict['model']} (price unknown)")

    return listings


# ---------------------------------------------------------------------------
# Trade-A-Plane email parser
# ---------------------------------------------------------------------------
#
# TAP alert email format (as of 2024-2026):
#
# Subject: "Trade-A-Plane Search Alert - Piper PA-28-181"
#    or:   "New listings match your Trade-A-Plane search"
#
# Body contains listing blocks like:
#
#   1979 Piper PA-28-181 Archer II
#   Price: $84,500
#   TTAF: 2,910    SMOH: 540
#   Location: Trenton, NJ
#   [description text]
#   https://www.trade-a-plane.com/search?listing_id=XXXXXXX
#
# TAP sometimes sends HTML-only emails — BeautifulSoup handles stripping.

def parse_tradeaplane_email(subject: str, body: str) -> list:
    listings = []

    # TAP uses horizontal rules or blank lines between listings
    blocks = re.split(r'\n{3,}|_{5,}|-{5,}', body)
    urls   = re.findall(r'(https?://(?:www\.)?trade-a-plane\.com/\S+)', body)

    if len(blocks) <= 1:
        blocks = [body]

    for i, block in enumerate(blocks):
        if not block.strip():
            continue
        model_dict = match_model(block)
        if not model_dict:
            model_dict = match_model(subject)
        if not model_dict:
            continue

        tier  = 1 if model_dict in TIER1_MODELS else 2
        year  = extract_year(block) or extract_year(subject)
        price = extract_price(block) or extract_price(subject)
        if price and price > 110000:
            continue

        ttaf  = extract_hours(block, r"(?:TTAF|Total\s*Time|TT\b)")
        smoh  = extract_hours(block, r"(?:SMOH|SFRM|Engine\s*Time|Since\s*Overhaul)")
        n_num = extract_n_number(block)
        eng   = infer_engine_type(block)
        av    = infer_avionics(block)

        loc_m = re.search(r'(?:Location|City|Located)\s*[:\-]?\s*([A-Za-z\s]+,\s*[A-Z]{2})', block, re.I)
        if not loc_m:
            loc_m = re.search(r'([A-Z][a-z]+(?:\s[A-Z][a-z]+)*,\s*[A-Z]{2})\b', block)
        loc_name = loc_m.group(1).strip() if loc_m else None

        url = urls[i] if i < len(urls) else "https://www.trade-a-plane.com"

        listing = build_listing("tradeaplane", url, model_dict, tier, year, price,
                                ttaf, smoh, eng, av, block[:600].strip(), loc_name,
                                n_number=n_num)
        listings.append(listing)
        log.info(f"  TAP: {year} {model_dict['model']} ${price:,}" if price else f"  TAP: {year} {model_dict['model']} (price unknown)")

    return listings


# ---------------------------------------------------------------------------
# Confidence scoring for parsed listings
# ---------------------------------------------------------------------------
# Listings with too many None fields get flagged for manual review rather
# than auto-ingested, so you don't end up with junk data.

def confidence_score(listing: dict) -> float:
    """0.0 – 1.0. Below 0.5 = needs manual review."""
    score = 0.0
    checks = [
        listing.get("year") is not None,
        listing.get("price") is not None,
        listing.get("model") is not None,
        listing.get("ttaf") is not None,
        listing.get("smoh") is not None,
        listing.get("locationName") is not None,
        listing.get("url", "").startswith("http"),
    ]
    return sum(checks) / len(checks)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_email_parser(output_path: str = OUTPUT_PATH):
    log.info("=== Starting email ingestion ===")

    service = get_gmail_service()

    watch_id     = get_or_create_label(service, WATCH_LABEL)
    processed_id = get_or_create_label(service, PROCESSED_LABEL)

    messages = fetch_unprocessed_emails(service, watch_id, processed_id)
    log.info(f"Found {len(messages)} unprocessed alert emails")

    new_listings  = []
    needs_review  = []

    for msg in messages:
        subject, from_addr, body = get_email_body(service, msg["id"])
        source = detect_source(from_addr, subject)

        if not source:
            log.info(f"  Skipping unrecognized sender: {from_addr}")
            mark_processed(service, msg["id"], processed_id)
            continue

        log.info(f"  Parsing {source} email: {subject[:80]}")

        if source == "controller":
            parsed = parse_controller_email(subject, body)
        elif source == "tradeaplane":
            parsed = parse_tradeaplane_email(subject, body)
        else:
            parsed = []

        for listing in parsed:
            conf = confidence_score(listing)
            if conf >= 0.5:
                new_listings.append(listing)
                log.info(f"    Ingested (conf={conf:.0%}): {listing.get('year')} {listing.get('model')}")
            else:
                listing["_needsReview"] = True
                listing["_confidence"] = conf
                needs_review.append(listing)
                log.warning(f"    Low confidence ({conf:.0%}) — flagged for review: {listing.get('model')}")

        mark_processed(service, msg["id"], processed_id)

    log.info(f"Ingested: {len(new_listings)} | Needs review: {len(needs_review)}")

    if not new_listings and not needs_review:
        log.info("No new listings found.")
        return []

    # Merge with existing listings (price history, manual fields)
    all_new = merge_with_existing(new_listings + needs_review, output_path)

    # Load existing and append
    try:
        with open(output_path) as f:
            existing = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        existing = []

    existing_ids = {l["id"] for l in existing}
    truly_new = [l for l in all_new if l["id"] not in existing_ids]

    combined = existing + truly_new
    unique, dupes = deduplicate(combined)
    dup_ids = {l["id"] for l in dupes}
    for l in unique:
        l["isDuplicate"] = l["id"] in dup_ids

    with open(output_path, "w") as f:
        json.dump(unique + dupes, f, indent=2)

    log.info(f"Saved {len(unique + dupes)} total listings to {output_path}")

    if needs_review:
        review_path = "listings_needs_review.json"
        with open(review_path, "w") as f:
            json.dump(needs_review, f, indent=2)
        log.info(f"⚠ {len(needs_review)} listings need manual review → {review_path}")

    return truly_new


if __name__ == "__main__":
    new = run_email_parser()
    print(f"\nDone. {len(new)} new listings added.")
