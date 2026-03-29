"""
parser.py — Aircraft email alert parser
Reads Gmail aircraft-alerts label, extracts listings, saves listings.json
"""

import base64, hashlib, json, logging, os, re
from datetime import date
from bs4 import BeautifulSoup
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("parser")

SCOPES        = ["https://www.googleapis.com/auth/gmail.modify"]
WATCH_LABEL   = "aircraft-alerts"
DONE_LABEL    = "aircraft-processed"
OUTPUT        = "listings.json"

TIER1 = [
    {"model":"Piper Archer II",  "code":"PA-28-181",  "kw":["archer ii","pa-28-181","pa28-181"]},
    {"model":"Piper Arrow III",  "code":"PA-28R-201", "kw":["arrow iii","pa-28r-201","pa28r-201"]},
    {"model":"Mooney M20E",      "code":"M20E",       "kw":["m20e"]},
    {"model":"Mooney M20F",      "code":"M20F",       "kw":["m20f"]},
]
TIER2 = [
    {"model":"Piper Warrior",    "code":"PA-28-161",  "kw":["warrior","pa-28-161","pa-28-151"]},
    {"model":"Mooney M20C",      "code":"M20C",       "kw":["m20c"]},
    {"model":"Beech Musketeer",  "code":"A23",        "kw":["musketeer","sundowner","a23","c23"]},
    {"model":"Grumman AA5A",     "code":"AA5A",       "kw":["aa5a","cheetah"]},
]
ALL_MODELS = TIER1 + TIER2

def today():
    return date.today().isoformat()

def uid(source, text):
    return source[:3].upper() + "_" + hashlib.md5(text.encode()).hexdigest()[:8]

def find_model(text):
    t = text.lower()
    for m in ALL_MODELS:
        if any(k in t for k in m["kw"]):
            return m
    return None

def get_year(text):
    m = re.search(r'\b(19[5-9]\d|20[0-2]\d)\b', text)
    return int(m.group(1)) if m else None

def get_price(text):
    t = text.replace(',','')
    m = re.search(r'\$\s*(\d{4,6})', t)
    if m:
        v = int(m.group(1))
        if 10000 < v < 500000:
            return v
    return None

def get_hours(text, kw):
    m = re.search(rf'(\d{{3,5}})\s*(?:hrs?)?\s*{kw}|{kw}\s*[:\-]?\s*(\d{{3,5}})', text, re.I)
    return int(m.group(1) or m.group(2)) if m else None

def get_n(text):
    m = re.search(r'\bN(\d[A-Z0-9]{1,4}[A-Z]?)\b', text)
    return m.group(1) if m else None

def get_avionics(text):
    t = text.lower()
    gtn = None
    for p in ["gtn 750","gtn 650","gtn 530w","gtn 530","gtn 430w","gtn 430","ifd 550","ifd 440","avidyne"]:
        if p in t:
            gtn = p.upper() if p.startswith("gtn") else "Avidyne"
            break
    return {
        "vacuum":    None if "vacuum" not in t else not any(x in t for x in ["no vacuum","vacuum del"]),
        "gtn":       gtn,
        "autopilot": any(x in t for x in ["autopilot","stec","s-tec","kap 140","century"]),
        "engMonitor":any(x in t for x in ["jpi","engine monitor","eis","edm"]),
        "g5":        any(x in t for x in ["g5","gi 275","aspen","efis"]),
        "ifr":       any(x in t for x in ["ifr","instrument","ils"]),
    }

def get_engine_type(text):
    t = text.lower()
    if any(x in t for x in ["factory reman","factory remanufactured","zero time"]):
        return "Factory Reman"
    if any(x in t for x in ["major overhaul","field overhaul","recently overhauled"]):
        return "Field Overhaul"
    if any(x in t for x in ["run out","run-out","approaching tbo","at tbo"]):
        return "Run-out"
    return None

def make_listing(source, ref, model, tier, year, price, ttaf, smoh, eng, av, notes, loc=None, n=None):
    return {
        "id":               uid(source, ref),
        "source":           source,
        "tier":             tier,
        "model":            model["model"],
        "code":             model["code"],
        "year":             year,
        "price":            price,
        "priceHistory":     [{"price":price,"date":today()}] if price else [],
        "ttaf":             ttaf,
        "smoh":             smoh,
        "engineType":       eng,
        "annualDate":       None,
        "paintCondition":   None,
        "interiorCondition":None,
        "locationName":     loc,
        "distanceFromBase": 9999,
        "noAccident":       None,
        "nNumber":          n,
        "avionics":         av,
        "notes":            (notes or "")[:600],
        "dateFound":        today(),
        "isDuplicate":      False,
    }

# ── Gmail ─────────────────────────────────────────────────────────────────────

def gmail_service():
    creds = None
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        with open("token.json","w") as f:
            f.write(creds.to_json())
    return build("gmail","v1",credentials=creds)

def get_or_make_label(svc, name):
    labels = svc.users().labels().list(userId="me").execute().get("labels",[])
    for l in labels:
        if l["name"].lower() == name.lower():
            return l["id"]
    return svc.users().labels().create(userId="me",body={"name":name}).execute()["id"]

def get_body(svc, mid):
    msg = svc.users().messages().get(userId="me",id=mid,format="full").execute()
    hdrs = {h["name"].lower():h["value"] for h in msg["payload"].get("headers",[])}
    def text(p):
        if p.get("mimeType")=="text/plain":
            d=p.get("body",{}).get("data","")
            return base64.urlsafe_b64decode(d+"==").decode("utf-8",errors="replace") if d else ""
        if p.get("mimeType")=="text/html":
            d=p.get("body",{}).get("data","")
            if d:
                return BeautifulSoup(base64.urlsafe_b64decode(d+"==").decode("utf-8",errors="replace"),"html.parser").get_text(" ",strip=True)
        return "".join(text(pp) for pp in p.get("parts",[]))
    return hdrs.get("subject",""), hdrs.get("from",""), text(msg["payload"])

def mark_done(svc, mid, done_id):
    svc.users().messages().modify(userId="me",id=mid,body={"addLabelIds":[done_id]}).execute()

# ── Parsers ───────────────────────────────────────────────────────────────────

def detect(frm, subj):
    c = (frm+" "+subj).lower()
    if "controller" in c: return "controller"
    if "trade-a-plane" in c or "tradeaplane" in c: return "tradeaplane"
    if "barnstormer" in c: return "barnstormers"
    return None

def parse_tap(subj, body):
    results = []
    # Split on "* MODEL HEADER" lines
    parts  = re.split(r'\n\*\s+[A-Z][A-Z0-9 \-]+\n', body)
    headers= re.findall(r'\n\*\s+([A-Z][A-Z0-9 \-]+)\n', body)
    blocks = parts[1:] if len(parts)>1 else [body]
    for i,block in enumerate(blocks):
        block=block.strip()
        if not block: continue
        hdr = headers[i] if i<len(headers) else ""
        model = find_model(hdr+" "+block)
        if not model: continue
        tier  = 1 if model in TIER1 else 2
        year  = get_year(block) or get_year(hdr)
        price = get_price(block)
        if price and price>110000: continue
        ttaf  = get_hours(block, r"TT\b|TTAF")
        smoh  = get_hours(block, r"TSOH|SMOH|SFRM")
        n     = get_n(block)
        loc_m = re.search(r'([A-Z][a-z][\w\s]{2,20})\s+based', block, re.I)
        loc   = loc_m.group(1).strip() if loc_m else None
        urls  = re.findall(r'(https?://(?:www\.)?trade-a-plane\.com/\S+)', body)
        url   = urls[i] if i<len(urls) else "https://www.trade-a-plane.com"
        results.append(make_listing("tradeaplane", url, model, tier, year, price, ttaf, smoh, get_engine_type(block), get_avionics(block), block[:600], loc, n))
        log.info(f"  TAP: {year} {model['model']} ${price:,}" if price else f"  TAP: {year} {model['model']}")
    return results

def parse_controller(subj, body):
    results = []
    blocks = re.split(r'(?:View Listing|See Details)\s*[:\-]?\s*https?://\S+', body, flags=re.I)
    urls   = re.findall(r'(?:View Listing|See Details)\s*[:\-]?\s*(https?://\S+)', body, re.I)
    if len(blocks)<=1:
        blocks=[body]
        urls=re.findall(r'(https?://www\.controller\.com/listings/\S+)',body)
    for i,block in enumerate(blocks):
        block=block.strip()
        if not block: continue
        model = find_model(block) or find_model(subj)
        if not model: continue
        tier  = 1 if model in TIER1 else 2
        year  = get_year(block) or get_year(subj)
        price = get_price(block) or get_price(subj)
        if price and price>110000: continue
        ttaf  = get_hours(block, r"TTAF|total\s*time")
        smoh  = get_hours(block, r"SMOH|engine\s*time")
        n     = get_n(block)
        loc_m = re.search(r'Location\s*[:\-]?\s*([A-Za-z\s]+,\s*[A-Z]{2})', block, re.I)
        loc   = loc_m.group(1).strip() if loc_m else None
        url   = urls[i] if i<len(urls) else "https://www.controller.com"
        results.append(make_listing("controller", url, model, tier, year, price, ttaf, smoh, get_engine_type(block), get_avionics(block), block[:600], loc, n))
        log.info(f"  Controller: {year} {model['model']} ${price:,}" if price else f"  Controller: {year} {model['model']}")
    return results

def parse_barnstormers(subj, body):
    results = []
    blocks = re.split(r'\n{3,}', body) or [body]
    urls   = re.findall(r'(https?://(?:www\.)?barnstormers\.com/\S+)', body)
    for i,block in enumerate(blocks):
        block=block.strip()
        if not block: continue
        model = find_model(block) or find_model(subj)
        if not model: continue
        tier  = 1 if model in TIER1 else 2
        year  = get_year(block) or get_year(subj)
        price = get_price(block) or get_price(subj)
        if price and price>110000: continue
        ttaf  = get_hours(block, r"TTAF|total\s*time")
        smoh  = get_hours(block, r"SMOH|TSOH")
        n     = get_n(block)
        loc_m = re.search(r'([A-Z][a-z]+(?:\s[A-Z][a-z]+)*,\s*[A-Z]{2})\b', block)
        loc   = loc_m.group(1).strip() if loc_m else None
        url   = urls[i] if i<len(urls) else (urls[0] if urls else "https://www.barnstormers.com")
        results.append(make_listing("barnstormers", url, model, tier, year, price, ttaf, smoh, get_engine_type(block), get_avionics(block), block[:600], loc, n))
        log.info(f"  Barnstormers: {year} {model['model']} ${price:,}" if price else f"  Barnstormers: {year} {model['model']}")
    return results

# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    # Always work in the directory where this script lives
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    log.info(f"Working in: {os.getcwd()}")

    svc      = gmail_service()
    watch_id = get_or_make_label(svc, WATCH_LABEL)
    done_id  = get_or_make_label(svc, DONE_LABEL)

    q = f"label:{WATCH_LABEL} -label:{DONE_LABEL}"
    msgs = svc.users().messages().list(userId="me", q=q, maxResults=50).execute().get("messages",[])
    log.info(f"Found {len(msgs)} unprocessed emails")

    new = []
    for msg in msgs:
        subj, frm, body = get_body(svc, msg["id"])
        src = detect(frm, subj)
        if not src:
            log.info(f"  Skipping: {frm}")
            mark_done(svc, msg["id"], done_id)
            continue
        log.info(f"  Parsing {src}: {subj[:70]}")
        if src=="tradeaplane":    parsed = parse_tap(subj, body)
        elif src=="controller":   parsed = parse_controller(subj, body)
        elif src=="barnstormers": parsed = parse_barnstormers(subj, body)
        else:                     parsed = []
        new.extend(parsed)
        mark_done(svc, msg["id"], done_id)

    log.info(f"Parsed {len(new)} listings from emails")

    # Load existing
    try:
        with open(OUTPUT) as f:
            existing = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        existing = []

    # Merge — keep existing, add truly new (by id)
    existing_ids = {l["id"] for l in existing}
    added = [l for l in new if l["id"] not in existing_ids]

    # Update price history for existing listings
    existing_map = {l["id"]:l for l in existing}
    for l in new:
        if l["id"] in existing_map and l["price"]:
            prev = existing_map[l["id"]]
            last = prev["priceHistory"][-1]["price"] if prev["priceHistory"] else None
            if l["price"] != last:
                prev["priceHistory"].append({"price":l["price"],"date":today()})

    combined = existing + added
    with open(OUTPUT,"w") as f:
        json.dump(combined, f, indent=2)

    log.info(f"Saved {len(combined)} total listings ({len(added)} new) to {OUTPUT}")
    log.info(f"File size: {os.path.getsize(OUTPUT)} bytes")
    return added

if __name__ == "__main__":
    added = run()
    print(f"\nDone. {len(added)} new listings saved to {OUTPUT}")
