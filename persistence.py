"""
persistence.py
--------------
Thin Flask server that:
  1. Serves listings_raw.json to the dashboard as a JSON API
  2. Accepts PATCH requests to save manual fields (paint, interior,
     annual date, noAccident, dismissed status) back to the file
  3. Serves the built dashboard (index.html) as a static file

This is what you run locally (or on a free-tier host like Render/Railway)
so the dashboard can load real listings and persist your manual edits.

Usage:
    pip install flask flask-cors
    python persistence.py
    # Open http://localhost:5050 in your browser

The dashboard React app should fetch from /api/listings on load
and PATCH /api/listings/:id for manual field updates.
"""

import json
import os
from datetime import datetime
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

app = Flask(__name__, static_folder="dist")
CORS(app)

LISTINGS_FILE = "listings_raw.json"
DISMISSED_FILE = "dismissed.json"


def load_listings():
    try:
        with open(LISTINGS_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def save_listings(listings):
    with open(LISTINGS_FILE, "w") as f:
        json.dump(listings, f, indent=2)


def load_dismissed():
    try:
        with open(DISMISSED_FILE) as f:
            return set(json.load(f))
    except (FileNotFoundError, json.JSONDecodeError):
        return set()


def save_dismissed(dismissed_set):
    with open(DISMISSED_FILE, "w") as f:
        json.dump(list(dismissed_set), f)


# ── API routes ────────────────────────────────────────────────────────────────

@app.route("/api/listings", methods=["GET"])
def get_listings():
    listings = load_listings()
    dismissed = load_dismissed()
    # Attach dismissed flag — dashboard can filter or show differently
    for l in listings:
        l["dismissed"] = l["id"] in dismissed
    return jsonify(listings)


@app.route("/api/listings/<listing_id>", methods=["PATCH"])
def patch_listing(listing_id):
    """
    Update manual fields on a listing. Accepted fields:
      paintCondition, interiorCondition, annualDate,
      noAccident, notes, dismissed
    """
    listings = load_listings()
    dismissed = load_dismissed()
    data = request.json or {}

    EDITABLE_FIELDS = [
        "paintCondition", "interiorCondition", "annualDate",
        "noAccident", "notes"
    ]

    updated = False
    for l in listings:
        if l["id"] == listing_id:
            for field in EDITABLE_FIELDS:
                if field in data:
                    l[field] = data[field]
            l["_lastEdited"] = datetime.utcnow().isoformat()
            updated = True
            break

    if "dismissed" in data:
        if data["dismissed"]:
            dismissed.add(listing_id)
        else:
            dismissed.discard(listing_id)
        save_dismissed(dismissed)

    if not updated and "dismissed" not in data:
        return jsonify({"error": "Listing not found"}), 404

    save_listings(listings)
    return jsonify({"ok": True})


@app.route("/api/listings/<listing_id>/dismiss", methods=["POST"])
def dismiss_listing(listing_id):
    dismissed = load_dismissed()
    dismissed.add(listing_id)
    save_dismissed(dismissed)
    return jsonify({"ok": True, "dismissed": listing_id})


@app.route("/api/stats", methods=["GET"])
def get_stats():
    listings = load_listings()
    dismissed = load_dismissed()
    active = [l for l in listings if l["id"] not in dismissed]
    return jsonify({
        "total": len(active),
        "tier1": sum(1 for l in active if l.get("tier") == 1),
        "tier2": sum(1 for l in active if l.get("tier") == 2),
        "sources": {s: sum(1 for l in active if l.get("source") == s)
                    for s in ["controller","tradeaplane","aopa","barnstormers","gmax"]},
        "lastUpdated": datetime.utcnow().isoformat(),
    })


# ── Static file serving ───────────────────────────────────────────────────────

@app.route("/", defaults={"path": ""})
@app.route("/<path:path>")
def serve_static(path):
    if path and os.path.exists(os.path.join(app.static_folder, path)):
        return send_from_directory(app.static_folder, path)
    return send_from_directory(app.static_folder, "index.html")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    print(f"Aircraft Monitor running at http://localhost:{port}")
    app.run(host="0.0.0.0", port=port, debug=False)
