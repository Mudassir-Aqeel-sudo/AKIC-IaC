#!/usr/bin/env python3
# tmp/scripts/sevenrooms_extract.py
"""
SevenRooms API extraction: every GET in the Postman collection "SevenRooms AKIC:
RAW + ANALYTICS (GET only)", all pages, saved as indented (readable) JSON with
the content unchanged. Ends with a summary and the
list of inaccessible links (non-200), so you can see what this account can read.
Read-only: the only POST is /auth.

How it works:
  1. Reads credentials from .env (never from this file, never printed).
  2. POST /auth -> token (sent as the raw Authorization header, no "Bearer").
  3. Each endpoint is read page by page, following data.cursor until there is no
     cursor or no rows. Every page is saved to OUT_DIR/<endpoint>/page_0001.json
     (charges: OUT_DIR/charges/<venue_id>/page_0001.json).
  4. 401 -> logs in again once. 429 (rate limit) -> waits and retries, 3 times max.
  5. Only status codes, row counts and SevenRooms error messages are printed, never
     field values. The venue group id is masked in printed links.

Collection mapping: 1.1 venues, 1.3 + 1.4 reservations (updated_since + cursor),
1.5 reservations/export, 1.6 charges for every venue, 3.1 to 3.7 (saved when they
return data). 1.2 is the first page of 1.4 without the date filter, and 2.1 to 2.4
are the same GETs as 1.x, so they are not repeated.

Where it sits: local capture helper next to tmp/scripts/sevenrooms_requests.sh.
Upgrade path: the same calls move into the connector Lambda, with the guest-PII
allow-list applied before anything is stored or logged.

Required in .env (gitignored):
  SEVENROOMS_CLIENT_ID=...
  SEVENROOMS_CLIENT_SECRET=...
  SEVENROOMS_VENUE_GROUP_ID=...
Variables already exported in your shell win over .env, so you can override one
run, e.g. UPDATED_SINCE=2026-09-01 python tmp/scripts/sevenrooms_extract.py reservations

Optional:
  SEVENROOMS_BASE_URL (default https://api.sevenrooms.com/2_4)
  LIMIT (default 100), UPDATED_SINCE (default 2026-10-01), MAX_PAGES (default 500),
  OUT_DIR (default raw_data/sevenrooms/sevenrooms_data)

Usage (from the repo root, with .venv active):
  python tmp/scripts/sevenrooms_extract.py                       # every endpoint
  python tmp/scripts/sevenrooms_extract.py reservations charges  # only these
  python tmp/scripts/sevenrooms_extract.py list                  # endpoint names (no network)

A rerun overwrites pages in place: delete an endpoint's old page_*.json first if
the new pull may have fewer pages.

WARNING: reservations, clients, feedback and reservation requests hold guest PII
(names, phones, emails). Their folders are gitignored. Do not share these files.
"""
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from dotenv import load_dotenv

ENV_FILE = os.getenv("ENV_FILE", ".env")
load_dotenv(ENV_FILE)  # override=False: shell variables win

BASE_URL = os.getenv("SEVENROOMS_BASE_URL", "https://api.sevenrooms.com/2_4").rstrip("/")
LIMIT = os.getenv("LIMIT", "100")
UPDATED_SINCE = os.getenv("UPDATED_SINCE", "2026-10-01")
MAX_PAGES = int(os.getenv("MAX_PAGES", "500"))
OUT_DIR = Path(os.getenv("OUT_DIR", "raw_data/sevenrooms/sevenrooms_data"))
VENUE_GROUP_ID = os.getenv("SEVENROOMS_VENUE_GROUP_ID", "")
TIMEOUT = 120  # seconds

# folder: (collection id, path, query as in the collection). Charges run once per venue.
ENDPOINTS = {
    "venues": ("1.1", "/venues", {}),
    "reservations": ("1.3+1.4", "/reservations", {"limit": LIMIT, "updated_since": UPDATED_SINCE}),
    "reservations_export": ("1.5", "/reservations/export",
                            {"venue_group_id": VENUE_GROUP_ID, "updated_since": UPDATED_SINCE}),
    "charges": ("1.6", "/venues/{venue_id}/charges", {}),
    "clients_export": ("3.1", "/clients/export", {"venue_group_id": VENUE_GROUP_ID}),
    "clients": ("3.2", "/clients", {}),
    "events": ("3.3", "/events", {}),
    "reservation_requests": ("3.4", "/reservation-requests", {}),
    "programs": ("3.5", "/programs", {}),
    "payout_profiles": ("3.6", "/payout-profiles", {}),
    "feedback": ("3.7", "/feedback", {}),
}

RESULTS = []  # (verdict, label, detail, link)


def parse(raw):
    try:
        body = json.loads(raw)
    except ValueError:
        return {}
    return body if isinstance(body, dict) else {"data": body}


def pretty(raw):
    """Indented JSON (2 spaces) so saved pages are readable; non-JSON bodies stay as they are."""
    try:
        return (json.dumps(json.loads(raw), indent=2, ensure_ascii=False) + "\n").encode()
    except ValueError:
        return raw


def rows_of(body):
    """The row list of a page: data.results, data.charges, or the first list in data."""
    data = body.get("data")
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return next((v for v in data.values() if isinstance(v, list)), [])
    return []


def cursor_of(body):
    data = body.get("data")
    return data.get("cursor") if isinstance(data, dict) else None


def error_of(raw):
    body = parse(raw)
    return body.get("msg") or body.get("message") or raw[:120].decode(errors="replace").strip()


def mask(url):
    return url.replace(VENUE_GROUP_ID, "<venue_group_id>") if VENUE_GROUP_ID else url


class Client:
    def __init__(self):
        self.token = ""

    @staticmethod
    def request(method, url, data=None, headers=None):
        """Returns (status, body bytes, headers). Status 0 = network or TLS error."""
        req = urllib.request.Request(url, data=data, method=method,
                                     headers={"Accept": "application/json", **(headers or {})})
        try:
            with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
                return r.status, r.read(), r.headers
        except urllib.error.HTTPError as e:
            return e.code, e.read(), e.headers
        except OSError as e:
            return 0, str(getattr(e, "reason", e)).encode(), {}

    def auth(self):
        creds = {k: os.getenv(f"SEVENROOMS_{k.upper()}", "") for k in ("client_id", "client_secret", "venue_group_id")}
        missing = [f"SEVENROOMS_{k.upper()}" for k, v in creds.items() if not v]
        if missing:
            sys.exit(f"Missing {', '.join(missing)}. Add them to {ENV_FILE}.")
        status, raw, _ = self.request("POST", f"{BASE_URL}/auth", urllib.parse.urlencode(creds).encode(),
                                      {"Content-Type": "application/x-www-form-urlencoded"})
        data = parse(raw).get("data")
        token = data.get("token") if isinstance(data, dict) else None
        if status != 200 or not token:
            sys.exit(f"auth: FAILED (HTTP {status}: {error_of(raw)}). "
                     f"Check the SEVENROOMS_* values in {ENV_FILE}.")
        self.token = token
        print(f"auth: OK (token expires {data.get('token_expiration_datetime', 'unknown')})")

    def get(self, url):
        status, raw = 0, b""
        for attempt in range(4):
            status, raw, headers = self.request("GET", url, headers={"Authorization": self.token})
            if status == 401 and attempt == 0:
                print("token rejected, logging in again")
                self.auth()
                continue
            if status == 429 and attempt < 3:
                retry_after = str(headers.get("Retry-After") or "")
                wait = int(retry_after) if retry_after.isdigit() else 10
                print(f"rate limited, waiting {wait}s")
                time.sleep(wait)
                continue
            break
        return status, raw


def pull(c, label, path, query, out):
    """All pages of one endpoint -> out/page_NNNN.json, then one RESULTS entry."""
    cursor, page, rows, url = None, 0, 0, ""
    while page < MAX_PAGES:
        q = {**query, **({"cursor": cursor} if cursor else {})}
        url = f"{BASE_URL}{path}" + (f"?{urllib.parse.urlencode(q)}" if q else "")
        status, raw = c.get(url)
        if status != 200:
            if page == 0:
                RESULTS.append(("INACCESSIBLE", label, f"HTTP {status} {error_of(raw)}", mask(url)))
                print(f"{label}: HTTP {status} {error_of(raw)}")
                return
            print(f"{label} page {page + 1}: HTTP {status} {error_of(raw)}, stopping")
            break
        body = parse(raw)
        n = len(rows_of(body))
        if n == 0 and page > 0:  # trailing empty page: nothing to save
            break
        page += 1
        rows += n
        out.mkdir(parents=True, exist_ok=True)
        (out / f"page_{page:04d}.json").write_bytes(pretty(raw))
        print(f"{label} page {page}: rows {n} (total {rows})")
        previous, cursor = cursor, cursor_of(body)
        if not cursor or cursor == previous or n == 0:
            break
    else:
        print(f"{label}: stopped at MAX_PAGES={MAX_PAGES}, more rows may exist")
    verdict = "DATA" if rows else "EMPTY"
    RESULTS.append((verdict, label, f"{rows} rows, {page} pages -> {out}", mask(url)))


def venue_ids():
    ids = []
    for f in sorted((OUT_DIR / "venues").glob("page_*.json")):
        ids += [v["id"] for v in rows_of(parse(f.read_bytes())) if isinstance(v, dict) and "id" in v]
    return ids


def summary():
    print("\n=== SUMMARY ===")
    for verdict, label, detail, _ in RESULTS:
        print(f"{verdict:13} {label:42} {detail}")
    bad = [r for r in RESULTS if r[0] == "INACCESSIBLE"]
    print(f"\nInaccessible links ({len(bad)} of {len(RESULTS)}):")
    for _, label, detail, link in bad:
        print(f"  {detail:40} GET {link}")
    if not bad:
        print("  none")


def main(argv):
    names = argv[1:]
    if names == ["list"]:
        for name, (cid, path, _) in ENDPOINTS.items():
            print(f"{name:22} {cid:8} GET {path}")
        return
    if names and names[0] in ("help", "-h", "--help"):
        print(__doc__)
        return
    unknown = [n for n in names if n not in ENDPOINTS]
    if unknown:
        sys.exit(f"Unknown endpoints: {', '.join(unknown)}. Run 'list' to see them.")

    selected = [n for n in ENDPOINTS if n in names] or list(ENDPOINTS)
    if "charges" in selected and "venues" not in selected and not venue_ids():
        selected.insert(0, "venues")  # charges need the venue ids

    c = Client()
    c.auth()
    for name in selected:
        cid, path, query = ENDPOINTS[name]
        if name == "charges":
            for vid in venue_ids():
                pull(c, f"{cid} charges {vid[:12]}...", path.format(venue_id=urllib.parse.quote(vid)),
                     query, OUT_DIR / "charges" / vid)
        else:
            pull(c, f"{cid} {name}", path, query, OUT_DIR / name)
    summary()


if __name__ == "__main__":
    main(sys.argv)
