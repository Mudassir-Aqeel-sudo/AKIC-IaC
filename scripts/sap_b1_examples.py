#!/usr/bin/env python3
# tmp/scripts/sap_b1_examples.py
"""
Downloads every example response from the public Postman documentation of the
"Alkhozama - SAP Business One Service Layer - BI Data APIs (Read-Only)" collection
in one go, so they can be inspected together instead of one by one in the browser.

No credentials, no SAP call: it only reads the public documenter JSON
(the same data the documentation page shows under "Example Response").

Output (OUT_DIR, default raw_data/sap_business_one/sap_example_response_data, gitignored):
  <folder>/<sub folder>/<request>.json   one file per 2xx example, pretty-printed
  index.csv                              folder, request, method, path, status,
                                         rows, fields, file (one line per example)
Error examples (404 / 401) are skipped: they all share SAP's error shape.

Note from the documentation: samples are real responses with personal and
commercial data masked, and nested line collections (e.g. DocumentLines) are
shortened to their first element. Row counts here describe the sample, not SAP.

Where it sits: input for inspecting and validating SAP B1 data before the
connector is built. Upgrade path: none needed; live pulls go through the
connector (or tmp/scripts/sap_b1_requests.py) once credentials exist.

Usage (from the repo root):
  python tmp/scripts/sap_b1_examples.py
"""
import csv
import json
import os
import re
import sys
import urllib.request
from pathlib import Path

COLLECTION_URL = ("https://documenter.gw.postman.com/api/collections/10675543/2sBYHXSKAS"
                  "?segregateAuth=true&versionTag=latest")
BASE_URL = "https://akmobapp.alkhozama.com:50000/b1s/v1"
OUT_DIR = Path(os.getenv("OUT_DIR", "raw_data/sap_business_one/sap_example_response_data/"))


def slug(text):
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


def shape(body):
    """(rows, fields) of a sample: OData list -> len(value); single record -> 1."""
    if isinstance(body, dict) and isinstance(body.get("value"), list):
        rows = body["value"]
        return len(rows), len(rows[0]) if rows and isinstance(rows[0], dict) else 0
    if isinstance(body, dict):
        return 1, len([k for k in body if not k.startswith("odata.")])
    return 1, 0


def examples(items, path=()):
    """Yields (folder path, request item, response) for every 2xx example with a body."""
    for item in items:
        if "item" in item:
            yield from examples(item["item"], path + (item["name"],))
            continue
        for resp in item.get("response", []):
            if 200 <= (resp.get("code") or 0) < 300 and (resp.get("body") or "").strip():
                yield path, item, resp


def main():
    # Postman's documenter rejects the default "Python-urllib" User-Agent with 403.
    req = urllib.request.Request(COLLECTION_URL, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as r:
        collection = json.load(r)
    print(f"collection: {collection['info']['name']}")

    index, seen = [], set()
    for path, item, resp in examples(collection["item"]):
        folder = OUT_DIR.joinpath(*(slug(p) for p in path))
        name = slug(item["name"])
        f = folder / f"{name}.json"
        n = 2
        while f in seen:  # same request name twice in one folder
            f, n = folder / f"{name}_{n}.json", n + 1
        seen.add(f)

        try:
            body = json.loads(resp["body"])
        except json.JSONDecodeError:
            body = resp["body"]
        folder.mkdir(parents=True, exist_ok=True)
        f.write_text(json.dumps(body, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

        req = item.get("request", {})
        url = req.get("url", {})
        raw_url = url.get("raw", "") if isinstance(url, dict) else url
        rows, fields = shape(body)
        index.append({
            "folder": " > ".join(path),
            "request": item["name"],
            "method": req.get("method"),
            "path": raw_url.replace(BASE_URL, "").replace("{{baseUrl}}", ""),
            "status": resp.get("code"),
            "rows": rows,
            "fields": fields,
            "file": f.relative_to(OUT_DIR).as_posix(),
        })

    with open(OUT_DIR / "index.csv", "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(index[0]))
        writer.writeheader()
        writer.writerows(index)

    by_top = {}
    for row in index:
        top = row["folder"].split(" > ")[0]
        by_top[top] = by_top.get(top, 0) + 1
    for top, count in by_top.items():
        print(f"  {top:40} {count} examples")
    print(f"saved {len(index)} examples -> {OUT_DIR}/ (index: {OUT_DIR / 'index.csv'})")


if __name__ == "__main__":
    sys.exit(main())
