#!/usr/bin/env bash
# tmp/scripts/sevenrooms_requests.sh
# -----------------------------------------------------------------------------
# SevenRooms API requests as curl, one for each request in the Postman collection
# "SevenRooms AKIC: RAW + ANALYTICS (GET only)". Read-only: the only POST is /auth.
#
# How it works:
#   1. Reads credentials from .env (never from this file, never printed).
#   2. POST /auth  -> token (sent as the raw Authorization header, no "Bearer").
#   3. GET requests save each response body to OUT_DIR/<endpoint>/... (venues/,
#      reservations/, reservations_export/, charges/). A rerun overwrites files in
#      place: delete old reservations/page_*.json first if the new pull has fewer pages.
#   4. Reservations follow data.cursor page by page until no cursor or no results.
#   5. Probes (clients, events, ...) only print the HTTP status; bodies are discarded.
#
# Upgrade path:
#   The same calls move into the connector Lambda later; this script is for
#   capturing demo data and checking which endpoints this tenant exposes.
#
# Required in .env (gitignored):
#   SEVENROOMS_CLIENT_ID=...
#   SEVENROOMS_CLIENT_SECRET=...
#   SEVENROOMS_VENUE_GROUP_ID=...
# Variables already exported in your shell win over .env, so you can override
# one run, e.g. UPDATED_SINCE=2026-09-01 bash tmp/scripts/sevenrooms_requests.sh reservations-all
#
# Optional:
#   SEVENROOMS_BASE_URL (default https://api.sevenrooms.com/2_4)
#   LIMIT (default 100), UPDATED_SINCE (default 2026-10-01), MAX_PAGES (default 500),
#   OUT_DIR (default raw_data/sevenrooms/sevenrooms_data)
#
# Usage (from the repo root):
#   bash tmp/scripts/sevenrooms_requests.sh auth              # check login only
#   bash tmp/scripts/sevenrooms_requests.sh venues            # 1.1
#   bash tmp/scripts/sevenrooms_requests.sh reservations      # 1.2 first page only
#   bash tmp/scripts/sevenrooms_requests.sh reservations-all  # 1.3 + 1.4 all pages since UPDATED_SINCE
#   bash tmp/scripts/sevenrooms_requests.sh export            # 1.5 (may return 404)
#   bash tmp/scripts/sevenrooms_requests.sh charges           # 1.6 for every venue (may return 404)
#   bash tmp/scripts/sevenrooms_requests.sh probes            # 3.1 to 3.7, status codes only
#   bash tmp/scripts/sevenrooms_requests.sh all               # everything above
#   bash tmp/scripts/sevenrooms_requests.sh check             # one call per collection GET: HTTP, rows, DATA or not
#
# WARNING: reservation responses contain guest PII (names, phones, emails).
# OUT_DIR must be gitignored. Do not email, paste in Teams, or commit these files.
#
# Single requests, if you prefer to copy and paste (set TOKEN after the first one):
#   curl -sS -X POST "$BASE_URL/auth" -H "Content-Type: application/x-www-form-urlencoded" \
#        --data-urlencode "client_id=$SEVENROOMS_CLIENT_ID" \
#        --data-urlencode "client_secret=$SEVENROOMS_CLIENT_SECRET" \
#        --data-urlencode "venue_group_id=$SEVENROOMS_VENUE_GROUP_ID"
#   curl -sS "$BASE_URL/venues" -H "Authorization: $TOKEN" -o venues.json
#   curl -sS "$BASE_URL/reservations?limit=100" -H "Authorization: $TOKEN" -o reservations_p1.json
#   curl -sS "$BASE_URL/reservations?limit=100&updated_since=2026-10-01" -H "Authorization: $TOKEN"
#   curl -sS "$BASE_URL/reservations?limit=100&cursor=<data.cursor from previous page>" -H "Authorization: $TOKEN"
#   curl -sS "$BASE_URL/reservations/export?venue_group_id=$SEVENROOMS_VENUE_GROUP_ID&updated_since=2026-10-01" -H "Authorization: $TOKEN"
#   curl -sS "$BASE_URL/venues/<venue_id>/charges" -H "Authorization: $TOKEN"
# -----------------------------------------------------------------------------
set -euo pipefail

ENV_FILE="${ENV_FILE:-.env}"

# Load .env without overriding variables already set in the shell (so
# "UPDATED_SINCE=2026-09-01 bash tmp/scripts/sevenrooms_requests.sh ..." wins).
load_env() {
  [ -f "$ENV_FILE" ] || return 0
  local key value
  while IFS='=' read -r key value || [ -n "$key" ]; do
    key="${key%$'\r'}"; key="${key#export }"; key="${key// /}"
    [[ -z "$key" || "$key" == \#* ]] && continue
    value="${value%$'\r'}"; value="${value%\"}"; value="${value#\"}"; value="${value%\'}"; value="${value#\'}"
    [ -z "${!key:-}" ] && export "$key=$value"
  done < "$ENV_FILE"
  return 0
}
load_env

BASE_URL="${SEVENROOMS_BASE_URL:-https://api.sevenrooms.com/2_4}"
LIMIT="${LIMIT:-100}"
UPDATED_SINCE="${UPDATED_SINCE:-2026-10-01}"
MAX_PAGES="${MAX_PAGES:-500}"
OUT_DIR="${OUT_DIR:-raw_data/sevenrooms/sevenrooms_data}"
RUN_DIR="$OUT_DIR"
TOKEN=""

need() { : "${!1:?Missing $1. Add it to $ENV_FILE}"; }

# Read one value from a JSON file: json_field <file> <python expression on d>
json_field() {
  python3 - "$1" "$2" <<'PY'
import json, sys
try:
    d = json.load(open(sys.argv[1]))
    v = eval(sys.argv[2], {"d": d})
    print("" if v is None else v)
except Exception:
    print("")
PY
}

urlencode() { python3 -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' "$1"; }

auth() {
  need SEVENROOMS_CLIENT_ID; need SEVENROOMS_CLIENT_SECRET; need SEVENROOMS_VENUE_GROUP_ID
  mkdir -p "$RUN_DIR"
  local tmp code
  tmp="$(mktemp)"
  code="$(curl -sS -o "$tmp" -w '%{http_code}' -X POST "$BASE_URL/auth" \
    -H "Accept: application/json" \
    -H "Content-Type: application/x-www-form-urlencoded" \
    --data-urlencode "client_id=$SEVENROOMS_CLIENT_ID" \
    --data-urlencode "client_secret=$SEVENROOMS_CLIENT_SECRET" \
    --data-urlencode "venue_group_id=$SEVENROOMS_VENUE_GROUP_ID")"
  TOKEN="$(json_field "$tmp" 'd["data"]["token"]')"
  local expires; expires="$(json_field "$tmp" 'd["data"].get("token_expiration_datetime")')"
  rm -f "$tmp"
  if [ "$code" != "200" ] || [ -z "$TOKEN" ]; then
    echo "auth: FAILED (HTTP $code). Check the client_id, client_secret and venue_group_id in $ENV_FILE." >&2
    exit 1
  fi
  echo "auth: OK (token expires ${expires:-unknown})"
}

# api_get <path with query> <output file>  -> prints the HTTP status code
api_get() {
  mkdir -p "$(dirname "$2")"
  curl -sS -o "$2" -w '%{http_code}' -X GET "$BASE_URL$1" \
    -H "Accept: application/json" -H "Authorization: $TOKEN"
}

count_rows() {
  json_field "$1" 'len((d.get("data") or {}).get("results") or (d.get("data") or {}).get("charges") or (d["data"] if isinstance(d.get("data"), list) else []))'
}

venues() {
  local f="$RUN_DIR/venues/venues.json" code
  code="$(api_get "/venues" "$f")"
  echo "1.1 venues: HTTP $code, rows $(count_rows "$f") -> $f"
}

reservations_first_page() {
  local f="$RUN_DIR/reservations_page1/page_0001.json" code
  code="$(api_get "/reservations?limit=$LIMIT" "$f")"
  echo "1.2 reservations (first page): HTTP $code, rows $(count_rows "$f") -> $f"
}

reservations_all() {
  local page=1 cursor="" prev="" total=0 path f code n
  while [ "$page" -le "$MAX_PAGES" ]; do
    path="/reservations?limit=$LIMIT&updated_since=$UPDATED_SINCE"
    [ -n "$cursor" ] && path="$path&cursor=$(urlencode "$cursor")"
    f="$RUN_DIR/reservations/page_$(printf '%04d' "$page").json"
    code="$(api_get "$path" "$f")"
    if [ "$code" != "200" ]; then echo "1.3 reservations page $page: HTTP $code, stopping"; break; fi
    n="$(count_rows "$f")"; total=$((total + ${n:-0}))
    echo "1.3/1.4 reservations page $page: rows $n (total $total)"
    [ "${n:-0}" -eq 0 ] && { rm -f "$f"; break; }
    prev="$cursor"
    cursor="$(json_field "$f" '(d.get("data") or {}).get("cursor")')"
    if [ -z "$cursor" ] || [ "$cursor" = "$prev" ]; then break; fi
    page=$((page + 1))
  done
  echo "reservations: $total rows since $UPDATED_SINCE -> $RUN_DIR/reservations/"
}

export_backfill() {
  local f="$RUN_DIR/reservations_export/export.json" code
  code="$(api_get "/reservations/export?venue_group_id=$(urlencode "$SEVENROOMS_VENUE_GROUP_ID")&updated_since=$UPDATED_SINCE" "$f")"
  echo "1.5 reservations/export: HTTP $code -> $f"
}

charges() {
  local vf="$RUN_DIR/venues/venues.json" ids id f code
  [ -f "$vf" ] || venues
  ids="$(json_field "$vf" '"\n".join(v["id"] for v in d["data"]["results"])')"
  for id in $ids; do
    f="$RUN_DIR/charges/${id}.json"
    code="$(api_get "/venues/$id/charges" "$f")"
    echo "1.6 charges ${id:0:12}...: HTTP $code, rows $(count_rows "$f")"
  done
}

probes() {
  local p code
  for p in "/clients/export?venue_group_id=$(urlencode "$SEVENROOMS_VENUE_GROUP_ID")" /clients /events \
           /reservation-requests /programs /payout-profiles /feedback; do
    code="$(curl -sS -o /dev/null -w '%{http_code}' -X GET "$BASE_URL$p" \
      -H "Accept: application/json" -H "Authorization: $TOKEN")"
    echo "3.x probe ${p%%\?*}: HTTP $code"
  done
}

# One call per GET in the Postman collection (first page only). Prints HTTP status,
# row count and DATA / EMPTY / NO DATA, plus key names or the API "msg", never values.
# Bodies are deleted straight away (clients and feedback hold guest PII).
# 2.1 to 2.4 are the same GETs as 1.1, 1.2, 1.3 and 1.6, so they are not repeated.
check() {
  local vf="$RUN_DIR/venues/venues.json" tmp vg venue_id cursor id path code rows verdict
  [ -f "$vf" ] || venues
  venue_id="$(json_field "$vf" 'd["data"]["results"][0]["id"]')"
  vg="$(urlencode "$SEVENROOMS_VENUE_GROUP_ID")"
  tmp="$(mktemp)"
  api_get "/reservations?limit=$LIMIT" "$tmp" > /dev/null
  cursor="$(json_field "$tmp" '(d.get("data") or {}).get("cursor")')"
  while IFS='|' read -r id path; do
    code="$(api_get "$path" "$tmp")"
    rows="$(count_rows "$tmp")"
    if [ "$code" = "200" ] && [ "${rows:-0}" -gt 0 ]; then
      verdict="DATA"
    elif [ "$code" = "200" ]; then
      verdict="EMPTY (data keys: $(json_field "$tmp" 'list(d["data"]) if isinstance(d.get("data"), dict) else type(d.get("data")).__name__'))"
    else
      verdict="NO DATA ($(json_field "$tmp" 'd.get("msg")'))"
    fi
    printf '%-30s HTTP %s  rows %-4s %s\n' "$id" "$code" "${rows:-0}" "$verdict"
  done <<EOF
1.1 venues|/venues
1.2 reservations (page)|/reservations?limit=$LIMIT
1.3 reservations updated_since|/reservations?limit=$LIMIT&updated_since=$UPDATED_SINCE
1.4 reservations next cursor|/reservations?limit=$LIMIT&cursor=$(urlencode "$cursor")
1.5 reservations/export|/reservations/export?venue_group_id=$vg&updated_since=$UPDATED_SINCE
1.6 venues/{id}/charges|/venues/$venue_id/charges
3.1 clients/export|/clients/export?venue_group_id=$vg
3.2 clients|/clients
3.3 events|/events
3.4 reservation-requests|/reservation-requests
3.5 programs|/programs
3.6 payout-profiles|/payout-profiles
3.7 feedback|/feedback
EOF
  rm -f "$tmp"
}

cmd="${1:-help}"
case "$cmd" in
  auth)             auth ;;
  venues)           auth; venues ;;
  reservations)     auth; reservations_first_page ;;
  reservations-all) auth; reservations_all ;;
  export)           auth; export_backfill ;;
  charges)          auth; charges ;;
  probes)           auth; probes ;;
  all)              auth; venues; reservations_all; export_backfill; charges; probes ;;
  check)            auth; check ;;
  *) sed -n '2,44p' "$0"; exit 0 ;;
esac