#!/usr/bin/env python3
# scripts/sap_b1_requests.py
"""
SAP Business One Service Layer extraction, one call per "List" request in the
Postman collection "Alkhozama - SAP Business One Service Layer - BI Data APIs
(Read-Only)". Read-only: the only POSTs are /Login, /Logout and SAP service
actions that only return data.

How it works:
  1. Reads credentials from .env (never from this file, never printed).
  2. POST /Login -> B1SESSION (+ ROUTEID) cookies, kept in memory for the run.
     A failed login is never retried, so the SAP user does not get locked.
  3. Each entity is read from its first record, page by page (header Prefer:
     odata.maxpagesize=PAGE_SIZE), following "odata.nextLink" until it is absent,
     with no page limit. full and all have no date filter; only delta uses FROM_DATE.
     Every page is saved as-is to OUT_DIR/run_<YYYYMMDD_HHMMSS>/<entity>/page_0001.json,
     so each run is a complete copy of its own and never mixes with an older run.
  4. Jobs (each entity, plus the services block) run WORKERS at a time (default 8),
     each worker on its own SAP session; when one job finishes, the next starts.
     Sessions log in one by one first; if SAP refuses a later login, the run goes
     on with the sessions it has. A summary with result and time per entity is
     printed at the end.
  5. Only HTTP codes, row counts and page counts are printed, never field values.
  6. A 401 (session expired) logs in again once and repeats the request. A page
     that fails with a network error or HTTP 429/5xx is retried 3 times (30, 60
     and 90 seconds apart) before that entity is reported as failed.
  7. Every session logs out at the end, also after errors or Ctrl+C, to release
     the SAP license. Ctrl+C lets in-flight pages finish first.

Where it sits: local capture helper, like scripts/sevenrooms_requests.sh.
Upgrade path: the same calls move into a connector Lambda (Bronze raw/ in S3)
with credentials in Secrets Manager.

Required in .env (gitignored). Wrap values in single quotes if they contain
spaces, # or quotes:
  SAP_B1_COMPANY_DB=...
  SAP_B1_USERNAME=...      (domain users: DOMAIN\\user)
  SAP_B1_PASSWORD=...
Variables already exported in your shell win over .env, so you can override one
run, e.g. FROM_DATE=2026-09-01 python scripts/sap_b1_requests.py delta

Optional:
  SAP_B1_BASE_URL (default https://akmobapp.alkhozama.com:50000/b1s/v1)
  PAGE_SIZE (default 100, SAP recommends max 500), FROM_DATE (default 2026-01-01),
  MAX_PAGES (default 0 = no limit), OUT_DIR (default raw_data/sap_business_one/sap_data)
  WORKERS (default 8 jobs at a time; 0 = every job at once, 1 = one at a time)

Usage (from the repo root, with .venv active):
  python scripts/sap_b1_requests.py login                  # check login only
  python scripts/sap_b1_requests.py entities               # list entities (no network)
  python scripts/sap_b1_requests.py counts                 # row count per entity ($count)
  python scripts/sap_b1_requests.py services               # read-only service actions
  python scripts/sap_b1_requests.py full ar_invoices items # full load of some entities
  python scripts/sap_b1_requests.py full                   # full load of every entity
  python scripts/sap_b1_requests.py delta                  # changed since FROM_DATE
  python scripts/sap_b1_requests.py all                    # services + full

WARNING: responses hold business partner names, addresses, phones, bank and tax
data and SAP user details. OUT_DIR must stay gitignored. Do not share these files.

Not covered (each call needs a key or parameter): the "Get ... by key" requests,
Reconciliation (read by ReconNum only), Get Currency Rate, Get Document Series and
Get Asset Values (per asset). Journal Vouchers have no read endpoint in SAP.
"""
import builtins
import http.cookiejar
import json
import os
import queue
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from dotenv import load_dotenv

ENV_FILE = os.getenv("ENV_FILE", ".env")
load_dotenv(ENV_FILE)  # override=False: shell variables win

BASE_URL = os.getenv("SAP_B1_BASE_URL", "https://akmobapp.alkhozama.com:50000/b1s/v1").rstrip("/")
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "100"))
FROM_DATE = os.getenv("FROM_DATE", "2026-01-01")
MAX_PAGES = int(os.getenv("MAX_PAGES", "0")) or sys.maxsize  # 0 = no limit
OUT_DIR = Path(os.getenv("OUT_DIR", "raw_data/sap_business_one/sap_data"))
RUN_DIR = OUT_DIR / time.strftime("run_%Y%m%d_%H%M%S")
TIMEOUT = 900  # seconds; with every entity at once, heavy pages (items ~11 MB) can wait long on the server
WORKERS = int(os.getenv("WORKERS", "8"))  # 8 jobs at a time keeps the client's SAP server comfortable; 0 = all at once
RETRY_STATUS = {0, 429, 500, 502, 503, 504}  # 0 = network error or timeout
RETRIES = 3
STOP = threading.Event()  # set on Ctrl+C or a fatal error: workers stop after their current page
PRINT_LOCK = threading.Lock()


def print(*args, **kwargs):
    """builtins.print under a lock, so lines from parallel workers never run together."""
    with PRINT_LOCK:
        builtins.print(*args, **kwargs)

# name: (resource, $orderby key, "changed since" date field, fixed $filter)
# Date field as in the collection: UpdateDate where SAP has it. Journal Entries,
# Payments, Activities and Fixed-Asset documents only have a posting/document date,
# so schedule a periodic full reload for them. None = setup table, always full.
ENTITIES = {
    # 01 Administration
    "currencies": ("Currencies", "Code", None, None),
    "user_permission_tree": ("UserPermissionTree", "PermissionID", None, None),
    "users": ("Users", "InternalKey", None, None),
    "user_groups": ("UserGroups", "UserGroupId", None, None),
    "sales_employees": ("SalesPersons", "SalesEmployeeCode", None, None),
    "freight": ("AdditionalExpenses", "ExpensCode", None, None),
    "customer_groups": ("BusinessPartnerGroups", "Code", None, "Type eq 'bbpgt_CustomerGroup'"),
    "vendor_groups": ("BusinessPartnerGroups", "Code", None, "Type eq 'bbpgt_VendorGroup'"),
    "bp_properties": ("BusinessPartnerProperties", "PropertyCode", None, None),
    "banks": ("Banks", "AbsoluteEntry", None, None),
    "house_bank_accounts": ("HouseBankAccounts", "AbsoluteEntry", None, None),
    "item_groups": ("ItemGroups", "Number", None, None),
    "item_properties": ("ItemProperties", "Number", None, None),
    "warehouses": ("Warehouses", "WarehouseCode", None, None),
    "units_of_measure": ("UnitOfMeasurements", "AbsEntry", None, None),
    "uom_groups": ("UnitOfMeasurementGroups", "AbsEntry", None, None),
    "length_units": ("LengthMeasures", "UnitCode", None, None),
    "weight_units": ("WeightMeasures", "UnitCode", None, None),
    "approval_stages": ("ApprovalStages", "Code", None, None),
    "approval_templates": ("ApprovalTemplates", "Code", None, None),
    "alerts": ("AlertManagements", "Code", None, None),
    # 02 Financials
    "chart_of_accounts": ("ChartOfAccounts", "Code", None, None),
    "journal_entries": ("JournalEntries", "JdtNum", "ReferenceDate", None),
    "projects": ("Projects", "Code", None, None),
    "cash_flow_line_items": ("CashFlowLineItems", "LineItemID", None, None),
    "tax_groups": ("VatGroups", "Code", None, None),
    "withholding_tax_codes": ("WithholdingTaxCodes", "WTCode", None, None),
    "fixed_assets": ("Items", "ItemCode", "UpdateDate", "ItemType eq 'itFixedAssets'"),
    "asset_capitalization": ("AssetCapitalization", "DocEntry", "PostingDate", None),
    "asset_retirement": ("AssetRetirement", "DocEntry", "PostingDate", None),
    "budget_scenarios": ("BudgetScenarios", "Numerator", None, None),
    "budget_distributions": ("BudgetDistributions", "DivisionCode", None, None),
    "budgets": ("Budgets", "Numerator", None, None),
    "project_wbs": ("PRJ", "Code", "UpdateDate", None),
    "dimensions": ("Dimensions", "DimensionCode", None, None),
    "cost_centers": ("ProfitCenters", "CenterCode", None, None),
    "distribution_rules": ("DistributionRules", "FactorCode", None, None),
    # 03 Sales A/R
    "sales_quotations": ("Quotations", "DocEntry", "UpdateDate", None),
    "sales_orders": ("Orders", "DocEntry", "UpdateDate", None),
    "ar_invoices": ("Invoices", "DocEntry", "UpdateDate", None),
    "ar_credit_memos": ("CreditNotes", "DocEntry", "UpdateDate", None),
    # 04 Purchasing A/P
    "purchase_requests": ("PurchaseRequests", "DocEntry", "UpdateDate", None),
    "purchase_quotations": ("PurchaseQuotations", "DocEntry", "UpdateDate", None),
    "purchase_orders": ("PurchaseOrders", "DocEntry", "UpdateDate", None),
    "goods_receipt_po": ("PurchaseDeliveryNotes", "DocEntry", "UpdateDate", None),
    "ap_down_payment_requests": ("PurchaseDownPayments", "DocEntry", "UpdateDate", "DownPaymentType eq 'dptRequest'"),
    "ap_invoices": ("PurchaseInvoices", "DocEntry", "UpdateDate", None),
    "ap_credit_memos": ("PurchaseCreditNotes", "DocEntry", "UpdateDate", None),
    # 05 Business Partners
    "business_partners": ("BusinessPartners", "CardCode", "UpdateDate", None),
    "activities": ("Activities", "ActivityCode", "ActivityDate", None),
    # 06 Banking
    "incoming_payments": ("IncomingPayments", "DocEntry", "DocDate", None),
    "outgoing_payments": ("VendorPayments", "DocEntry", "DocDate", None),
    # 07 Inventory
    "items": ("Items", "ItemCode", "UpdateDate", None),
    "goods_receipts": ("InventoryGenEntries", "DocEntry", "UpdateDate", None),
    "goods_issues": ("InventoryGenExits", "DocEntry", "UpdateDate", None),
    "inventory_transfer_requests": ("InventoryTransferRequests", "DocEntry", "UpdateDate", None),
    "inventory_transfers": ("StockTransfers", "DocEntry", "UpdateDate", None),
    # 08 Service
    "service_calls": ("ServiceCalls", "ServiceCallID", "UpdateDate", None),
    "equipment_cards": ("CustomerEquipmentCards", "EquipmentCardNum", None, None),
    "service_contracts": ("ServiceContracts", "ContractID", None, None),
    "skill_tech": ("SKILL", "Code", "UpdateDate", None),
    "skilled_technicians": ("U_SKL_TECH", "Code", None, None),
    "service_call_details": ("SERVICES", "DocEntry", "UpdateDate", None),
    # 09 Rent Services (add-on)
    "leasing_contracts": ("CONTRACT", "DocEntry", "UpdateDate", None),
    "department_authorization": ("DeptAuth", "DocEntry", "UpdateDate", None),
}

# Same $select as the collection's "List Users" request.
SELECT = {
    "users": "InternalKey,UserCode,UserName,Superuser,eMail,MobilePhoneNumber,Defaults,FaxNumber,"
    "Branch,Department,LanguageCode,Locked,Group,MaxDiscountGeneral,MaxDiscountSales,"
    "MaxDiscountPurchase,CashLimit,MaxCashAmtForIncmngPayts,LastLogoutDate,LastLoginTime,"
    "LastLogoutTime,LastPasswordChangeTime,LastPasswordChangedBy,NaturalPer,U_BPCR,U_CUSTAPL,"
    "U_TENAPL,U_WANO,U_WAEXP,U_FINAPL,U_WAATC,U_WADUS,UserGroupByUser,UserBranchAssignment",
}

# Read-only service actions (POST, no parameters). GetPeriods also drives
# CompanyService_GetFinancePeriods once per period category.
SERVICES = [
    "SBOBobService_GetLocalCurrency",
    "SBOBobService_GetSystemCurrency",
    "CompanyService_GetCompanyInfo",
    "CompanyService_GetAdminInfo",
    "CompanyService_GetPeriods",
    "DepreciationAreasService_GetList",
    "LicenseService_GetInstallationNumber",
]


def sap_error(raw):
    """SAP error text from {"error": {"message": {"value": ...}}}, else the start of the body."""
    try:
        return json.loads(raw)["error"]["message"]["value"]
    except Exception:
        return raw[:200].decode(errors="replace").strip()


def encode(params):
    return urllib.parse.urlencode(params, quote_via=urllib.parse.quote, safe="$',")


def query(name, since):
    """OData query options for one entity; since=True adds the 'changed since' filter."""
    resource, orderby, date_field, fixed = ENTITIES[name]
    filters = [f for f in (fixed, f"{date_field} ge '{FROM_DATE}'" if since and date_field else None) if f]
    params = {"$orderby": orderby}
    if filters:
        params["$filter"] = " and ".join(filters)
    if name in SELECT:
        params["$select"] = SELECT[name]
    return f"{resource}?{encode(params)}"


class Session:
    def __init__(self):
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))

    def call(self, method, path, body=None, retry=True):
        """Returns (status, body bytes). Status 0 = network or TLS error."""
        url = path if "://" in path else f"{BASE_URL}/{path}"
        data = json.dumps(body).encode() if body is not None else (b"" if method == "POST" else None)
        req = urllib.request.Request(url, data=data, method=method, headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Prefer": f"odata.maxpagesize={PAGE_SIZE}",
        })
        try:
            with self.opener.open(req, timeout=TIMEOUT) as r:
                status, raw = r.status, r.read()
        except urllib.error.HTTPError as e:
            status, raw = e.code, e.read()
        except OSError as e:
            return 0, str(getattr(e, "reason", e)).encode()
        if status == 401 and retry:  # session expired (SAP code 301)
            print("session expired, logging in again")
            self.login()
            return self.call(method, path, body, retry=False)
        return status, raw

    def login(self):
        creds = {"CompanyDB": "SAP_B1_COMPANY_DB", "UserName": "SAP_B1_USERNAME", "Password": "SAP_B1_PASSWORD"}
        missing = [v for v in creds.values() if not os.getenv(v)]
        if missing:
            sys.exit(f"Missing {', '.join(missing)}. Add them to {ENV_FILE}.")
        status, raw = self.call("POST", "Login", {k: os.getenv(v) for k, v in creds.items()}, retry=False)
        if status != 200:
            sys.exit(f"login: FAILED (HTTP {status}: {sap_error(raw)}). Check SAP_B1_* in {ENV_FILE}. "
                     "Not retrying, so the SAP user does not get locked.")
        print(f"login: OK (session timeout {json.loads(raw).get('SessionTimeout')} min)")

    def logout(self):
        status, _ = self.call("POST", "Logout", retry=False)
        print(f"logout: HTTP {status}")


def pull(s, name, since=False):
    """All pages of one entity -> RUN_DIR/<name>/page_NNNN.json. Returns a short result for the summary."""
    out = RUN_DIR / name
    url, page, rows = query(name, since), 0, 0
    while url and page < MAX_PAGES:
        if STOP.is_set():
            return f"interrupted after page {page} ({rows} rows)"
        page += 1
        for attempt in range(1, RETRIES + 2):
            status, raw = s.call("GET", url)
            if status not in RETRY_STATUS or attempt > RETRIES:
                break
            print(f"{name} page {page}: HTTP {status} {sap_error(raw)}, retry {attempt}/{RETRIES} in {30 * attempt}s")
            if STOP.wait(30 * attempt):  # Ctrl+C ends the wait at once
                return f"interrupted after page {page - 1} ({rows} rows)"
        if status != 200:
            print(f"{name} page {page}: HTTP {status} {sap_error(raw)}, stopping")
            return f"HTTP {status} on page {page} ({rows} rows saved)"
        out.mkdir(parents=True, exist_ok=True)  # only once data arrives: no empty folder for a blocked screen
        (out / f"page_{page:04d}.json").write_bytes(raw)
        body = json.loads(raw)
        n = len(body.get("value", []))
        rows += n
        print(f"{name} page {page}: rows {n} (total {rows})")
        nxt = body.get("odata.nextLink")
        url = urllib.parse.urljoin(BASE_URL + "/", nxt) if nxt else None
    if url:
        print(f"{name}: stopped at MAX_PAGES={MAX_PAGES}, more rows exist")
    date_field = ENTITIES[name][2]
    mode = f"changed since {FROM_DATE} ({date_field})" if since and date_field else "full"
    print(f"{name}: {rows} rows, {mode} -> {out}")
    return f"{rows} rows" + (f", stopped at MAX_PAGES={MAX_PAGES}" if url else "")


def counts(s):
    """GET /<Resource>/$count per entity: a plain number, no data saved."""
    for name, (resource, _, _, fixed) in ENTITIES.items():
        q = f"?{encode({'$filter': fixed})}" if fixed else ""
        status, raw = s.call("GET", f"{resource}/$count{q}")
        result = raw.decode(errors="replace").strip() if status == 200 else sap_error(raw)
        print(f"{name:30} HTTP {status}  {result}")


def services(s):
    out = RUN_DIR / "services"
    out.mkdir(parents=True, exist_ok=True)

    def save(action, status, raw, suffix=""):
        f = out / f"{action}{suffix}.json"
        if status == 200:
            f.write_bytes(raw)
        print(f"{action}{suffix}: HTTP {status}" + (f" -> {f}" if status == 200 else f" {sap_error(raw)}"))

    for action in SERVICES:
        status, raw = s.call("POST", action)
        save(action, status, raw)
        if action == "CompanyService_GetPeriods" and status == 200:
            for p in json.loads(raw).get("value", []):
                entry = p["AbsoluteEntry"]
                body = {"PeriodCategoryParams": {"AbsoluteEntry": entry}}
                save("CompanyService_GetFinancePeriods", *s.call("POST", "CompanyService_GetFinancePeriods", body),
                     suffix=f"_{entry}")


def run_parallel(jobs, since):
    """Runs jobs ("services" or entity names) on up to WORKERS threads, one SAP session each,
    so threads never share cookies or a re-login."""
    sessions, results = queue.Queue(), {}

    def run(job):
        s, t0 = sessions.get(), time.monotonic()
        try:
            if job == "services":
                services(s)
                result = "done"
            else:
                result = pull(s, job, since)
        except Exception as e:  # one broken entity must not stop the others
            result = f"FAILED {type(e).__name__}: {e}"
        finally:
            sessions.put(s)
        results[job] = f"{result} in {time.monotonic() - t0:.0f}s"

    try:
        for i in range(min(WORKERS or len(jobs), len(jobs))):  # one by one: a wrong password fails once, no lockout
            s = Session()
            try:
                s.login()
            except SystemExit as e:
                if i == 0:
                    raise  # first login failed: credentials, stop here
                print(f"login {i + 1} refused ({str(e.code).split('. Check')[0]}), going on with {i} sessions")
                break
            sessions.put(s)
        print(f"running {len(jobs)} jobs on {sessions.qsize()} parallel sessions -> {RUN_DIR}")
        with ThreadPoolExecutor(max_workers=sessions.qsize()) as pool:
            futures = [pool.submit(run, job) for job in jobs]
            try:
                for f in as_completed(futures):
                    f.result()
            except BaseException:  # Ctrl+C, or a failed re-login (sys.exit) in a worker
                STOP.set()
                print("stopping: waiting for in-flight pages, then logging out")
                pool.shutdown(cancel_futures=True)
                raise
    finally:
        while not sessions.empty():
            sessions.get().logout()
        if results:
            print("\nsummary:")
            for job in jobs:
                print(f"  {job:30} {results.get(job, 'not run')}")


def main(argv):
    cmd, names = (argv[1] if len(argv) > 1 else "help"), argv[2:]
    if cmd == "entities":
        for name, (resource, _, date_field, _) in ENTITIES.items():
            print(f"{name:30} {resource:28} {date_field or 'full reload'}")
        return
    if cmd not in ("login", "counts", "services", "full", "delta", "all"):
        print(__doc__)
        return
    unknown = [n for n in names if n not in ENTITIES]
    if unknown:
        sys.exit(f"Unknown entities: {', '.join(unknown)}. Run 'entities' to list them.")

    if cmd in ("login", "counts"):
        s = Session()
        s.login()
        try:
            if cmd == "counts":
                counts(s)
        finally:
            s.logout()
        return
    jobs = ["services"] if cmd in ("services", "all") else []
    if cmd in ("full", "delta", "all"):
        jobs += names or list(ENTITIES)
    run_parallel(jobs, since=cmd == "delta")


if __name__ == "__main__":
    main(sys.argv)
