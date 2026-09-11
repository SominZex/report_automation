"""
purchase_sales_stock_report.py — Purchase/Sales/Stock Report by Product & Brand Group
────────────────────────────────────────────────────────────────────────────
Runs two queries over a DYNAMIC rolling 6-COMPLETE-MONTH window, merges in
current stock (from `store_product_snapshot`, a Postgres table this script
populates itself), and builds a single .xlsx **in memory** (never written to
disk) with three sheets:

  1. "Product Wise"            — purchase vs sales vs stock, per (brand,
                                  barcode), FULL OUTER JOIN. Includes a
                                  "Brand Group" column mapped from the
                                  brand_sub table (sub_brand -> brand_group);
                                  brands with no brand_sub row fall back to
                                  showing their own brand name as the group.
  2. "Brand Group"              — purchase vs sales vs stock, summed over the
                                  WHOLE selected window per brand_group (no
                                  vendor or month breakdown). Start Date,
                                  Legal Name, TOT Validity, and Off Invoice
                                  Margin (%) come from the brand portal
                                  (brand_group_meta). Off Invoice Value is
                                  blank until the margin is set.
  3. "Mondelez Product Wise"    — same product-level grain as sheet 1, but
                                  filtered to brand_group == MONDELEZ_BRAND_GROUP
                                  and reshaped to the columns: Brand (Group),
                                  Sub Brand, Product Name, barcode, Purchase
                                  Qty, Purchase Amount (₹, Ex-GST), Sale Qty,
                                  Sale Revenue (₹, Ex-GST), Current Stock Qty,
                                  Current Stock Amt (₹), Gross Margin on
                                  Sales (%).

STOCK DOWNLOAD — integrated directly into this script (from
current_stock_downloader.py), so running this ONE script does the whole
pipeline: download the current stock snapshot for every store in the
`store_contacts` table, THEN generate the report from it. No separate
scheduled job is needed to run the downloader first. If some individual
stores fail to download but at least one store's data was captured, the
report proceeds anyway (with a warning); only a total failure (nothing
fetched at all) aborts the run.

STORE LIST & STOCK STORAGE — both now live in Postgres instead of CSV files:
  - Store list: `store_contacts` (distinct values of the "store" column),
    replacing the old partner.csv.
  - Stock snapshot: `store_product_snapshot`, replacing the old
    stock_combined/all_stores_stock.csv. This table is TRUNCATED and then
    reloaded on every run — it always holds exactly the latest snapshot,
    nothing historical. Note: the table's columns were created unquoted, so
    Postgres folded them to lowercase (e.g. `productId` -> `productid`) —
    this script reads/writes using those lowercase names and immediately
    renames back to the camelCase names the rest of the pipeline expects.

DATE WINDOW — always 6 full calendar months, excluding whatever month the
script is run in, regardless of which day of the current month it runs on:

    Run date        -> Window used
    2026-09-07       -> 2026-03-01 to 2026-08-31  (Mar, Apr, May, Jun, Jul, Aug)
    2026-09-28       -> 2026-03-01 to 2026-08-31  (same — day-of-month doesn't matter)
    2026-01-15       -> 2025-07-01 to 2025-12-31  (year boundary handled)

STOCK HANDLING:
  - Stock is a CURRENT-DAY snapshot only — it has no historical month
    breakdown and no vendor breakdown. There is no way to compute "stock
    as of March" or "stock for Vendor X" from this data.
  - Product-wise sheets: stock is summed network-wide (across all stores)
    per (brand, barcode) and merged onto the matching product row.
  - Brand-group sheet: stock is summed network-wide per brand_group.

DELIVERY — the finished workbook is never saved to local disk. It's built
into an in-memory buffer and either attached directly to the report email
(small files) or streamed straight to Zoho WorkDrive for a share link
(large files) — see deliver_report().

CONFIG — secrets (DB creds, stock-API creds, mail creds, Zoho creds) are
read via _config(), which checks Streamlit's secrets manager first (when
running inside the Streamlit portal) and falls back to environment
variables / .env (for the standalone script / Airflow DAG). See
secrets.toml.example for the expected keys.
────────────────────────────────────────────────────────────────────────────
"""

from dotenv import load_dotenv
load_dotenv()

import io
import os
import time
import base64
import smtplib
import logging
from io import StringIO
from datetime import date, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

import pandas as pd
import requests
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.pool import NullPool
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# =============================================================================
# CONFIG — Streamlit secrets first, then environment variables (.env)
# =============================================================================
# Works in both contexts this module runs in:
#   - the Streamlit portal (brand_date_portal.py), which can populate
#     .streamlit/secrets.toml — see secrets.toml.example
#   - the standalone script / Airflow DAG, which has no Streamlit runtime
#     and relies on a .env file / real environment variables instead.

def _config(key: str, default: str | None = None) -> str | None:
    try:
        import streamlit as st  # local import: streamlit may not be installed
        if key in st.secrets:
            return st.secrets[key]
    except Exception:
        pass
    return os.getenv(key, default)


def require_config(key: str) -> str:
    value = _config(key)
    if not value:
        raise RuntimeError(
            f"Missing required config value: {key}. Set it in .streamlit/secrets.toml "
            f"(Streamlit portal) or in your .env / environment (standalone script)."
        )
    return value


DB_URI = (
    f"postgresql+psycopg2://"
    f"{require_config('DB_USER')}:{require_config('DB_PASSWORD')}"
    f"@{require_config('DB_HOST')}:{_config('DB_PORT', '5432')}"
    f"/{require_config('DB_NAME')}"
)

engine = create_engine(
    DB_URI,
    poolclass=NullPool,
    connect_args={
        "connect_timeout": 10,
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 10,
        "keepalives_count": 5,
    },
)

# ── Stock downloader config (integrated from current_stock_downloader.py,
# so this script can run standalone: download stock -> generate report,
# with no separate scheduled job needed to run first).
STOCK_API_LOGIN_URL = _config("STOCK_API_LOGIN_URL", "https://api.thenewshop.in/login")
STOCK_API_STOCK_URL = _config("STOCK_API_STOCK_URL", "https://api.thenewshop.in/store-stocks/report/storeStocksCSV")
STOCK_API_USERNAME  = require_config("STOCK_API_USERNAME")
STOCK_API_PASSWORD  = require_config("STOCK_API_PASSWORD")

# ── Zoho WorkDrive config (cloud upload + public link) — same pattern as
# monday_previous_month_reports.py. Used automatically whenever the finished
# report exceeds ATTACH_SIZE_LIMIT_MB.
ZOHO_ACCOUNTS_BASE           = _config("ZOHO_ACCOUNTS_BASE", "https://accounts.zoho.in")
ZOHO_API_BASE                = _config("ZOHO_API_BASE", "https://www.zohoapis.in")
ZOHO_WORKDRIVE_CLIENT_ID     = _config("ZOHO_WORKDRIVE_CLIENT_ID", "")
ZOHO_WORKDRIVE_CLIENT_SECRET = _config("ZOHO_WORKDRIVE_CLIENT_SECRET", "")
ZOHO_WORKDRIVE_REFRESH_TOKEN = _config("ZOHO_WORKDRIVE_REFRESH_TOKEN", "")
ZOHO_WORKDRIVE_FOLDER_ID     = _config("ZOHO_WORKDRIVE_FOLDER_ID", "")

# ── Report email config (Zoho Mail SMTP) ──────────────────────────────────
REPORT_MAIL_SMTP_SERVER = _config("REPORT_MAIL_SMTP_SERVER", "smtp.zoho.in")
REPORT_MAIL_SMTP_PORT   = int(_config("REPORT_MAIL_SMTP_PORT", "465"))
REPORT_MAIL_SENDER      = require_config("REPORT_MAIL_SENDER")
REPORT_MAIL_PASSWORD    = require_config("REPORT_MAIL_PASSWORD")
REPORT_MAIL_RECIPIENT   = require_config("REPORT_MAIL_RECIPIENT")


def _parse_email_list(value: str) -> list[str]:
    """Splits a comma-separated string into a clean list of email addresses.
    Empty/whitespace-only entries are dropped."""
    if not value:
        return []
    return [addr.strip() for addr in value.split(",") if addr.strip()]


REPORT_MAIL_CC  = _parse_email_list(_config("REPORT_MAIL_CC", ""))
REPORT_MAIL_BCC = _parse_email_list(_config("REPORT_MAIL_BCC", ""))

# Above this size (MB), the report is uploaded to WorkDrive and only a link
# is emailed, instead of attaching the file directly.
ATTACH_SIZE_LIMIT_MB = float(_config("REPORT_ATTACH_SIZE_LIMIT_MB", "20"))

# brand_group value (as it appears in brand_sub.brand_group) that the
# "Mondelez Product Wise" sheet filters down to.
MONDELEZ_BRAND_GROUP = _config("MONDELEZ_BRAND_GROUP", "Mondelez")

# Same brand cohort used in both ad-hoc queries — kept exactly as given,
# including the near-duplicate spellings (Toblerone/Tobelrone, Dobra/DOBRA,
# Dizzle/DIZZLES) which are intentionally NOT normalized here, matching the
# original queries. Used only as a one-time seed for
# brand_portal_selected_brands — see get_selected_brands() below.
BRANDS = [
    "UE Boost", "The Good Vibe", "PAT INDUSTRIES - VEDASTIKA", "Farmley",
    "Jimmys", "Rite Bite", "HOT WHEELS", "Dabur", "UNO", "Coca Cola",
    "Superyou", "Vadilal", "BOURNVITA", "Fitspire", "Voll Pro", "Rio",
    "Cornitos", "Cadbury", "Celebrations", "Oreo", "BOURNVILLE",
    "Toblerone", "Tobelrone", "Mogu Mogu", "2:00 PM", "Kettle", "Nutro",
    "Ganesh Bhel", "WhyFryy", "Dobra", "DOBRA", "Walkers", "POPART",
    "Kokozo", "Ketchup", "Olives Etc", "Hersheys", "Burff", "Mattel",
    "Tempt Wellness", "Cake Tale", "Phab", "Tang", "For Men", "Dizzle",
    "DIZZLES", "Harvest Gold", "Millet Monk", "Malee", "A House of Oud",
    "Averon", "EatElite", "The Good Tempt", "Laruna", "TeepiTaap",
    "Barbie", "Baskin Robbins",
]


# =============================================================================
# DB helper — retry wrapper (same pattern as the other automated scripts)
# =============================================================================

def safe_read_sql(query, params=None, retries=5, delay=3) -> pd.DataFrame:
    for attempt in range(retries):
        try:
            with engine.connect() as conn:
                return pd.read_sql(query, conn, params=params)
        except OperationalError as e:
            error_str = str(e)
            log.warning(f"Database error (attempt {attempt + 1}/{retries}): {error_str[:150]}")
            if attempt == retries - 1:
                raise RuntimeError(f"Query failed after {retries} retries: {error_str[:200]}")
            wait_time = delay * (2 ** attempt)
            log.info(f"Waiting {wait_time}s before retry...")
            time.sleep(wait_time)
            if "EOF" in error_str or "closed" in error_str.lower() or "terminated" in error_str.lower():
                log.info("Recreating database connection pool...")
                engine.dispose()
        except Exception as e:
            log.warning(f"Unexpected error (attempt {attempt + 1}/{retries}): {e}")
            if attempt == retries - 1:
                raise RuntimeError(f"Query failed: {e}")
            time.sleep(delay)
    raise RuntimeError("Query failed after multiple retries.")


# =============================================================================
# Selected brands — now driven by the brand_portal_selected_brands table
# =============================================================================
# On first run, if the table doesn't exist yet or is empty, this seeds it
# with the original BRANDS list and returns that — so nothing changes in
# the report until you actually add/remove brands via the portal. BRANDS
# itself stays in the file purely as that seed value.

def get_selected_brands() -> list[str]:
    try:
        df = safe_read_sql('SELECT "brand_name" FROM brand_portal_selected_brands ORDER BY "brand_name"')
    except Exception as e:
        log.warning(
            f"Could not read brand_portal_selected_brands ({e}) — falling back to the "
            f"built-in BRANDS list. Run brand_portal_schema.sql if you haven't yet."
        )
        return list(BRANDS)

    if df.empty:
        log.info("brand_portal_selected_brands is empty — seeding it with the built-in BRANDS list.")
        with engine.begin() as conn:
            conn.execute(
                text(
                    'INSERT INTO brand_portal_selected_brands ("brand_name") '
                    'VALUES (:brand_name) ON CONFLICT ("brand_name") DO NOTHING'
                ),
                [{"brand_name": b} for b in BRANDS],
            )
        return list(BRANDS)

    return df["brand_name"].tolist()


# =============================================================================
# Dynamic date window — last 6 COMPLETE calendar months, current month excluded
# =============================================================================

def get_last_6_months_window(run_date: date | None = None) -> tuple[date, date, date]:
    """
    Returns (start_date, end_exclusive, end_date_inclusive):
      - start_date       : 1st of the month that is 6 months before/inclusive
                            of the last complete month
      - end_exclusive     : 1st of the CURRENT month (use as `< end_exclusive`
                            in SQL so the current, still-open month is
                            always excluded, no matter what day it is)
      - end_date_inclusive: last day of the most recent COMPLETE month
                            (for display purposes only)

    Examples (run_date -> window):
      2026-09-07 -> (2026-03-01, 2026-09-01, 2026-08-31)  # Mar-Aug 2026
      2026-09-28 -> (2026-03-01, 2026-09-01, 2026-08-31)  # same, day doesn't matter
      2026-01-15 -> (2025-07-01, 2026-01-01, 2025-12-31)  # year boundary
    """
    run_date = run_date or date.today()
    first_of_this_month = run_date.replace(day=1)
    end_exclusive = first_of_this_month
    end_date_inclusive = first_of_this_month - timedelta(days=1)

    end_month_first = end_date_inclusive.replace(day=1)
    year = end_month_first.year
    month = end_month_first.month - 5  # 5 months back from the last complete month = 6 months total
    while month <= 0:
        month += 12
        year -= 1
    start_date = date(year, month, 1)

    return start_date, end_exclusive, end_date_inclusive


# =============================================================================
# Query 1 — Product-wise (FULL OUTER JOIN on brandName + barcode)
# =============================================================================

PRODUCT_WISE_QUERY = """
WITH purchase AS (
    SELECT
        "brandName",
        TRIM("barcode") AS "barcode",
        MAX("productName") AS "productName",
        SUM("recievedQuantity") AS purchase_qty,
        SUM("totalCost") AS purchase_amount
    FROM grn_data
    WHERE "brandName" = ANY(%(brands)s)
      AND "Date" >= %(start)s
      AND "Date" < %(end_exclusive)s
    GROUP BY "brandName", TRIM("barcode")
),
sales AS (
    SELECT
        "brandName",
        TRIM("barcode") AS "barcode",
        MAX("productName") AS "productName",
        SUM("quantity") AS sale_qty,
        SUM("orderAmountNet") AS sale_revenue
    FROM billing_data
    WHERE "brandName" = ANY(%(brands)s)
      AND "orderDate" >= %(start)s
      AND "orderDate" < %(end_exclusive)s
      AND "orderStatus" NOT IN ('CANCELLED', 'CANCELED')
    GROUP BY "brandName", TRIM("barcode")
)
SELECT
    COALESCE(p."brandName", s."brandName") AS "brandName",
    COALESCE(p."barcode", s."barcode") AS "barcode",
    COALESCE(p."productName", s."productName") AS "productName",
    COALESCE(p.purchase_qty, 0) AS "purchase_qty",
    ROUND(COALESCE(p.purchase_amount, 0), 2) AS "purchase_amount",
    COALESCE(s.sale_qty, 0) AS "sale_qty",
    ROUND(COALESCE(s.sale_revenue, 0), 2) AS "sale_revenue",
    ROUND(
        (
            (s.sale_revenue / NULLIF(s.sale_qty, 0))
            - (p.purchase_amount / NULLIF(p.purchase_qty, 0))
        )
        / NULLIF(s.sale_revenue / NULLIF(s.sale_qty, 0), 0)
        * 100,
        2
    ) AS "profit_margin_pct"
FROM purchase p
FULL OUTER JOIN sales s
    ON p."brandName" = s."brandName"
   AND p."barcode" = s."barcode"
ORDER BY "brandName", "productName"
"""


def get_product_wise(start_date: date, end_exclusive: date, brands: list[str]) -> pd.DataFrame:
    return safe_read_sql(
        PRODUCT_WISE_QUERY,
        params={"start": start_date, "end_exclusive": end_exclusive, "brands": brands},
    )


# =============================================================================
# Query 2 — Brand Group (via brand_sub mapping, no vendor/month breakdown)
# =============================================================================
# Groups purely by brand_group (from the brand_sub table you maintain:
# brand_sub.sub_brand = billing/grn brandName, brand_sub.brand_group = the
# rolled-up group name), summed across the WHOLE selected date window — no
# per-vendor or per-month breakdown.
#
# LEFT JOIN to brand_sub (not INNER): any selected brand that has no row in
# brand_sub still shows up, grouped under its own brandName as a fallback
# "group of one" — so nothing silently disappears just because brand_sub
# hasn't been populated for it yet.

BRAND_GROUP_QUERY = """
WITH purchase AS (
    SELECT
        COALESCE(bs."brand_group", g."brandName") AS "brand_group",
        SUM(g."recievedQuantity") AS purchase_qty,
        SUM(g."totalCost") AS purchase_amount
    FROM grn_data g
    LEFT JOIN brand_sub bs ON g."brandName" = bs."sub_brand"
    WHERE g."brandName" = ANY(%(brands)s)
      AND g."Date" >= %(start)s
      AND g."Date" < %(end_exclusive)s
    GROUP BY COALESCE(bs."brand_group", g."brandName")
),
sales AS (
    SELECT
        COALESCE(bs."brand_group", b."brandName") AS "brand_group",
        SUM(b."quantity") AS sale_qty,
        SUM(b."orderAmountNet") AS sale_revenue
    FROM billing_data b
    LEFT JOIN brand_sub bs ON b."brandName" = bs."sub_brand"
    WHERE b."brandName" = ANY(%(brands)s)
      AND b."orderDate" >= %(start)s
      AND b."orderDate" < %(end_exclusive)s
      AND b."orderStatus" NOT IN ('CANCELLED', 'CANCELED')
    GROUP BY COALESCE(bs."brand_group", b."brandName")
)
SELECT
    p."brand_group" AS "brand_group",
    p.purchase_qty AS "purchase_qty",
    ROUND(p.purchase_amount, 2) AS "purchase_amount",
    COALESCE(s.sale_qty, 0) AS "sale_qty",
    ROUND(COALESCE(s.sale_revenue, 0), 2) AS "sale_revenue"
FROM purchase p
LEFT JOIN sales s
    ON p."brand_group" = s."brand_group"
ORDER BY p."brand_group"
"""


def get_brand_group_summary(start_date: date, end_exclusive: date, brands: list[str]) -> pd.DataFrame:
    return safe_read_sql(
        BRAND_GROUP_QUERY,
        params={"start": start_date, "end_exclusive": end_exclusive, "brands": brands},
    )


# =============================================================================
# brand_sub mapping — sub_brand (= billing/grn brandName) -> brand_group
# =============================================================================

def get_brand_sub_mapping() -> pd.DataFrame:
    """Reads the brand_sub table you maintain (brand_group, sub_brand)."""
    return safe_read_sql('SELECT "brand_group", "sub_brand" FROM brand_sub')


# =============================================================================
# brand_group_meta — the portal-editable Start Date / Legal Name / TOT
# Validity / Off Invoice Margin (%), keyed to brand_sub.brand_group
# =============================================================================

_BRAND_GROUP_META_COLUMNS = ["brand_group", "start_date", "legal_name", "tot_validity", "off_invoice_margin_pct"]


def get_brand_group_meta() -> pd.DataFrame:
    """Reads the portal-editable metadata per brand_group. If the table
    doesn't exist yet (brand_portal_schema.sql hasn't been run), returns an
    empty frame with the right columns so the report still runs — all
    metadata columns just stay blank, same as before the portal existed."""
    try:
        return safe_read_sql(
            'SELECT "brand_group", "start_date", "legal_name", "tot_validity", "off_invoice_margin_pct" '
            'FROM brand_group_meta'
        )
    except Exception as e:
        log.warning(
            f"Could not read brand_group_meta ({e}) — metadata columns will be blank. "
            f"Run brand_portal_schema.sql if you haven't yet."
        )
        return pd.DataFrame(columns=_BRAND_GROUP_META_COLUMNS)


# =============================================================================
# Store list — now driven by store_contacts (replaces partner.csv)
# =============================================================================

def get_store_ids() -> list[int]:
    """Distinct store IDs to download stock for, from store_contacts.store.
    Non-numeric/blank values are dropped rather than raising, since the
    table may accumulate stray rows over time."""
    df = safe_read_sql('SELECT DISTINCT "store" FROM store_contacts WHERE "store" IS NOT NULL')
    store_ids = pd.to_numeric(df["store"], errors="coerce").dropna().astype(int).tolist()
    store_ids = sorted(set(store_ids))
    if not store_ids:
        raise RuntimeError("No store IDs found in store_contacts — nothing to download stock for.")
    return store_ids


# =============================================================================
# Stock Download — integrated from current_stock_downloader.py
# =============================================================================
# Same login -> per-store fetch -> combine logic as the standalone
# downloader script, folded in here so running THIS script does the whole
# pipeline (download stock, then generate the report) without needing a
# separate scheduled job to run current_stock_downloader.py first. The
# combined result is written to store_product_snapshot (truncate + reload)
# instead of a CSV file.

def _stock_api_login() -> str:
    log.info("Logging in to stock API...")
    try:
        r = requests.post(
            STOCK_API_LOGIN_URL,
            json={"username": STOCK_API_USERNAME, "password": STOCK_API_PASSWORD},
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
    except requests.RequestException as e:
        raise RuntimeError(f"Stock API login request failed: {e}")
    except ValueError:
        raise RuntimeError(f"Stock API login returned non-JSON response: {r.text}")

    token = data.get("token")
    if not token:
        raise RuntimeError(f"Stock API login succeeded but no token returned: {data}")

    log.info("Stock API login successful.")
    return token


def _fetch_store_stock(token: str, store_id: int, run_date: str) -> pd.DataFrame:
    headers = {"Authorization": token, "accept": "*/*"}

    try:
        r = requests.get(STOCK_API_STOCK_URL, headers=headers, params={"store": store_id}, timeout=60)
    except requests.RequestException as e:
        raise RuntimeError(f"Request failed: {e}")

    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")

    if not r.text.strip():
        raise RuntimeError("Empty response body")

    try:
        decoded = base64.b64decode(r.text)
    except Exception as e:
        raise RuntimeError(f"Base64 decode failed: {e}")

    if len(decoded) < 100:
        raise RuntimeError(f"Decoded payload too small ({len(decoded)} bytes) — likely invalid data")

    try:
        df = pd.read_csv(StringIO(decoded.decode("utf-8")))
    except Exception as e:
        raise RuntimeError(f"CSV parse failed: {e}")

    if df.empty:
        raise RuntimeError("Parsed CSV has no rows")

    df.insert(0, "store_id", store_id)
    df["snapshot_date"] = run_date

    log.info(f"  Store {store_id} -> {len(df)} rows fetched")
    return df


# Target columns of store_product_snapshot, exactly as Postgres folded them
# (the table was created with unquoted identifiers, so mixed-case names like
# `productId` became `productid`). Used only for the DB write — see
# save_stock_snapshot() and its rename-back in load_stock().
_SNAPSHOT_TABLE_COLUMNS = [
    "store_id", "productid", "productname", "barcode", "quantity",
    "sellingprice", "printedmrp", "costprice", "totalamount",
    "storename", "vendorname", "categoryname", "subcategoryof",
    "brand", "snapshot_date",
]


def save_stock_snapshot(combined: pd.DataFrame, run_date: str) -> int:
    """Truncates store_product_snapshot and reloads it with `combined` —
    the table always holds exactly the latest snapshot. Source column names
    are matched case-insensitively against the API's CSV headers, so this
    doesn't break if the API's exact casing shifts slightly. Returns the
    number of rows written."""
    lower_lookup = {c.lower(): c for c in combined.columns}

    out = pd.DataFrame()
    missing = []
    for target_col in _SNAPSHOT_TABLE_COLUMNS:
        source_col = lower_lookup.get(target_col)
        if source_col is None:
            missing.append(target_col)
            out[target_col] = pd.NA
        else:
            out[target_col] = combined[source_col]

    if missing:
        log.warning(
            f"Stock download is missing expected column(s) {missing} — those "
            f"columns will be NULL in store_product_snapshot."
        )

    out["store_id"] = pd.to_numeric(out["store_id"], errors="coerce")
    out["barcode"] = out["barcode"].astype(str).str.strip()
    out["brand"] = out["brand"].astype(str).str.strip()
    out["snapshot_date"] = pd.to_datetime(run_date).date()

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE store_product_snapshot"))
        out.to_sql(
            "store_product_snapshot",
            conn,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000,
        )

    log.info(f"store_product_snapshot truncated and reloaded with {len(out)} rows.")
    return len(out)


def stock_snapshot_row_count() -> int:
    try:
        df = safe_read_sql("SELECT COUNT(*) AS n FROM store_product_snapshot")
        return int(df["n"].iloc[0])
    except Exception:
        return 0


def download_current_stock(run_date: str | None = None) -> None:
    """
    Downloads the current stock snapshot for every store in store_contacts
    and writes the combined result into store_product_snapshot (truncate +
    reload — see save_stock_snapshot()).

    Same behavior as the original standalone current_stock_downloader.py:
    still RAISES at the end if ANY individual store failed to download —
    even though the snapshot table was already reloaded successfully with
    whatever stores DID succeed. The caller decides whether that's fatal
    (see the try/except around this call in run_report() below).
    """
    run_date = run_date or date.today().isoformat()
    log.info(f"Stock snapshot date: {run_date}")

    store_ids = get_store_ids()
    log.info(f"Loaded {len(store_ids)} stores from store_contacts")

    token = _stock_api_login()

    all_dfs = []
    failed = []

    for store_id in store_ids:
        try:
            df = _fetch_store_stock(token, store_id, run_date)
            all_dfs.append(df)
        except RuntimeError as e:
            log.error(f"Store {store_id} failed: {e}")
            failed.append(store_id)

    if not all_dfs:
        raise RuntimeError("No store data fetched — aborting, store_product_snapshot left unchanged")

    combined = pd.concat(all_dfs, ignore_index=True)
    log.info(f"Combined {len(combined)} rows across {len(all_dfs)} stores")

    save_stock_snapshot(combined, run_date)

    if failed:
        raise RuntimeError(f"Stock download completed with failures for stores: {failed}")

    log.info("Stock snapshot download completed successfully.")


# =============================================================================
# Stock — read the snapshot back from store_product_snapshot
# =============================================================================

def load_stock() -> pd.DataFrame:
    """
    Reads store_product_snapshot in full and renames its (lowercase-folded)
    columns back to the camelCase names the rest of the pipeline expects
    (brand, barcode, quantity, totalAmount, etc).

    NOTE: this is whatever day the downloader last ran, not tied to the
    6-month sales/purchase window — there's no historical stock to pull
    for past months (same limitation as the existing monthly report's
    stock handling).
    """
    stock_df = safe_read_sql("SELECT * FROM store_product_snapshot")

    if stock_df.empty:
        raise RuntimeError("store_product_snapshot is empty — has the stock downloader run yet?")

    stock_df = stock_df.rename(columns={
        "productid": "productId",
        "productname": "productName",
        "sellingprice": "sellingPrice",
        "printedmrp": "printedMrp",
        "costprice": "costPrice",
        "totalamount": "totalAmount",
        "storename": "storeName",
        "vendorname": "vendorName",
        "categoryname": "categoryName",
        "subcategoryof": "subCategoryOf",
    })

    required_cols = {"brand", "barcode", "quantity", "totalAmount"}
    missing = required_cols - set(stock_df.columns)
    if missing:
        raise RuntimeError(
            f"store_product_snapshot is missing expected column(s) {missing}. "
            f"Columns found: {list(stock_df.columns)}"
        )

    # Trim whitespace so a stray leading/trailing space on barcode/brand
    # doesn't create a duplicate, unmatched group here or silently fail to
    # join against the (now also trimmed) query results.
    stock_df["barcode"] = stock_df["barcode"].astype(str).str.strip()
    stock_df["brand"] = stock_df["brand"].astype(str).str.strip()

    return stock_df


def aggregate_stock_by_barcode(stock_df: pd.DataFrame) -> pd.DataFrame:
    """Network-wide stock per (brand, barcode) — used for the product-wise sheets."""
    agg = stock_df.groupby(["brand", "barcode"], as_index=False).agg(
        stock_qty=("quantity", "sum"),
        stock_amt=("totalAmount", "sum"),
    )
    return agg.rename(columns={"brand": "brandName"})


def aggregate_stock_by_brand_group(stock_df: pd.DataFrame, brand_sub_df: pd.DataFrame) -> pd.DataFrame:
    """Network-wide stock per brand_group — used for the Brand Group sheet.
    Maps each stock row's brand through brand_sub the same way the SQL
    queries do: unmapped brands fall back to their own brand name as a
    group of one, rather than being dropped."""
    mapping = brand_sub_df.set_index("sub_brand")["brand_group"]
    stock_df = stock_df.copy()
    stock_df["brand_group"] = stock_df["brand"].map(mapping).fillna(stock_df["brand"])
    return stock_df.groupby("brand_group", as_index=False).agg(
        stock_qty=("quantity", "sum"),
        stock_amt=("totalAmount", "sum"),
    )


# =============================================================================
# Merge stock into each report
# =============================================================================

def _merge_product_wise_base(
    product_df: pd.DataFrame, stock_by_barcode: pd.DataFrame, brand_sub_df: pd.DataFrame,
) -> pd.DataFrame:
    """Shared, un-renamed merge used by BOTH the full "Product Wise" sheet
    and the "Mondelez Product Wise" sheet, so the two never drift apart."""
    mapping = brand_sub_df.set_index("sub_brand")["brand_group"]

    merged = product_df.merge(stock_by_barcode, on=["brandName", "barcode"], how="left")
    merged["stock_qty"] = merged["stock_qty"].fillna(0)
    merged["stock_amt"] = merged["stock_amt"].fillna(0)

    # Same unmapped-brand fallback as the SQL brand_group queries: a brand
    # with no brand_sub row shows its own brandName as the group, rather
    # than a blank cell.
    merged["brand_group"] = merged["brandName"].map(mapping).fillna(merged["brandName"])

    return merged


def build_product_wise_sheet(merged_base: pd.DataFrame) -> pd.DataFrame:
    df = merged_base[[
        "brand_group", "brandName", "barcode", "productName", "purchase_qty", "purchase_amount",
        "sale_qty", "sale_revenue", "stock_qty", "stock_amt", "profit_margin_pct",
    ]].copy()
    df.columns = [
        "Brand Group", "Brands", "Barcode", "Product Name", "Purchase Qty", "Purchase Amount (₹)",
        "Sale Qty", "Sale Revenue (₹)", "stock qty", "stock amnt", "profit margin",
    ]
    return df


def build_mondelez_product_sheet(
    merged_base: pd.DataFrame, brand_group_name: str = MONDELEZ_BRAND_GROUP,
) -> pd.DataFrame:
    """Same grain as "Product Wise", filtered to a single brand_group and
    reshaped to the columns requested for the Mondelez-specific sheet."""
    mask = merged_base["brand_group"].astype(str).str.strip().str.casefold() == brand_group_name.casefold()
    df = merged_base.loc[mask, [
        "brand_group", "brandName", "productName", "barcode", "purchase_qty", "purchase_amount",
        "sale_qty", "sale_revenue", "stock_qty", "stock_amt", "profit_margin_pct",
    ]].copy()
    df.columns = [
        "Brand (Group)", "Sub Brand", "Product Name", "barcode", "Purchase Qty",
        "Purchase Amount (₹, Ex-GST)", "Sale Qty", "Sale Revenue (₹, Ex-GST)",
        "Current Stock Qty", "Current Stock Amt (₹)", "Gross Margin on Sales (%)",
    ]
    return df.sort_values(["Sub Brand", "Product Name"]).reset_index(drop=True)


def build_brand_group_sheet(
    brand_group_df: pd.DataFrame,
    stock_by_brand_group: pd.DataFrame,
    brand_group_meta_df: pd.DataFrame,
) -> pd.DataFrame:
    """Builds the Brand Group sheet. Start Date, Legal Name, TOT Validity,
    and Off Invoice Margin (%) come from brand_group_meta (the portal-
    editable table) — blank for any brand_group that hasn't been
    configured yet, same as before the portal existed. Off Invoice Value
    = MAX(Purchase Amount, Sale Revenue) x Off Invoice Margin (%), and
    stays blank wherever the margin itself is blank."""
    merged = brand_group_df.merge(stock_by_brand_group, on="brand_group", how="left")
    merged["stock_qty"] = merged["stock_qty"].fillna(0)
    merged["stock_amt"] = merged["stock_amt"].fillna(0)

    merged = merged.merge(brand_group_meta_df, on="brand_group", how="left")
    for col in ["start_date", "legal_name", "tot_validity", "off_invoice_margin_pct"]:
        if col not in merged.columns:
            merged[col] = pd.NA

    # NaN propagates through this multiplication automatically, so a blank
    # margin naturally yields a blank Off Invoice Value — no extra guard needed.
    base_for_margin = merged[["purchase_amount", "sale_revenue"]].max(axis=1)
    margin_fraction = pd.to_numeric(merged["off_invoice_margin_pct"], errors="coerce") / 100.0
    merged["off_invoice_value"] = (base_for_margin * margin_fraction).round(2)

    merged = merged.sort_values("brand_group").reset_index(drop=True)

    merged = merged[[
        "brand_group", "start_date", "legal_name", "tot_validity", "off_invoice_margin_pct",
        "purchase_qty", "purchase_amount", "sale_qty", "sale_revenue",
        "stock_qty", "stock_amt", "off_invoice_value",
    ]]
    merged.columns = [
        "Brand(Group)", "Start Date", "Legal Name", "TOT Validity", "Off Invoice Margin (%)",
        "Purchase Qty", "Purchase Amount (₹)", "Sale Qty", "Sale Revenue (₹)",
        "Stock Qty", "Stock Amount (₹)", "Off Invoice Value",
    ]
    return merged


# =============================================================================
# Zoho WorkDrive — upload the finished report (from memory) and get a link
# =============================================================================
# SCAFFOLD NOTE: same caveat as monday_previous_month_reports.py — Zoho
# WorkDrive's REST API field names aren't fully/consistently documented.
# The parsing below tries the field names seen most consistently, but on
# the first live run with real credentials, print upload_json/link_json in
# full and confirm the exact keys before trusting it unattended.

def zoho_get_access_token() -> str:
    """Exchange the long-lived WorkDrive refresh token for a short-lived access token."""
    if not (ZOHO_WORKDRIVE_CLIENT_ID and ZOHO_WORKDRIVE_CLIENT_SECRET and ZOHO_WORKDRIVE_REFRESH_TOKEN):
        raise RuntimeError(
            "Zoho WorkDrive credentials are not configured. Set ZOHO_WORKDRIVE_CLIENT_ID, "
            "ZOHO_WORKDRIVE_CLIENT_SECRET, ZOHO_WORKDRIVE_REFRESH_TOKEN and ZOHO_WORKDRIVE_FOLDER_ID "
            "in secrets.toml / your .env."
        )
    resp = requests.post(
        f"{ZOHO_ACCOUNTS_BASE}/oauth/v2/token",
        params={
            "grant_type": "refresh_token",
            "client_id": ZOHO_WORKDRIVE_CLIENT_ID,
            "client_secret": ZOHO_WORKDRIVE_CLIENT_SECRET,
            "refresh_token": ZOHO_WORKDRIVE_REFRESH_TOKEN,
        },
        timeout=30,
    )
    if not resp.ok:
        log.warning(f"Zoho token refresh failed ({resp.status_code}): {resp.text}")
    resp.raise_for_status()
    token_data = resp.json()
    if "access_token" not in token_data:
        raise RuntimeError(f"Zoho token refresh did not return an access_token: {token_data}")
    return token_data["access_token"]


def upload_to_workdrive_and_get_link(file_bytes: bytes, filename: str) -> str:
    """Uploads `file_bytes` into ZOHO_WORKDRIVE_FOLDER_ID (streamed straight
    from memory — never written to disk) and returns a public ("anyone with
    the link, view-only") share URL."""
    if not ZOHO_WORKDRIVE_FOLDER_ID:
        raise RuntimeError("ZOHO_WORKDRIVE_FOLDER_ID is not set in secrets.toml / your .env.")

    access_token = zoho_get_access_token()
    auth_header = {"Authorization": f"Zoho-oauthtoken {access_token}"}

    files = {"content": (filename, io.BytesIO(file_bytes))}
    data = {
        "parent_id": ZOHO_WORKDRIVE_FOLDER_ID,
        # Filenames here are deterministic (date-ranged), so a rerun for
        # the same window would otherwise hit a 409 Conflict.
        "override-name-exist": "true",
    }
    upload_resp = requests.post(
        f"{ZOHO_API_BASE}/workdrive/api/v1/upload",
        headers=auth_header,
        files=files,
        data=data,
        timeout=120,
    )
    upload_resp.raise_for_status()
    upload_json = upload_resp.json()

    try:
        file_entry = upload_json["data"][0]
        file_id = file_entry.get("attributes", {}).get("resource_id") or file_entry.get("id")
    except (KeyError, IndexError, TypeError) as e:
        raise RuntimeError(f"Could not parse WorkDrive upload response: {upload_json}") from e

    if not file_id:
        raise RuntimeError(f"WorkDrive upload succeeded but no file ID was found: {upload_json}")

    link_headers = {
        **auth_header,
        "Accept": "application/vnd.api+json",
        "Content-Type": "application/vnd.api+json",
    }
    link_payload = {
        "data": {
            "attributes": {
                "resource_id": file_id,
                "link_name": filename,
                "allow_download": True,
                "request_user_data": False,
                "role_id": "7",  # view-only
            },
            "type": "links",
        }
    }
    link_resp = requests.post(
        f"{ZOHO_API_BASE}/workdrive/api/v1/links",
        headers=link_headers,
        json=link_payload,
        timeout=30,
    )

    if not link_resp.ok:
        log.warning(f"WorkDrive link-creation failed ({link_resp.status_code}): {link_resp.text}")
        if link_resp.status_code == 400 and "already" in link_resp.text.lower():
            log.info("An external link already exists for this file — fetching it instead ...")
            existing_url = _get_existing_workdrive_link(file_id, auth_header)
            if existing_url:
                return existing_url
        link_resp.raise_for_status()

    link_json = link_resp.json()

    try:
        link_attrs = link_json["data"]["attributes"]
        share_url = link_attrs.get("link") or link_attrs.get("permalink") or link_attrs.get("url")
    except (KeyError, TypeError) as e:
        raise RuntimeError(f"Could not parse WorkDrive link-creation response: {link_json}") from e

    if not share_url:
        raise RuntimeError(f"WorkDrive link created but no URL was found in the response: {link_json}")

    return share_url


def _get_existing_workdrive_link(file_id: str, auth_header: dict) -> str | None:
    """Fallback for the "link already exists" 400 case."""
    try:
        resp = requests.get(
            f"{ZOHO_API_BASE}/workdrive/api/v1/files/{file_id}/links",
            headers={**auth_header, "Accept": "application/vnd.api+json"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json().get("data", [])
        for entry in data:
            attrs = entry.get("attributes", {})
            url = attrs.get("link") or attrs.get("permalink") or attrs.get("url")
            if url:
                return url
    except Exception as e:
        log.warning(f"Could not fetch existing WorkDrive link: {e}")
    return None


# =============================================================================
# Email — deliver the report (link for large files, attachment for small ones)
# =============================================================================

def _create_report_email_body(start_date: date, end_date_inclusive: date, file_size_mb: float,
                               share_url: str | None = None) -> str:
    """HTML body for the report email, stating the exact date range covered."""
    if share_url:
        delivery_note = f"""
            <p>File size: <strong>{file_size_mb:.1f} MB</strong> — shared via WorkDrive link
            below rather than as an attachment.</p>

            <div style="text-align: center; margin: 25px 0;">
                <a href="{share_url}" target="_blank"
                   style="background-color: #0078D7; color: #ffffff; padding: 12px 28px;
                          text-decoration: none; border-radius: 6px; font-size: 15px;
                          font-weight: bold; display: inline-block;">
                    View Report
                </a>
            </div>

            <p style="font-size: 13px; color: #777; text-align: center;">
                If the button doesn't work, copy and paste this URL into your browser:<br>
                <a href="{share_url}" style="color: #0078D7; word-break: break-all;">{share_url}</a>
            </p>
        """
    else:
        delivery_note = f"""
            <p>File size: <strong>{file_size_mb:.1f} MB</strong> — the workbook is attached
            directly to this email.</p>
        """

    return f"""
    <html>
    <body style="font-family: 'Segoe UI', Arial, sans-serif; color: #333; background-color: #f9f9f9; padding: 20px;">
        <div style="max-width: 600px; background: #ffffff; padding: 25px; border-radius: 10px; box-shadow: 0 2px 8px rgba(0,0,0,0.1);">
            <h2 style="color: #0078D7; text-align: center;">Purchase vs Sales vs Stock Report</h2>
            <hr style="border: 1px solid #0078D7; width: 80%; margin: 15px auto;">

            <p>Hi,</p>

            <p>The report covering <strong>{start_date:%d %b %Y}</strong> to
            <strong>{end_date_inclusive:%d %b %Y}</strong> (last 6 complete months) is ready,
            with Product Wise, Brand Group, and Mondelez Product Wise sheets.</p>

            {delivery_note}

            <br>
            <p>Warm regards,</p>
            <p><strong>Analytics & Insights Team</strong><br>
            <em>New Shop.</em></p>

            <hr style="margin-top: 25px;">
            <p style="font-size: 12px; color: #777; text-align: center;">
                This is an automated email. Please do not reply directly to this message.
            </p>
        </div>
    </body>
    </html>
    """


def send_report_link_email(start_date: date, end_date_inclusive: date, share_url: str, file_size_mb: float):
    """Emails REPORT_MAIL_RECIPIENT the WorkDrive share link for the report."""
    subject = f"Purchase vs Sales vs Stock Report — {start_date:%d %b} to {end_date_inclusive:%d %b %Y}"
    body = _create_report_email_body(start_date, end_date_inclusive, file_size_mb, share_url=share_url)

    msg = MIMEMultipart()
    msg["From"] = REPORT_MAIL_SENDER
    msg["To"] = REPORT_MAIL_RECIPIENT
    if REPORT_MAIL_CC:
        msg["Cc"] = ", ".join(REPORT_MAIL_CC)
    # Bcc is deliberately NOT set as a header — only added to the envelope
    # recipient list below, so it stays blind.
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "html"))

    all_recipients = [REPORT_MAIL_RECIPIENT] + REPORT_MAIL_CC + REPORT_MAIL_BCC

    with smtplib.SMTP_SSL(REPORT_MAIL_SMTP_SERVER, REPORT_MAIL_SMTP_PORT) as server:
        server.login(REPORT_MAIL_SENDER, REPORT_MAIL_PASSWORD)
        server.sendmail(REPORT_MAIL_SENDER, all_recipients, msg.as_string())

    log.info(
        f"Report link emailed to {REPORT_MAIL_RECIPIENT}"
        f"{' (CC: ' + ', '.join(REPORT_MAIL_CC) + ')' if REPORT_MAIL_CC else ''}"
        f"{' (BCC: ' + ', '.join(REPORT_MAIL_BCC) + ')' if REPORT_MAIL_BCC else ''}"
    )


def send_report_attachment_email(
    start_date: date, end_date_inclusive: date, file_bytes: bytes, filename: str, file_size_mb: float,
):
    """Emails REPORT_MAIL_RECIPIENT the report as a direct attachment, straight
    from memory — the workbook is never written to disk. Only used when
    file_size_mb <= ATTACH_SIZE_LIMIT_MB."""
    subject = f"Purchase vs Sales vs Stock Report — {start_date:%d %b} to {end_date_inclusive:%d %b %Y}"
    body = _create_report_email_body(start_date, end_date_inclusive, file_size_mb, share_url=None)

    msg = MIMEMultipart()
    msg["From"] = REPORT_MAIL_SENDER
    msg["To"] = REPORT_MAIL_RECIPIENT
    if REPORT_MAIL_CC:
        msg["Cc"] = ", ".join(REPORT_MAIL_CC)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "html"))

    part = MIMEBase("application", "octet-stream")
    part.set_payload(file_bytes)
    encoders.encode_base64(part)
    part.add_header("Content-Disposition", f'attachment; filename="{filename}"')
    msg.attach(part)

    all_recipients = [REPORT_MAIL_RECIPIENT] + REPORT_MAIL_CC + REPORT_MAIL_BCC

    with smtplib.SMTP_SSL(REPORT_MAIL_SMTP_SERVER, REPORT_MAIL_SMTP_PORT) as server:
        server.login(REPORT_MAIL_SENDER, REPORT_MAIL_PASSWORD)
        server.sendmail(REPORT_MAIL_SENDER, all_recipients, msg.as_string())

    log.info(
        f"Report attached and emailed to {REPORT_MAIL_RECIPIENT}"
        f"{' (CC: ' + ', '.join(REPORT_MAIL_CC) + ')' if REPORT_MAIL_CC else ''}"
        f"{' (BCC: ' + ', '.join(REPORT_MAIL_BCC) + ')' if REPORT_MAIL_BCC else ''}"
    )


def deliver_report(
    start_date: date, end_date_inclusive: date, file_bytes: bytes, filename: str, file_size_mb: float,
):
    """
    Single entry point that decides HOW to deliver the report and does it —
    entirely from the in-memory `file_bytes`, never touching local disk:
      - file_size_mb <= ATTACH_SIZE_LIMIT_MB  -> attach directly, no WorkDrive
      - file_size_mb  > ATTACH_SIZE_LIMIT_MB  -> stream to WorkDrive, email the link
    """
    if file_size_mb <= ATTACH_SIZE_LIMIT_MB:
        log.info(f"Report is {file_size_mb:.1f} MB (<= {ATTACH_SIZE_LIMIT_MB:.0f} MB limit) — attaching directly ...")
        send_report_attachment_email(start_date, end_date_inclusive, file_bytes, filename, file_size_mb)
        return {"delivery": "attachment"}
    else:
        log.info(f"Report is {file_size_mb:.1f} MB (> {ATTACH_SIZE_LIMIT_MB:.0f} MB limit) — routing via WorkDrive ...")
        share_url = upload_to_workdrive_and_get_link(file_bytes, filename)
        log.info(f"  Share link: {share_url}")
        send_report_link_email(start_date, end_date_inclusive, share_url, file_size_mb)
        return {"delivery": "workdrive_link", "share_url": share_url}


# =============================================================================
# Excel writer — builds the workbook entirely in memory
# =============================================================================

HEADER_FILL = PatternFill(start_color="0078D7", end_color="0078D7", fill_type="solid")
HEADER_FONT = Font(bold=True, color="FFFFFF")
CURRENCY_HINTS = ("Amount", "Revenue", "amnt", "Value")


def _style_sheet(ws, df: pd.DataFrame):
    for col_idx, col_name in enumerate(df.columns, start=1):
        cell = ws.cell(row=1, column=col_idx)
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT
        cell.alignment = Alignment(horizontal="center")

    for row_idx in range(2, len(df) + 2):
        for col_idx, col_name in enumerate(df.columns, start=1):
            cell = ws.cell(row=row_idx, column=col_idx)
            if any(hint in col_name for hint in CURRENCY_HINTS):
                cell.number_format = "#,##0.00"
            elif "Qty" in col_name or "qty" in col_name:
                cell.number_format = "#,##0"
            elif "margin" in col_name.lower():
                cell.number_format = "0.00"
            elif "%" in col_name:
                cell.number_format = "0.00"

    for col_idx, col_name in enumerate(df.columns, start=1):
        max_len = max(
            len(str(col_name)),
            df[col_name].astype(str).map(len).max() if len(df) else 0,
        )
        ws.column_dimensions[get_column_letter(col_idx)].width = min(max_len + 4, 40)

    ws.freeze_panes = "A2"


def write_workbook_bytes(
    product_sheet: pd.DataFrame, brand_group_sheet: pd.DataFrame, mondelez_sheet: pd.DataFrame,
) -> bytes:
    """Builds the three-sheet workbook entirely in memory and returns its
    raw bytes — nothing is ever written to local disk."""
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        product_sheet.to_excel(writer, sheet_name="Product Wise", index=False)
        brand_group_sheet.to_excel(writer, sheet_name="Brand Group", index=False)
        mondelez_sheet.to_excel(writer, sheet_name="Mondelez Product Wise", index=False)

        _style_sheet(writer.sheets["Product Wise"], product_sheet)
        _style_sheet(writer.sheets["Brand Group"], brand_group_sheet)
        _style_sheet(writer.sheets["Mondelez Product Wise"], mondelez_sheet)

    file_bytes = buffer.getvalue()
    log.info(f"Workbook built in memory ({len(file_bytes) / (1024*1024):.1f} MB) — not saved to disk.")
    return file_bytes


# =============================================================================
# Full pipeline — used by both this script's __main__ (default rolling
# 6-month window) and the Streamlit portal (user-picked window)
# =============================================================================

def run_report(start_date: date, end_exclusive: date, end_date_inclusive: date) -> dict:
    """Runs the full pipeline for the given window: selected brands, both
    queries, stock download + merge, in-memory workbook build, email
    delivery. Returns a small status dict — there is no output file path,
    since the workbook is never saved locally. Raises on any unrecoverable
    failure — callers (including the Streamlit portal) should catch and
    surface that."""
    log.info(f"Report window: {start_date.isoformat()} to {end_date_inclusive.isoformat()} (inclusive)")

    log.info("Loading selected brand list (brand_portal_selected_brands)...")
    selected_brands = get_selected_brands()
    log.info(f"  {len(selected_brands)} brands selected")

    log.info("Loading brand_sub mapping (sub_brand -> brand_group)...")
    brand_sub_df = get_brand_sub_mapping()
    log.info(f"  {len(brand_sub_df)} sub_brand -> brand_group mappings")

    log.info("Loading brand_group_meta (Start Date / Legal Name / TOT Validity / Off Invoice Margin %)...")
    brand_group_meta_df = get_brand_group_meta()
    log.info(f"  {len(brand_group_meta_df)} brand_group_meta rows")

    log.info("Running product-wise query (purchase vs sales, FULL OUTER JOIN)...")
    product_df = get_product_wise(start_date, end_exclusive, selected_brands)
    log.info(f"  {len(product_df)} product rows")

    log.info("Running brand-group query (purchase vs sales, summed over the whole window)...")
    brand_group_df = get_brand_group_summary(start_date, end_exclusive, selected_brands)
    log.info(f"  {len(brand_group_df)} brand-group rows")

    log.info("Downloading current stock snapshot for all stores (store_contacts -> store_product_snapshot)...")
    try:
        download_current_stock()
    except RuntimeError as e:
        if stock_snapshot_row_count() > 0:
            log.warning(
                f"Stock download had per-store failures, but store_product_snapshot was "
                f"still reloaded with the stores that succeeded — continuing: {e}"
            )
        else:
            log.error(f"Stock download failed and store_product_snapshot is empty: {e}")
            raise

    log.info("Loading stock snapshot from store_product_snapshot ...")
    stock_df = load_stock()
    stock_by_barcode = aggregate_stock_by_barcode(stock_df)
    stock_by_brand_group = aggregate_stock_by_brand_group(stock_df, brand_sub_df)
    log.info(f"  Stock loaded: {len(stock_df)} rows -> {stock_by_brand_group['brand_group'].nunique()} brand groups")

    log.info("Merging stock into product-level data and building sheets...")
    merged_base = _merge_product_wise_base(product_df, stock_by_barcode, brand_sub_df)
    product_sheet = build_product_wise_sheet(merged_base)
    mondelez_sheet = build_mondelez_product_sheet(merged_base)
    brand_group_sheet = build_brand_group_sheet(brand_group_df, stock_by_brand_group, brand_group_meta_df)

    log.info(
        f"Done. Product Wise: {len(product_sheet)} rows | Brand Group: {len(brand_group_sheet)} rows "
        f"| Mondelez Product Wise: {len(mondelez_sheet)} rows"
    )

    filename = f"Purchase_Sales_Stock_Report_{start_date:%Y-%m-%d}_to_{end_date_inclusive:%Y-%m-%d}.xlsx"
    file_bytes = write_workbook_bytes(product_sheet, brand_group_sheet, mondelez_sheet)
    file_size_mb = len(file_bytes) / (1024 * 1024)
    log.info(f"Report size: {file_size_mb:.1f} MB")

    log.info("Delivering report by email (in-memory attachment or WorkDrive link) ...")
    try:
        delivery_result = deliver_report(start_date, end_date_inclusive, file_bytes, filename, file_size_mb)
    except Exception as e:
        log.error(f"Report generated successfully but delivery (email/WorkDrive) failed: {e}")
        raise

    return {
        "filename": filename,
        "size_mb": round(file_size_mb, 2),
        "rows": {
            "product_wise": len(product_sheet),
            "brand_group": len(brand_group_sheet),
            "mondelez_product_wise": len(mondelez_sheet),
        },
        **delivery_result,
    }


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    start_date, end_exclusive, end_date_inclusive = get_last_6_months_window()
    run_report(start_date, end_exclusive, end_date_inclusive)