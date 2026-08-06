"""
Trademo Buyer/Supplier List ETL
API → trademo.buyer_supplier_list (PostgreSQL)

Usage:
  python3 trademo/bsl_etl.py --fromDate 2024-01-01 --toDate 2026-07-19 \
      --companyRole Supplier --hsCodes 7202 7201 --pageSize 50

Run from Mac with SSH tunnel open:
  ssh -N -L 5433:localhost:5432 powercozmo@100.93.186.72
  PG_PORT=5433 python3 trademo/bsl_etl.py ...
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import requests
import json
import argparse
import time
import logging
import psycopg2.extras
from db import get_conn, upsert_watermark
from config import TRADEMO

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')
log = logging.getLogger(__name__)

FEED = "trademo_bsl"


def get_session_token():
    resp = requests.get(
        TRADEMO["token_url"],
        headers={"Auth-Custom-Header": TRADEMO["auth_header"]},
        timeout=30
    )
    resp.raise_for_status()
    token = resp.json().get("token")
    log.info(f"Trademo token acquired: {token[:20]}...")
    return token


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--fromDate",    required=True)
    p.add_argument("--toDate",      required=True)
    p.add_argument("--companyRole", default="Supplier", choices=["Supplier", "Buyer"])
    p.add_argument("--hsCodes",             nargs="*", default=[])
    p.add_argument("--productKeywords",     nargs="*", default=[])
    p.add_argument("--companyCountryName",  nargs="*", default=[])
    p.add_argument("--pageSize",    type=int, default=50,
                        help="Must be multiple of 5 (API requirement)")
    return p.parse_args()


def fetch_page(session_id, payload):
    resp = requests.post(
        TRADEMO["bsl_url"],
        headers={
            "content-type"      : "application/json",
            "sessionid"         : session_id,
            "Auth-Custom-Header": TRADEMO["auth_header"],
        },
        data=json.dumps(payload),
        timeout=60,
    )
    if resp.status_code == 400:
        return None
    resp.raise_for_status()
    return resp.json()


def upsert_companies(conn, companies, role, from_date, to_date):
    sql = """
        INSERT INTO trademo.buyer_supplier_list (
            company_id, company_name, country, state, city, zip_code, address,
            number_of_shipments, shipment_value, trading_partner_count,
            matched_hs_codes, stock_tickers, matched_product_keyword,
            matched_countries_trading_with, company_role, from_date, to_date
        ) VALUES %s
        ON CONFLICT (company_id, company_role, from_date, to_date) DO UPDATE SET
            company_name                   = EXCLUDED.company_name,
            number_of_shipments            = EXCLUDED.number_of_shipments,
            shipment_value                 = EXCLUDED.shipment_value,
            trading_partner_count          = EXCLUDED.trading_partner_count,
            matched_hs_codes               = EXCLUDED.matched_hs_codes,
            matched_product_keyword        = EXCLUDED.matched_product_keyword,
            matched_countries_trading_with = EXCLUDED.matched_countries_trading_with
    """
    rows = []
    for c in companies:
        hs = ",".join(c.get("matchedHSCodes") or [])
        kw = ",".join(c.get("matchedProductKeyword") or [])
        countries = ",".join(c.get("matchedCountriesTradingWith") or [])
        tickers = json.dumps(c.get("stockTickers") or [])
        rows.append((
            c.get("companyId") or c.get("company_id"),
            c.get("companyName") or c.get("company_name"),
            c.get("country"),
            c.get("state"),
            c.get("city"),
            c.get("zipCode"),
            c.get("addressList") or c.get("address"),
            c.get("numberOfShipments") or c.get("number_of_shipments"),
            c.get("shipmentValue") or c.get("shipment_value"),
            c.get("tradingPartnerCount") or c.get("trading_partner_count"),
            hs, tickers, kw, countries,
            role, from_date, to_date,
        ))

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows)
    conn.commit()
    return len(rows)


def main():
    args = parse_args()

    if not args.hsCodes and not args.productKeywords:
        log.error("Provide --hsCodes or --productKeywords")
        sys.exit(1)
    if args.pageSize % 5 != 0:
        args.pageSize = (args.pageSize // 5) * 5 or 5
        log.warning(f"pageSize adjusted to {args.pageSize} (must be multiple of 5)")

    conn = get_conn()
    upsert_watermark(conn, FEED, "running")

    try:
        token = get_session_token()
        page  = 1
        total = 0

        while True:
            payload = {
                "companyRole"              : args.companyRole,
                "companyCountryName"       : args.companyCountryName,
                "hsCodes"                  : args.hsCodes,
                "productKeywords"          : args.productKeywords,
                "pageSize"                 : args.pageSize,
                "pageNumber"               : page,
                "tradeTimePeriod"          : {"fromDate": args.fromDate, "toDate": args.toDate},
            }

            log.info(f"Fetching page {page}...")
            data = fetch_page(token, payload)

            if data is None:
                log.info(f"API returned 400 — end of pages at {page}")
                break

            companies = data.get("companies", [])
            if not companies:
                log.info(f"No companies on page {page} — stopping")
                break

            n = upsert_companies(conn, companies, args.companyRole, args.fromDate, args.toDate)
            total += n
            log.info(f"  Page {page}: {n} companies upserted (total: {total})")

            page += 1
            time.sleep(0.3)

        upsert_watermark(conn, FEED, "success", total)
        log.info(f"Completed. Total BSL records: {total}")

    except Exception as e:
        log.exception("BSL ETL failed")
        upsert_watermark(conn, FEED, "failed", 0, str(e))
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
