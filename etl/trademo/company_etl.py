"""
Trademo Company Profile ETL
API → trademo.company_profiles (PostgreSQL)

Usage:
  python3 trademo/company_etl.py \
      --companyIds 6077ad0a507f71071df7930e abc123 \
      --fromDate 2022-01-01 --toDate 2026-07-19

Or from a file of IDs (one per line):
  python3 trademo/company_etl.py --companyIdsFile company_ids.txt \
      --fromDate 2022-01-01 --toDate 2026-07-19
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

FEED = "trademo_company_profiles"


def get_session_token():
    resp = requests.get(
        TRADEMO["token_url"],
        headers={"Auth-Custom-Header": TRADEMO["auth_header"]},
        timeout=30
    )
    resp.raise_for_status()
    token = resp.json().get("token")
    log.info(f"Trademo token acquired")
    return token


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--companyIds",     nargs="*", default=[], help="Trademo company IDs")
    p.add_argument("--companyIdsFile", default=None, help="File with company IDs, one per line")
    p.add_argument("--fromDate",       required=True)
    p.add_argument("--toDate",         required=True)
    p.add_argument("--partnerLimit",   type=int, default=10)
    return p.parse_args()


def fetch_company(session_id, company_id, from_date, to_date, partner_limit):
    payload = {
        "companyID"                    : company_id,
        "tradingPartnerLimit"          : partner_limit,
        "tradeTimePeriod"              : {"fromDate": from_date, "toDate": to_date},
        "includeSubsidiaryCompanies"   : False,
        "tradingPartnerTradeVolumeUnit": "value",
    }
    resp = requests.post(
        TRADEMO["company_url"],
        headers={
            "Content-Type"      : "application/json",
            "sessionid"         : session_id,
            "Auth-Custom-Header": TRADEMO["auth_header"],
        },
        data=json.dumps(payload),
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()


def upsert_profile(conn, data, from_date, to_date):
    hs_codes = ",".join(data.get("primaryHsCodes") or [])
    top_partners = json.dumps(data.get("topTradingPartners") or [])

    sql = """
        INSERT INTO trademo.company_profiles (
            company_id, company_name, country, address,
            total_shipments, total_trading_partners,
            primary_hs_codes, top_partners_json,
            from_date, to_date, raw_json
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (company_id) DO UPDATE SET
            company_name            = EXCLUDED.company_name,
            total_shipments         = EXCLUDED.total_shipments,
            total_trading_partners  = EXCLUDED.total_trading_partners,
            primary_hs_codes        = EXCLUDED.primary_hs_codes,
            top_partners_json       = EXCLUDED.top_partners_json,
            from_date               = EXCLUDED.from_date,
            to_date                 = EXCLUDED.to_date,
            raw_json                = EXCLUDED.raw_json,
            ingested_at             = NOW()
    """
    with conn.cursor() as cur:
        cur.execute(sql, (
            data.get("companyID"),
            data.get("companyName"),
            data.get("country"),
            data.get("address"),
            data.get("totalShipments"),
            data.get("totalTradingPartners"),
            hs_codes,
            top_partners,
            from_date, to_date,
            json.dumps(data),
        ))
    conn.commit()


def main():
    args = parse_args()

    company_ids = list(args.companyIds)
    if args.companyIdsFile:
        with open(args.companyIdsFile) as f:
            company_ids += [line.strip() for line in f if line.strip()]

    if not company_ids:
        log.error("Provide --companyIds or --companyIdsFile")
        sys.exit(1)

    conn = get_conn()
    upsert_watermark(conn, FEED, "running")

    try:
        token = get_session_token()
        total = 0

        for cid in company_ids:
            log.info(f"Fetching profile: {cid}")
            try:
                data = fetch_company(token, cid, args.fromDate, args.toDate, args.partnerLimit)
                if data.get("companyID"):
                    upsert_profile(conn, data, args.fromDate, args.toDate)
                    total += 1
                    log.info(f"  Upserted: {data.get('companyName')}")
                else:
                    log.warning(f"  No data for {cid}")
            except Exception as e:
                log.warning(f"  Error for {cid}: {e}")
            time.sleep(0.5)

        upsert_watermark(conn, FEED, "success", total)
        log.info(f"Completed. Total profiles: {total}")

    except Exception as e:
        log.exception("Company ETL failed")
        upsert_watermark(conn, FEED, "failed", 0, str(e))
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
