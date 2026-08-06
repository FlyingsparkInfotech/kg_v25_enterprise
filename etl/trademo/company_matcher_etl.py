"""
Trademo Company Matcher ETL
API → raw.trademo_company_matcher (PostgreSQL)

Resolves a free-text company name to a Trademo company ID + profile.
Used to link GoGlo CRM contacts/companies to Trademo trade data.

Usage:
  python3 trademo/company_matcher_etl.py \
      --companyName "Infosys Limited" --companyCountry india --limit 5

  # Batch mode from file (one company name per line):
  python3 trademo/company_matcher_etl.py --batchFile companies.txt
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import requests
import argparse
import time
import logging
import json
import psycopg2.extras
from datetime import datetime
from db import get_conn, upsert_watermark
from config import TRADEMO

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')
log = logging.getLogger(__name__)

FEED         = "trademo_company_matcher"
MATCHER_URL  = "https://trademo.com/api/v1/company_matcher_api"


def get_session_token():
    resp = requests.get(
        TRADEMO["token_url"],
        headers={"Auth-Custom-Header": TRADEMO["auth_header"]},
        timeout=30,
    )
    resp.raise_for_status()
    token = resp.json().get("token")
    if not token:
        raise Exception("No token returned from Trademo auth")
    log.info(f"Trademo token acquired: {token[:20]}...")
    return token


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--companyName",    default=None)
    p.add_argument("--companyCountry", default=None)
    p.add_argument("--limit",          type=int, default=10,
                   help="probableMatchesLimit")
    p.add_argument("--batchFile",      default=None,
                   help="File with lines: companyName[|country]")
    return p.parse_args()


def match_company(session_id, name, country=None, limit=10):
    payload = {
        "companyName":          name,
        "probableMatchesLimit": limit,
    }
    if country:
        payload["companyCountry"] = country.lower()

    resp = requests.post(
        MATCHER_URL,
        headers={
            "content-type":       "application/json",
            "sessionid":          session_id,
            "Auth-Custom-Header": TRADEMO["auth_header"],
        },
        data=json.dumps(payload),
        timeout=60,
    )
    if resp.status_code == 400:
        log.warning(f"400 for '{name}' — no matches")
        return []
    resp.raise_for_status()
    result = resp.json()
    # API returns list directly or wrapped
    if isinstance(result, list):
        return result
    return result.get("companies", result.get("data", []))


def upsert_matches(conn, query_name, matches):
    now = datetime.utcnow()
    inserted = 0
    with conn.cursor() as cur:
        for m in matches:
            cur.execute("""
                INSERT INTO raw.trademo_company_matcher (
                    company_id, company_name, country,
                    name_match_percentage, total_shipment_count,
                    company_address,
                    created_on, created_by, modified_on, modified_by
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (
                str(m.get("companyId", "") or m.get("company_id", "")),
                str(m.get("companyName", "") or m.get("company_name", "")),
                str(m.get("country", "")),
                float(m.get("nameMatchPercentage", 0) or m.get("name_match_percentage", 0) or 0),
                int(m.get("totalShipmentCount", 0) or m.get("total_shipment_count", 0) or 0),
                str(m.get("companyAddress", "") or m.get("address", "")),
                now, f"matcher:{query_name[:50]}",
                now, f"matcher:{query_name[:50]}",
            ))
            inserted += 1
    conn.commit()
    return inserted


def main():
    args  = parse_args()
    token = get_session_token()
    conn  = get_conn()
    total = 0

    # Build company list
    companies = []
    if args.batchFile:
        with open(args.batchFile) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                if "|" in line:
                    name, country = line.split("|", 1)
                    companies.append((name.strip(), country.strip()))
                else:
                    companies.append((line, args.companyCountry))
    elif args.companyName:
        companies = [(args.companyName, args.companyCountry)]
    else:
        log.error("Provide --companyName OR --batchFile")
        sys.exit(1)

    log.info(f"Matching {len(companies)} company name(s)")

    try:
        for name, country in companies:
            try:
                matches = match_company(token, name, country, args.limit)
                n = upsert_matches(conn, name, matches)
                log.info(f"  '{name}' → {len(matches)} matches, {n} inserted")
                total += n
            except Exception as e:
                log.error(f"  Error matching '{name}': {e}")
            time.sleep(0.3)

        upsert_watermark(conn, FEED, "success", total)
        log.info(f"Done — {total} company matches stored")
    except Exception as e:
        upsert_watermark(conn, FEED, "error", total, str(e))
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
