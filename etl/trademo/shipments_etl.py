"""
Trademo Shipment Search ETL
API → trademo.shipments (PostgreSQL)

Usage:
  python3 trademo/shipments_etl.py --fromDate 2024-01-01 --toDate 2026-07-19 \
      --shipperName "Acme Corp" --hsCodes 7202

Run from Mac with SSH tunnel open:
  ssh -N -L 5433:localhost:5432 powercozmo@100.93.186.72
  PG_PORT=5433 python3 trademo/shipments_etl.py ...
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import requests
import json
import argparse
import time
import logging
import psycopg2.extras
from datetime import datetime
from db import get_conn, upsert_watermark
from config import TRADEMO

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')
log = logging.getLogger(__name__)

FEED = "trademo_shipments"


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
    p.add_argument("--fromDate",            required=True)
    p.add_argument("--toDate",              required=True)
    p.add_argument("--shipperName",         nargs="*", default=[])
    p.add_argument("--shipperId",           nargs="*", default=[])
    p.add_argument("--consigneeName",       nargs="*", default=[])
    p.add_argument("--consigneeId",         nargs="*", default=[])
    p.add_argument("--shipperCountryName",  nargs="*", default=[])
    p.add_argument("--consigneeCountryName",nargs="*", default=[])
    p.add_argument("--portOfLading",        nargs="*", default=[])
    p.add_argument("--portOfUnlading",      nargs="*", default=[])
    p.add_argument("--productKeywords",     nargs="*", default=[])
    p.add_argument("--hsCodes",             nargs="*", default=[])
    p.add_argument("--sortField",     default="shipmentDate")
    p.add_argument("--sortDirection", default="desc", choices=["asc", "desc"])
    return p.parse_args()


def fetch_page(session_id, payload):
    resp = requests.post(
        TRADEMO["shipment_url"],
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


def safe_date(val):
    if not val:
        return None
    try:
        return datetime.strptime(val[:10], "%Y-%m-%d").date()
    except Exception:
        return None


def upsert_shipments(conn, shipments):
    sql = """
        INSERT INTO trademo.shipments (
            shipper_name, shipper_id, consignee_name, consignee_id,
            shipper_country, consignee_country,
            port_of_lading, port_of_unlading,
            hs_code, product_description,
            shipment_date, shipment_value, weight, quantity, unit,
            bill_of_lading, raw_json
        ) VALUES %s
    """
    rows = []
    for s in shipments:
        rows.append((
            s.get("shipperName"),
            s.get("shipperId"),
            s.get("consigneeName"),
            s.get("consigneeId"),
            s.get("shipperCountryName") or s.get("shipperCountry"),
            s.get("consigneeCountryName") or s.get("consigneeCountry"),
            s.get("portOfLading"),
            s.get("portOfUnlading"),
            s.get("hsCode") or (s.get("hsCodes") or [""])[0],
            s.get("productDescription"),
            safe_date(s.get("shipmentDate")),
            s.get("shipmentValue"),
            s.get("weight"),
            s.get("quantity"),
            s.get("unit"),
            s.get("billOfLading"),
            json.dumps(s),
        ))

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows)
    conn.commit()
    return len(rows)


def main():
    args = parse_args()
    conn = get_conn()
    upsert_watermark(conn, FEED, "running")

    try:
        token = get_session_token()
        page  = 1
        total = 0

        while True:
            payload = {
                "shipperName"          : args.shipperName,
                "shipperId"            : args.shipperId,
                "consigneeName"        : args.consigneeName,
                "consigneeId"          : args.consigneeId,
                "shipperCountryName"   : args.shipperCountryName,
                "consigneeCountryName" : args.consigneeCountryName,
                "portOfLading"         : args.portOfLading,
                "portOfUnlading"       : args.portOfUnlading,
                "productKeywords"      : args.productKeywords,
                "hsCodes"              : args.hsCodes,
                "pageNumber"           : page,
                "sort"                 : {"field": args.sortField, "direction": args.sortDirection},
                "tradeTimePeriod"      : {"fromDate": args.fromDate, "toDate": args.toDate},
            }

            log.info(f"Fetching page {page}...")
            data = fetch_page(token, payload)

            if data is None:
                log.info(f"API 400 — end of pages at page {page}")
                break

            shipments = data.get("shipments", [])
            if not shipments:
                log.info(f"No shipments on page {page} — stopping")
                break

            n = upsert_shipments(conn, shipments)
            total += n
            log.info(f"  Page {page}: {n} shipments inserted (total: {total})")

            page += 1
            time.sleep(0.3)

        upsert_watermark(conn, FEED, "success", total)
        log.info(f"Completed. Total shipments: {total}")

    except Exception as e:
        log.exception("Shipments ETL failed")
        upsert_watermark(conn, FEED, "failed", 0, str(e))
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
