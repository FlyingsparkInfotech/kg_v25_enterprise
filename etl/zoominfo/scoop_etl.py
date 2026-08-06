"""
ZoomInfo Scoop Search ETL
API → zoominfo.scoop_search (PostgreSQL)

Scoops are business events: leadership changes, new offices, funding rounds,
partnership announcements, technology stack changes. High-value triggers for:
  - Scenario AA (Champion Mover — job change)
  - Scenario DD (Technographic Shift)
  - Scenario HH (Channel Partner change)
  - Seasonal buying window detection

Usage:
  python3 zoominfo/scoop_etl.py \
      --publishedStartDate 2026-06-01 --publishedEndDate 2026-08-01 \
      --scoopType "leadership,expansion" \
      --industryKeywords "manufacturing"

  # Champion Mover: job changes
  python3 zoominfo/scoop_etl.py \
      --publishedStartDate 2026-06-01 --publishedEndDate 2026-08-01 \
      --scoopType "leadership"
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import requests
import argparse
import time
import logging
import math
import psycopg2.extras
from datetime import datetime
from db import get_conn, upsert_watermark
from config import ZOOMINFO

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')
log = logging.getLogger(__name__)

FEED = "zoominfo_scoop"


def get_token():
    resp = requests.post(
        ZOOMINFO["auth_url"],
        json={"username": ZOOMINFO["username"], "password": ZOOMINFO["password"]},
        headers={"Content-Type": "application/json"},
        timeout=30,
    )
    resp.raise_for_status()
    token = resp.json().get("jwt")
    if not token:
        raise Exception(f"No JWT token in response: {resp.text[:200]}")
    log.info("ZoomInfo token acquired")
    return token


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--publishedStartDate", required=True)
    p.add_argument("--publishedEndDate",   required=True)
    p.add_argument("--companyName",        default=None)
    p.add_argument("--industryKeywords",   default=None)
    p.add_argument("--primaryIndustriesOnly", action="store_true")
    p.add_argument("--scoopType",          default=None,
                   help="Comma-separated: awards,partnerships,earnings,leadership,expansion,funding")
    p.add_argument("--scoopTopic",         default=None,
                   help="Comma-separated: expansion,leadership,funding,technology")
    p.add_argument("--excludeDefunctCompanies", action="store_true", default=True)
    p.add_argument("--rpp",   type=int, default=25)
    p.add_argument("--sortBy",    default="publishedDate",
                   choices=["publishedDate", "scoopid"])
    p.add_argument("--sortOrder", default="desc", choices=["asc", "desc"])
    return p.parse_args()


def build_payload(args, page):
    p = {
        "publishedStartDate":      args.publishedStartDate,
        "publishedEndDate":        args.publishedEndDate,
        "excludeDefunctCompanies": args.excludeDefunctCompanies,
        "rpp":     args.rpp,
        "page":    page,
        "sortBy":  args.sortBy,
        "sortOrder": args.sortOrder,
    }
    if args.companyName:
        p["companyName"] = args.companyName
    if args.industryKeywords:
        p["industryKeywords"] = args.industryKeywords
        p["primaryIndustriesOnly"] = args.primaryIndustriesOnly
    if args.scoopType:
        p["scoopType"] = args.scoopType
    if args.scoopTopic:
        p["scoopTopic"] = args.scoopTopic
    return p


def fetch_page(token, payload):
    resp = requests.post(
        ZOOMINFO["scoop_url"],
        json=payload,
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()


def upsert_scoops(conn, records):
    now = datetime.utcnow()
    inserted = 0
    with conn.cursor() as cur:
        for s in records:
            types  = s.get("types", [])   or []
            topics = s.get("topics", [])  or []

            def _ids(lst):  return ",".join(str(x.get("id","")) for x in lst if isinstance(x, dict))
            def _names(lst):return ",".join(str(x.get("name","")) for x in lst if isinstance(x, dict))

            pub_date = None
            orig_date = None
            for field, var in [("publishedDate", "pub_date"), ("originalPublishedDate", "orig_date")]:
                raw = s.get(field)
                if raw:
                    try:
                        locals()[var]  # just to set up
                    except Exception:
                        pass
            try:
                pub_date  = datetime.strptime(str(s.get("publishedDate",""))[:19], "%Y-%m-%dT%H:%M:%S") if s.get("publishedDate") else None
            except Exception:
                pub_date = None
            try:
                orig_date = datetime.strptime(str(s.get("originalPublishedDate",""))[:19], "%Y-%m-%dT%H:%M:%S") if s.get("originalPublishedDate") else None
            except Exception:
                orig_date = None

            cur.execute("""
                INSERT INTO zoominfo.scoop_search (
                    scoop_id, company_id, company_name,
                    type_ids, type_names, topic_ids, topic_names,
                    link, link_text, description, update_text,
                    published_date, original_published_date,
                    created_on, created_by, modified_on, modified_by
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
            """, (
                int(s.get("scoopId", 0) or 0),
                int(s.get("companyId", 0) or 0),
                str(s.get("companyName", "")),
                _ids(types),   _names(types),
                _ids(topics),  _names(topics),
                str(s.get("link", "") or ""),
                str(s.get("linkText", "") or ""),
                str(s.get("description", "") or s.get("scoopText", "") or ""),
                str(s.get("updateText", "") or ""),
                pub_date, orig_date,
                now, "etl_scoop", now, "etl_scoop",
            ))
            inserted += 1
    conn.commit()
    return inserted


def main():
    args  = parse_args()
    token = get_token()
    conn  = get_conn()
    total = 0

    log.info(f"Fetching scoops: {args.publishedStartDate} → {args.publishedEndDate}")

    try:
        # First page to get total
        payload = build_payload(args, 1)
        data    = fetch_page(token, payload)
        max_results = int(data.get("maxResults", 0) or 0)
        records     = data.get("data", [])
        total_pages = math.ceil(max_results / args.rpp) if max_results else 1

        log.info(f"  maxResults={max_results} pages={total_pages}")

        all_records = list(records)
        for page in range(2, total_pages + 1):
            time.sleep(0.5)
            d = fetch_page(token, build_payload(args, page))
            batch = d.get("data", [])
            if not batch:
                break
            all_records.extend(batch)

        total = upsert_scoops(conn, all_records)
        upsert_watermark(conn, FEED, "success", total)
        log.info(f"Done — {total} scoops stored from {len(all_records)} fetched")
    except Exception as e:
        upsert_watermark(conn, FEED, "error", total, str(e))
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
