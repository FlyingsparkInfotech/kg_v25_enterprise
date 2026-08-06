"""
ZoomInfo News Search ETL
API → zoominfo.news_search (PostgreSQL)

Fetches news articles tagged by business event category. Used for:
  - Scenario CC (M&A Consolidation — existing customer acquires target)
  - Scenario HH (Channel Partner / Reseller Conflict)
  - Scenario u  (Existing Buyer, Adjacent Need — expansion signals)

Categories available:
  MERGER_OR_ACQUISITION, LEADERSHIP_CHANGE, NEW_OFFICE_OR_EXPANSION,
  PARTNERSHIP_OR_ALLIANCE, FUNDING_OR_INVESTMENT, PRODUCT_LAUNCH,
  BANKRUPTCY_OR_RESTRUCTURING, IPO, AWARD_OR_RECOGNITION

Usage:
  python3 zoominfo/news_etl.py \
      --pageDateMin 2026-07-01 --pageDateMax 2026-08-01 \
      --categories MERGER_OR_ACQUISITION LEADERSHIP_CHANGE

  # All high-value categories
  python3 zoominfo/news_etl.py \
      --pageDateMin 2026-07-01 --pageDateMax 2026-08-01 \
      --categories MERGER_OR_ACQUISITION LEADERSHIP_CHANGE NEW_OFFICE_OR_EXPANSION \
                   PARTNERSHIP_OR_ALLIANCE FUNDING_OR_INVESTMENT
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

FEED = "zoominfo_news"

VALID_CATEGORIES = [
    "MERGER_OR_ACQUISITION", "LEADERSHIP_CHANGE", "NEW_OFFICE_OR_EXPANSION",
    "PARTNERSHIP_OR_ALLIANCE", "FUNDING_OR_INVESTMENT", "PRODUCT_LAUNCH",
    "BANKRUPTCY_OR_RESTRUCTURING", "IPO", "AWARD_OR_RECOGNITION",
]


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
        raise Exception(f"No JWT token: {resp.text[:200]}")
    log.info("ZoomInfo token acquired")
    return token


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--pageDateMin",  required=True, help="YYYY-MM-DD")
    p.add_argument("--pageDateMax",  required=True, help="YYYY-MM-DD")
    p.add_argument("--categories",   nargs="*", default=["MERGER_OR_ACQUISITION"],
                   choices=VALID_CATEGORIES)
    p.add_argument("--urlKeywords",  nargs="*", default=[])
    p.add_argument("--rpp",          type=int,  default=20, help="Max 20 per API docs")
    p.add_argument("--sortBy",       default="pageDate")
    p.add_argument("--sortOrder",    default="desc")
    return p.parse_args()


def build_payload(args, page):
    p = {
        "categories":   args.categories,
        "pageDateMin":  args.pageDateMin,
        "pageDateMax":  args.pageDateMax,
        "rpp":          min(args.rpp, 20),  # API max is 20
        "page":         page,
        "sortBy":       args.sortBy,
        "sortOrder":    args.sortOrder,
    }
    if args.urlKeywords:
        p["url"] = args.urlKeywords
    return p


def fetch_page(token, payload):
    resp = requests.post(
        ZOOMINFO["news_url"],
        json=payload,
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()


def upsert_news(conn, records):
    now = datetime.utcnow()
    inserted = 0
    with conn.cursor() as cur:
        for n in records:
            page_date = None
            try:
                raw_date = n.get("pageDate") or n.get("publishedDate") or ""
                if raw_date:
                    page_date = datetime.strptime(str(raw_date)[:10], "%Y-%m-%d")
            except Exception:
                pass

            cats = n.get("categories", [])
            cats_str = ",".join(cats) if isinstance(cats, list) else str(cats or "")

            cur.execute("""
                INSERT INTO zoominfo.news_search (
                    company_id, company_name, domain, url, image_url,
                    title, description, categories, page_date,
                    created_on, created_by, modified_on, modified_by
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
            """, (
                int(n.get("companyId", 0) or 0),
                str(n.get("companyName", "") or ""),
                str(n.get("domain", "") or n.get("website", "") or ""),
                str(n.get("url", "") or n.get("articleUrl", "") or ""),
                str(n.get("imageUrl", "") or ""),
                str(n.get("title", "") or n.get("articleTitle", "") or ""),
                str(n.get("description", "") or n.get("summary", "") or ""),
                cats_str,
                page_date,
                now, "etl_news", now, "etl_news",
            ))
            inserted += 1
    conn.commit()
    return inserted


def main():
    args  = parse_args()
    token = get_token()
    conn  = get_conn()
    total = 0

    log.info(f"Fetching news: {args.pageDateMin} → {args.pageDateMax} categories={args.categories}")

    try:
        payload     = build_payload(args, 1)
        data        = fetch_page(token, payload)
        max_results = int(data.get("maxResults", 0) or 0)
        records     = data.get("data", [])
        total_pages = math.ceil(max_results / min(args.rpp, 20)) if max_results else 1

        log.info(f"  maxResults={max_results} pages={total_pages}")

        all_records = list(records)
        for page in range(2, total_pages + 1):
            time.sleep(0.5)
            d = fetch_page(token, build_payload(args, page))
            batch = d.get("data", [])
            if not batch:
                break
            all_records.extend(batch)

        total = upsert_news(conn, all_records)
        upsert_watermark(conn, FEED, "success", total)
        log.info(f"Done — {total} news articles stored from {len(all_records)} fetched")
    except Exception as e:
        upsert_watermark(conn, FEED, "error", total, str(e))
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
