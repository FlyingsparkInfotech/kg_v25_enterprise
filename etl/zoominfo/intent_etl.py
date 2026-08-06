"""
ZoomInfo Intent Data ETL
API → zoominfo.intent_data (PostgreSQL)

Usage:
  python3 zoominfo/intent_etl.py \
      --topics "Cloud Computing" "Cybersecurity" \
      --country India --rpp 25
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import requests
import argparse
import time
import logging
import math
import json
import psycopg2.extras
from db import get_conn, upsert_watermark
from config import ZOOMINFO

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')
log = logging.getLogger(__name__)

FEED = "zoominfo_intent"


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
        raise Exception(f"No JWT: {resp.text[:200]}")
    log.info("ZoomInfo token acquired")
    return token


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--topics",    nargs="*", default=[])
    p.add_argument("--country",   default=None)
    p.add_argument("--companyId", default=None)
    p.add_argument("--rpp",       type=int, default=25)
    return p.parse_args()


def build_payload(args, page):
    payload = {"rpp": args.rpp, "page": page}
    if args.topics:
        payload["topics"] = args.topics
    if args.country:
        payload["country"] = args.country
    if args.companyId:
        payload["companyId"] = args.companyId
    return payload


def fetch_page(token, payload):
    resp = requests.post(
        ZOOMINFO["intent_url"],
        json=payload,
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()


def upsert_intent(conn, records):
    # Writes to zoominfo.intent_search — the table the KG pipeline reads from
    # Columns: intent_key(serial PK), signal_id, company_id, company_name,
    #          company_website, has_other_topic_consumption, category,
    #          topic, signal_score, audience_strength, signal_date, spikes_in_date_range
    sql = """
        INSERT INTO zoominfo.intent_search (
            signal_id, company_id, company_name, company_website,
            category, topic, signal_score, audience_strength, signal_date
        ) VALUES %s
        ON CONFLICT DO NOTHING
    """
    from datetime import datetime
    now = datetime.utcnow()
    rows = []
    for r in records:
        for topic_obj in (r.get("topics") or [r]):
            topic_name  = topic_obj.get("topic")  if isinstance(topic_obj, dict) else topic_obj
            topic_score = topic_obj.get("score")  if isinstance(topic_obj, dict) else r.get("score")
            rows.append((
                r.get("signalId") or r.get("signal_id"),
                r.get("companyId"),
                r.get("companyName"),
                r.get("companyWebsite"),
                r.get("category"),
                topic_name,
                topic_score,
                r.get("audienceStrength"),
                r.get("signalDate") or now,
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
        token = get_token()
        page  = 1
        total = 0

        first = fetch_page(token, build_payload(args, 1))
        max_results = first.get("maxResults", 0)
        total_pages = math.ceil(max_results / args.rpp) if max_results else 1
        log.info(f"Total available: {max_results} | Pages: {total_pages}")

        records = first.get("data", [])
        if records:
            n = upsert_intent(conn, records)
            total += n
            log.info(f"  Page 1: {n} intent records")

        for page in range(2, total_pages + 1):
            time.sleep(0.5)
            resp = fetch_page(token, build_payload(args, page))
            records = resp.get("data", [])
            if not records:
                break
            n = upsert_intent(conn, records)
            total += n
            log.info(f"  Page {page}: {n} records (total: {total})")

        upsert_watermark(conn, FEED, "success", total)
        log.info(f"Completed. Total intent records: {total}")

    except Exception as e:
        log.exception("ZoomInfo intent ETL failed")
        upsert_watermark(conn, FEED, "failed", 0, str(e))
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
