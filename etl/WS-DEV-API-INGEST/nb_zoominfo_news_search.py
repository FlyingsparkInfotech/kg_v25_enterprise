import os
#!/usr/bin/env python3
# --------------------------------------------------------------------------------------
# ZoomInfo News Search — API INGEST
#
# Usage:
#   python3 nb_zoominfo_news_search.py \
#     --categories MERGER_OR_ACQUISITION LEADERSHIP_CHANGE NEW_OFFICE_OR_EXPANSION \
#     --pageDateMin 2025-01-01 \
#     --pageDateMax 2026-07-25 \
#     --rpp 20
#
# Available categories:
#   MERGER_OR_ACQUISITION, LEADERSHIP_CHANGE, NEW_OFFICE_OR_EXPANSION,
#   NEW_PARTNERSHIP_OR_CUSTOMER, NEW_PRODUCT_OR_SERVICE, REVENUE_OR_BOOKING,
#   AWARD_OR_RECOGNITION, INVESTMENT_OR_FUNDING, TECHNOLOGY_IMPLEMENTATION
#
# Saves to s3a://goglo-bronze-layer/zoominfo/news_search/
# Chains to WS-DEV-RAW-DATA/nb_zoominfo_news_search_raw.py
# --------------------------------------------------------------------------------------

import requests
import json
import argparse
import boto3
import time
import math
import subprocess
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s] %(levelname)s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

ETL_BASE   = "/opt/.debug/kg_v25_enterprise/etl"
BUCKET     = "goglo-bronze-layer"
AWS_KEY    = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
ZI_USER    = "sriram@powercozmo.com"
ZI_PASS    = "wewfox-3vanve-fecwuZ"


def get_access_token():
    r = requests.post("https://api.zoominfo.com/authenticate",
                      headers={"Content-Type": "application/json"},
                      json={"username": ZI_USER, "password": ZI_PASS})
    r.raise_for_status()
    token = r.json().get("jwt")
    if not token:
        raise RuntimeError(f"No JWT: {r.json()}")
    logger.info("ZoomInfo token obtained")
    return token


def fetch_all_pages(args, token):
    all_records = []
    page = 1
    while True:
        payload = {
            "categories"  : args.categories,
            "pageDateMin" : args.pageDateMin,
            "pageDateMax" : args.pageDateMax,
            "page"        : page,
            "rpp"         : args.rpp
        }
        if args.urlKeywords:
            payload["url"] = args.urlKeywords

        r = requests.post(
            "https://api.zoominfo.com/search/news",
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
            json=payload
        )
        r.raise_for_status()
        data = r.json()

        if page == 1:
            max_results = data.get("maxResults", 0)
            total_pages = math.ceil(max_results / args.rpp) if max_results else 1
            logger.info(f"Total available: {max_results}, pages: {total_pages}")

        records = data.get("data", [])
        if not records:
            break
        all_records.extend(records)
        logger.info(f"Page {page}: +{len(records)} (total: {len(all_records)})")

        if page >= total_pages:
            break
        page += 1
        time.sleep(0.5)

    return all_records, max_results


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--categories",   nargs="+", default=["MERGER_OR_ACQUISITION"])
    parser.add_argument("--urlKeywords",  nargs="+", default=[])
    parser.add_argument("--pageDateMin",  required=True)
    parser.add_argument("--pageDateMax",  required=True)
    parser.add_argument("--rpp",          type=int, default=20)
    return parser.parse_args()


def main():
    args  = parse_args()
    token = get_access_token()
    s3    = boto3.client("s3", aws_access_key_id=AWS_KEY,
                          aws_secret_access_key=AWS_SECRET, region_name="ap-south-1")

    all_records, max_results = fetch_all_pages(args, token)

    if not all_records:
        logger.warning("No records found — nothing saved")
        return

    ts     = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    s3_key = f"zoominfo/news_search/{args.pageDateMin}_to_{args.pageDateMax}_{ts}.json"

    envelope = {
        "ingested_at"    : datetime.utcnow().isoformat() + "Z",
        "pageDateMin"    : args.pageDateMin,
        "pageDateMax"    : args.pageDateMax,
        "categories"     : args.categories,
        "total_available": max_results,
        "total_fetched"  : len(all_records),
        "data"           : all_records
    }

    s3.put_object(Bucket=BUCKET, Key=s3_key,
                  Body=json.dumps(envelope, indent=2),
                  ContentType="application/json")
    logger.info(f"Saved {len(all_records)} records → s3://{BUCKET}/{s3_key}")

    logger.info("API ingest complete — starting RAW processing...")
    subprocess.run(
        ["python3", f"{ETL_BASE}/WS-DEV-RAW-DATA/nb_zoominfo_news_search_raw.py"],
        check=True
    )
    logger.info("RAW processing completed.")


if __name__ == "__main__":
    main()
