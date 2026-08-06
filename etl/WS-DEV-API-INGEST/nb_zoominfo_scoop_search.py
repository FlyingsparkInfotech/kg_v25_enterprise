import os
#!/usr/bin/env python3
# --------------------------------------------------------------------------------------
# ZoomInfo Scoop Search — API INGEST
#
# Usage (champion mover / leadership):
#   python3 nb_zoominfo_scoop_search.py \
#     --publishedStartDate 2025-01-01 \
#     --publishedEndDate   2026-07-25 \
#     --scoopType "awards,partnerships,expansion,leadership" \
#     --sortBy publishedDate --sortOrder desc --rpp 25
#
#   Industry mode:
#     python3 nb_zoominfo_scoop_search.py \
#       --publishedStartDate 2025-01-01 --publishedEndDate 2026-07-25 \
#       --industryKeywords manufacturing --primaryIndustriesOnly \
#       --excludeDefunctCompanies --rpp 25
#
# Saves to s3a://goglo-bronze-layer/zoominfo/scoop_search/
# Chains to WS-DEV-RAW-DATA/nb_zoominfo_scoop_search_raw.py
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

ETL_BASE = "/opt/.debug/kg_v25_enterprise/etl"
BUCKET   = "goglo-bronze-layer"
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
        raise RuntimeError(f"No JWT in response: {r.json()}")
    logger.info("ZoomInfo token obtained")
    return token


def build_payload(args, page):
    payload = {
        "publishedStartDate"     : args.publishedStartDate,
        "publishedEndDate"       : args.publishedEndDate,
        "updatedSinceCreation"   : args.updatedSinceCreation,
        "excludeDefunctCompanies": args.excludeDefunctCompanies,
        "rpp"                    : args.rpp,
        "page"                   : page,
        "sortBy"                 : args.sortBy,
        "sortOrder"              : args.sortOrder
    }
    if args.companyName:
        payload["companyName"] = args.companyName
    if args.industryKeywords:
        payload["industryKeywords"]      = args.industryKeywords
        payload["primaryIndustriesOnly"] = args.primaryIndustriesOnly
    if args.scoopType:
        payload["scoopType"] = args.scoopType
    if args.scoopTopic:
        payload["scoopTopic"] = args.scoopTopic
    return payload


def fetch_all_pages(args, token):
    all_records = []
    page = 1
    first = requests.post(
        "https://api.zoominfo.com/search/scoop",
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
        json=build_payload(args, page)
    )
    first.raise_for_status()
    data = first.json()
    max_results = data.get("maxResults", 0)
    total_pages = math.ceil(max_results / args.rpp) if max_results else 1
    logger.info(f"Total available: {max_results}, pages: {total_pages}")
    all_records.extend(data.get("data", []))

    for page in range(2, total_pages + 1):
        time.sleep(0.5)
        r = requests.post(
            "https://api.zoominfo.com/search/scoop",
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
            json=build_payload(args, page)
        )
        r.raise_for_status()
        records = r.json().get("data", [])
        if not records:
            break
        all_records.extend(records)
        logger.info(f"Page {page}: +{len(records)} (total: {len(all_records)})")

    return all_records, max_results


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--publishedStartDate", required=True)
    parser.add_argument("--publishedEndDate",   required=True)
    parser.add_argument("--companyName",        default=None)
    parser.add_argument("--industryKeywords",   default=None)
    parser.add_argument("--primaryIndustriesOnly", action="store_true", default=False)
    parser.add_argument("--scoopType",          default=None)
    parser.add_argument("--scoopTopic",         default=None)
    parser.add_argument("--updatedSinceCreation",    action="store_true", default=False)
    parser.add_argument("--excludeDefunctCompanies", action="store_true", default=False)
    parser.add_argument("--rpp",       type=int, default=25)
    parser.add_argument("--sortBy",    default="publishedDate")
    parser.add_argument("--sortOrder", default="desc", choices=["asc", "desc"])
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
    s3_key = (f"zoominfo/scoop_search/"
              f"{args.publishedStartDate}_to_{args.publishedEndDate}_{ts}.json")

    envelope = {
        "ingested_at"      : datetime.utcnow().isoformat() + "Z",
        "publishedStartDate": args.publishedStartDate,
        "publishedEndDate"  : args.publishedEndDate,
        "filters": {
            "companyName"           : args.companyName,
            "industryKeywords"      : args.industryKeywords,
            "primaryIndustriesOnly" : args.primaryIndustriesOnly,
            "scoopType"             : args.scoopType,
            "scoopTopic"            : args.scoopTopic,
            "updatedSinceCreation"  : args.updatedSinceCreation,
            "excludeDefunctCompanies": args.excludeDefunctCompanies
        },
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
        ["python3", f"{ETL_BASE}/WS-DEV-RAW-DATA/nb_zoominfo_scoop_search_raw.py"],
        check=True
    )
    logger.info("RAW processing completed.")


if __name__ == "__main__":
    main()
