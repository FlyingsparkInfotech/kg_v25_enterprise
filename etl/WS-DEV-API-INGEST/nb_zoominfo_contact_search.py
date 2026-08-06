import os
#!/usr/bin/env python3
# --------------------------------------------------------------------------------------
# ZoomInfo Contact Search — API INGEST
#
# Usage:
#   python3 nb_zoominfo_contact_search.py \
#     --companyName "Acme Corp" \
#     --country US --state California \
#     --jobFunction Engineering \
#     --managementLevel Director \
#     --employeeRangeMin 100 --employeeRangeMax 5000 \
#     --revenueMin 1000000 --revenueMax 500000000 \
#     --contactAccuracyScoreMin 85 \
#     --rpp 25 --sortBy lastUpdatedDate --sortOrder desc
#
# Saves envelope JSON to s3://goglo-bronze-layer/zoominfo/contact_search/{tag}_{ts}.json
# Chains to WS-DEV-RAW-DATA/nb_zoominfo_contact_search_raw.py
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
USERNAME   = os.environ.get("ZI_USERNAME", "sriram@powercozmo.com")
PASSWORD   = os.environ.get("ZI_PASSWORD", "wewfox-3vanve-fecwuZ")


def get_access_token():
    r = requests.post(
        "https://api.zoominfo.com/authenticate",
        headers={"Content-Type": "application/json"},
        json={"username": USERNAME, "password": PASSWORD}
    )
    r.raise_for_status()
    token = r.json().get("jwt")
    if not token:
        raise RuntimeError(f"No JWT in response: {r.json()}")
    logger.info("ZoomInfo token obtained")
    return token


def build_payload(args, page):
    payload = {"rpp": args.rpp, "page": page}

    if args.personId:
        payload["personId"] = args.personId
    if args.emailAddress:
        payload["emailAddress"] = args.emailAddress
    if args.jobTitle:
        payload["jobTitle"] = args.jobTitle
    if args.jobFunction:
        payload["jobFunction"] = args.jobFunction
    if args.department:
        payload["department"] = args.department
    if args.managementLevel:
        payload["managementLevel"] = args.managementLevel
    if args.companyId:
        payload["companyId"] = args.companyId
    if args.companyName:
        payload["companyName"] = args.companyName
    if args.country:
        payload["country"] = args.country
    if args.state:
        payload["state"] = args.state
    if args.industryKeywords:
        payload["industryKeywords"] = args.industryKeywords
    if args.revenueMin is not None:
        payload["revenueMin"] = args.revenueMin
    if args.revenueMax is not None:
        payload["revenueMax"] = args.revenueMax
    if args.employeeRangeMin is not None:
        payload["employeeRangeMin"] = args.employeeRangeMin
    if args.employeeRangeMax is not None:
        payload["employeeRangeMax"] = args.employeeRangeMax
    if args.contactAccuracyScoreMin is not None:
        payload["contactAccuracyScoreMin"] = args.contactAccuracyScoreMin
    if args.requiredFields:
        payload["requiredFields"] = args.requiredFields
    if args.sortBy:
        payload["sortBy"] = args.sortBy
        payload["sortOrder"] = args.sortOrder

    return payload


def fetch_all_pages(args, token):
    all_records = []
    max_results = 0
    page = 1

    first_payload = build_payload(args, page)
    first = requests.post(
        "https://api.zoominfo.com/search/contact",
        headers={"Content-Type": "application/json",
                 "Authorization": f"Bearer {token}"},
        json=first_payload
    )
    first.raise_for_status()
    data = first.json()

    max_results = data.get("maxResults", 0)
    total_pages = math.ceil(max_results / args.rpp) if max_results else 1
    logger.info(f"Total available: {max_results}, pages: {total_pages}")
    records = data.get("data", [])
    all_records.extend(records)
    logger.info(f"Page 1: +{len(records)} (total: {len(all_records)})")

    for page in range(2, total_pages + 1):
        time.sleep(0.5)
        r = requests.post(
            "https://api.zoominfo.com/search/contact",
            headers={"Content-Type": "application/json",
                     "Authorization": f"Bearer {token}"},
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
    parser = argparse.ArgumentParser(description="ZoomInfo Contact Search — API Ingest")
    parser.add_argument("--personId",                 default=None)
    parser.add_argument("--emailAddress",             default=None)
    parser.add_argument("--jobTitle",                 default=None)
    parser.add_argument("--jobFunction",              default=None)
    parser.add_argument("--department",               default=None)
    parser.add_argument("--managementLevel",          default=None)
    parser.add_argument("--companyId",                default=None)
    parser.add_argument("--companyName",              default=None)
    parser.add_argument("--country",                  default=None)
    parser.add_argument("--state",                    default=None)
    parser.add_argument("--industryKeywords",         nargs="+", default=None)
    parser.add_argument("--revenueMin",               type=int, default=None)
    parser.add_argument("--revenueMax",               type=int, default=None)
    parser.add_argument("--employeeRangeMin",         type=int, default=None)
    parser.add_argument("--employeeRangeMax",         type=int, default=None)
    parser.add_argument("--contactAccuracyScoreMin",  type=int, default=None)
    parser.add_argument("--requiredFields",           nargs="+", default=None)
    parser.add_argument("--rpp",                      type=int, default=25)
    parser.add_argument("--sortBy",                   default="lastUpdatedDate")
    parser.add_argument("--sortOrder",                default="desc",
                        choices=["asc", "desc"])
    parser.add_argument("--tag",                      default="search",
                        help="Label used in the S3 filename")
    return parser.parse_args()


def main():
    args  = parse_args()
    token = get_access_token()
    s3    = boto3.client("s3",
                         aws_access_key_id=AWS_KEY,
                         aws_secret_access_key=AWS_SECRET,
                         region_name="ap-south-1")

    all_records, max_results = fetch_all_pages(args, token)

    if not all_records:
        logger.warning("No records found — nothing saved")
        return

    ts     = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    s3_key = f"zoominfo/contact_search/{args.tag}_{ts}.json"

    envelope = {
        "ingested_at"    : datetime.utcnow().isoformat() + "Z",
        "tag"            : args.tag,
        "total_available": max_results,
        "total_fetched"  : len(all_records),
        "filters": {
            "personId"               : args.personId,
            "emailAddress"           : args.emailAddress,
            "jobTitle"               : args.jobTitle,
            "jobFunction"            : args.jobFunction,
            "department"             : args.department,
            "managementLevel"        : args.managementLevel,
            "companyId"              : args.companyId,
            "companyName"            : args.companyName,
            "country"                : args.country,
            "state"                  : args.state,
            "industryKeywords"       : args.industryKeywords,
            "revenueMin"             : args.revenueMin,
            "revenueMax"             : args.revenueMax,
            "employeeRangeMin"       : args.employeeRangeMin,
            "employeeRangeMax"       : args.employeeRangeMax,
            "contactAccuracyScoreMin": args.contactAccuracyScoreMin,
            "requiredFields"         : args.requiredFields,
            "sortBy"                 : args.sortBy,
            "sortOrder"              : args.sortOrder
        },
        "data": all_records
    }

    s3.put_object(
        Bucket=BUCKET,
        Key=s3_key,
        Body=json.dumps(envelope, indent=2),
        ContentType="application/json"
    )
    logger.info(f"Saved {len(all_records)} records → s3://{BUCKET}/{s3_key}")

    logger.info("API ingest complete — starting RAW processing...")
    subprocess.run(
        ["python3", f"{ETL_BASE}/WS-DEV-RAW-DATA/nb_zoominfo_contact_search_raw.py"],
        check=True
    )
    logger.info("RAW processing completed.")


if __name__ == "__main__":
    main()
