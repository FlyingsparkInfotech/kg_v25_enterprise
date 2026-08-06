import os
#!/usr/bin/env python3
# --------------------------------------------------------------------------------------
# Trademo Buyer Supplier List — API INGEST
# Paginated search by HS codes and/or product keywords
#
# Usage (by HS code):
#   python3 nb_trademo_buyer_supplier_list.py \
#     --fromDate 2026-01-01 --toDate 2026-07-31 \
#     --companyRole Supplier --companyCountryName "India" "China" \
#     --hsCodes "7202" "7201" --pageSize 10
#
# Usage (by keywords):
#   python3 nb_trademo_buyer_supplier_list.py \
#     --fromDate 2026-01-01 --toDate 2026-07-31 \
#     --companyRole Buyer --companyCountryName "United Kingdom" \
#     --productKeywords "baklawa" "kunafa" --pageSize 10
#
# Saves JSON pages to s3a://goglo-bronze-layer/trademo/buyer-supplier-list/
# Chains to WS-DEV-RAW-DATA/nb_trademo_buyer_supplier_list_raw.py
# --------------------------------------------------------------------------------------

import requests
import json
import argparse
import boto3
import time
import subprocess
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s] %(levelname)s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

ETL_BASE   = "/opt/.debug/kg_v25_enterprise/etl"
AWS_KEY    = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
BUCKET     = "goglo-bronze-layer"

TOKEN_URL = ("https://trademo.com/trademo/api/generateAPIToken"
             "?auth_key=U3JpcmFtQHBvd2VyY296bW8uY29tOjY0OWJkYmRiMjhiZWVlNmY5MDJhMWZjZDg3OTZkMjlj")
AUTH_HDR  = "cThadVkzbko2VkZLbzFhRGRmZ2hqRFNGRFNGREdHamhnaGpzZA=="
API_URL   = "https://trademo.com/api/v1/global_buyer_supplier_list"


def get_session_token():
    r = requests.get(TOKEN_URL, headers={"Auth-Custom-Header": AUTH_HDR})
    r.raise_for_status()
    token = r.json()["token"]
    logger.info(f"Token generated: {token[:12]}...")
    return token


def parse_args():
    parser = argparse.ArgumentParser(description="Trademo Buyer Supplier List API")
    parser.add_argument("--fromDate", required=True)
    parser.add_argument("--toDate",   required=True)
    parser.add_argument("--companyRole", default="Supplier", choices=["Supplier", "Buyer"])
    parser.add_argument("--hsCodes",            nargs="*", default=[])
    parser.add_argument("--productKeywords",    nargs="*", default=[])
    parser.add_argument("--companyCountryName",        nargs="*", default=[])
    parser.add_argument("--countriesTradingWithList",  nargs="*", default=[])
    parser.add_argument("--excludeCompanyCountryName", nargs="*", default=[])
    parser.add_argument("--excludeHSCodes",            nargs="*", default=[])
    parser.add_argument("--excludeProductKeywords",    nargs="*", default=[])
    parser.add_argument("--pageSize", type=int, default=10)
    return parser.parse_args()


def build_payload(args, page_number):
    return {
        "companyCountryName"        : args.companyCountryName,
        "companyRole"               : args.companyRole,
        "countriesTradingWithList"  : args.countriesTradingWithList,
        "excludeCompanyCountryName" : args.excludeCompanyCountryName,
        "excludeHSCodes"            : args.excludeHSCodes,
        "excludeProductKeywords"    : args.excludeProductKeywords,
        "hsCodes"                   : args.hsCodes,
        "productKeywords"           : args.productKeywords,
        "pageSize"                  : args.pageSize,
        "pageNumber"                : page_number,
        "tradeTimePeriod"           : {"fromDate": args.fromDate, "toDate": args.toDate}
    }


def call_api(payload, session_id):
    headers = {
        "content-type"      : "application/json",
        "sessionid"         : session_id,
        "Auth-Custom-Header": AUTH_HDR
    }
    r = requests.post(API_URL, headers=headers, data=json.dumps(payload))
    if r.status_code == 400:
        logger.info("API returned 400 — end of pagination")
        return None
    r.raise_for_status()
    return r.json()


def has_data(resp):
    total     = resp.get("totalCompanies")
    companies = resp.get("companies", [])
    if total is not None:
        return total > 0
    return len(companies) > 0


def build_s3_prefix(args):
    if args.hsCodes and args.productKeywords:
        mode = "hs_and_keywords"
    elif args.hsCodes:
        mode = "hs_" + "_".join(args.hsCodes[:3])
    else:
        mode = "kw_" + "_".join(w.lower().replace(" ", "-") for w in args.productKeywords[:3])

    country_tag = "_".join(
        c.lower().replace(" ", "-") for c in args.companyCountryName[:2]
    ) if args.companyCountryName else "all"

    role      = args.companyRole.lower()
    date_tag  = f"{args.fromDate}_to_{args.toDate}"
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    return f"trademo/buyer-supplier-list/{role}/{country_tag}/{mode}/{date_tag}/{timestamp}"


def save_to_s3(data, page_number, s3_prefix, s3_client):
    s3_key = f"{s3_prefix}/page_{page_number}.json"
    s3_client.put_object(
        Bucket=BUCKET, Key=s3_key,
        Body=json.dumps(data, indent=2),
        ContentType="application/json"
    )
    logger.info(f"Saved page {page_number} → s3://{BUCKET}/{s3_key}")


def main():
    args = parse_args()
    if not args.hsCodes and not args.productKeywords:
        raise ValueError("Provide --hsCodes and/or --productKeywords")

    session_id  = get_session_token()
    s3_prefix   = build_s3_prefix(args)
    page_number = 1

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
        region_name="ap-south-1"
    )

    logger.info(f"BSL ingest: role={args.companyRole}, date={args.fromDate}→{args.toDate}")
    logger.info(f"S3 prefix: s3://{BUCKET}/{s3_prefix}")

    while True:
        logger.info(f"Fetching page {page_number}...")
        payload = build_payload(args, page_number)
        resp    = call_api(payload, session_id)

        if resp is None:
            logger.info(f"Pagination complete — {page_number - 1} pages saved")
            break
        if not has_data(resp):
            logger.info(f"No data on page {page_number} — stopping")
            break

        save_to_s3(resp, page_number, s3_prefix, s3_client)
        page_number += 1
        time.sleep(0.3)

    logger.info("API ingest complete — starting RAW processing...")
    subprocess.run(
        ["python3", f"{ETL_BASE}/WS-DEV-RAW-DATA/nb_trademo_buyer_supplier_list_raw.py"],
        check=True
    )
    logger.info("RAW processing completed.")


if __name__ == "__main__":
    main()
