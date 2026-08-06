#!/usr/bin/env python3
"""
Trademo Shipment Search — API Ingest
WS-DEV-API-INGEST/nb_trademo_shipment_search.py

Paginates through the Trademo shipment search API and saves each page as a
JSON file under a timestamped prefix in S3 so multiple runs do not overwrite
each other.

S3 path  : s3://goglo-bronze-layer/trademo/shipments/{timestamp}/page_{N}.json
Chains to: nb_trademo_shipment_search_raw.py

Pagination: keep incrementing pageNumber until the API returns HTTP 400
            (meaning no more pages).
"""

import os
import sys
import json
import argparse
import subprocess
import logging
from datetime import datetime

import boto3
import requests

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
ETL_BASE = "/opt/.debug/kg_v25_enterprise/etl"

AWS_ACCESS_KEY_ID     = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "")

AUTH_KEY = "U3JpcmFtQHBvd2VyY296bW8uY29tOjY0OWJkYmRiMjhiZWVlNmY5MDJhMWZjZDg3OTZkMjlj"
AUTH_HDR = "cThadVkzbko2VkZLbzFhRGRmZ2hqRFNGRFNGREdHamhnaGpzZA=="

TOKEN_URL    = f"https://trademo.com/trademo/api/generateAPIToken?auth_key={AUTH_KEY}"
SEARCH_URL   = "https://trademo.com/api/v2.0/shipment_search_api"
S3_BUCKET    = "goglo-bronze-layer"
S3_BASE      = "trademo/shipments"

NEXT_SCRIPT = os.path.join(
    ETL_BASE,
    "WS-DEV-RAW-DATA",
    "nb_trademo_shipment_search_raw.py",
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def get_token() -> str:
    """Obtain a fresh Trademo session token."""
    resp = requests.get(
        TOKEN_URL,
        headers={"Auth-Custom-Header": AUTH_HDR},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    token = data.get("sessionid") or data.get("token") or data.get("data", {}).get("sessionid", "")
    if not token:
        raise RuntimeError(f"Could not extract token from response: {data}")
    log.info("Trademo token acquired.")
    return token


def build_payload(args: argparse.Namespace, page_number: int) -> dict:
    """Construct the search payload for one page."""
    payload: dict = {
        "fromDate": args.fromDate,
        "toDate": args.toDate,
        "pageNumber": page_number,
        "sortField": args.sortField,
        "sortDirection": args.sortDirection,
    }
    if args.shipperName:
        payload["shipperName"] = args.shipperName
    if args.shipperId:
        payload["shipperId"] = args.shipperId
    if args.consigneeName:
        payload["consigneeName"] = args.consigneeName
    if args.consigneeId:
        payload["consigneeId"] = args.consigneeId
    if args.shipperCountryName:
        payload["shipperCountryName"] = args.shipperCountryName
    if args.consigneeCountryName:
        payload["consigneeCountryName"] = args.consigneeCountryName
    if args.portOfLading:
        payload["portOfLading"] = args.portOfLading
    if args.portOfUnlading:
        payload["portOfUnlading"] = args.portOfUnlading
    if args.productKeywords:
        payload["productKeywords"] = args.productKeywords
    if args.hsCodes:
        payload["hsCodes"] = args.hsCodes
    return payload


def upload_page(s3_client, data: dict, ts_prefix: str, page_number: int) -> None:
    key = f"{S3_BASE}/{ts_prefix}/page_{page_number}.json"
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(data),
        ContentType="application/json",
    )
    log.info("Uploaded s3://%s/%s", S3_BUCKET, key)


def paginate(token: str, args: argparse.Namespace, ts_prefix: str) -> int:
    """Paginate through all results. Returns number of pages saved."""
    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    headers = {
        "Content-Type": "application/json",
        "sessionid": token,
        "Auth-Custom-Header": AUTH_HDR,
    }
    page = 1
    saved = 0

    while True:
        payload = build_payload(args, page)
        log.info("Fetching page %d ...", page)
        resp = requests.post(SEARCH_URL, json=payload, headers=headers, timeout=120)

        if resp.status_code == 400:
            log.info("HTTP 400 received — end of pagination at page %d.", page)
            break

        resp.raise_for_status()
        data = resp.json()
        upload_page(s3, data, ts_prefix, page)
        saved += 1
        page += 1

    return saved


def chain_next() -> None:
    if not os.path.exists(NEXT_SCRIPT):
        log.warning("Next script not found at %s — skipping chain.", NEXT_SCRIPT)
        return
    log.info("Chaining to %s", NEXT_SCRIPT)
    result = subprocess.run([sys.executable, NEXT_SCRIPT], check=False)
    if result.returncode != 0:
        log.error("Next script exited with code %d", result.returncode)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Trademo Shipment Search — API Ingest"
    )
    parser.add_argument("--fromDate", required=True,
                        help="Start date for shipment search (YYYY-MM-DD).")
    parser.add_argument("--toDate", required=True,
                        help="End date for shipment search (YYYY-MM-DD).")
    parser.add_argument("--shipperName", nargs="*", default=[],
                        help="One or more shipper names to filter.")
    parser.add_argument("--shipperId", nargs="*", default=[],
                        help="One or more shipper IDs to filter.")
    parser.add_argument("--consigneeName", nargs="*", default=[],
                        help="One or more consignee names to filter.")
    parser.add_argument("--consigneeId", nargs="*", default=[],
                        help="One or more consignee IDs to filter.")
    parser.add_argument("--shipperCountryName", nargs="*", default=[],
                        help="Shipper country names to filter.")
    parser.add_argument("--consigneeCountryName", nargs="*", default=[],
                        help="Consignee country names to filter.")
    parser.add_argument("--portOfLading", nargs="*", default=[],
                        help="Ports of lading to filter.")
    parser.add_argument("--portOfUnlading", nargs="*", default=[],
                        help="Ports of unlading to filter.")
    parser.add_argument("--productKeywords", nargs="*", default=[],
                        help="Product description keywords to filter.")
    parser.add_argument("--hsCodes", nargs="*", default=[],
                        help="HS codes to filter.")
    parser.add_argument("--sortField", default="shipmentDate",
                        help="Field to sort results by (default: shipmentDate).")
    parser.add_argument("--sortDirection", default="desc",
                        help="Sort direction: asc or desc (default: desc).")
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    args = parse_args()
    token = get_token()

    # Timestamp prefix ensures multiple runs land in separate S3 directories
    ts_prefix = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    log.info("Run timestamp prefix: %s", ts_prefix)

    pages_saved = paginate(token, args, ts_prefix)
    log.info("Total pages saved: %d", pages_saved)

    if pages_saved == 0:
        log.warning("No pages were saved — check filter parameters.")
        sys.exit(0)

    chain_next()
    log.info("nb_trademo_shipment_search.py — done.")


if __name__ == "__main__":
    main()
