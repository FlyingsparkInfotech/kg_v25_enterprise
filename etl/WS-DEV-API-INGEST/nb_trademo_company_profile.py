#!/usr/bin/env python3
"""
Trademo Company Profile — API Ingest
WS-DEV-API-INGEST/nb_trademo_company_profile.py

Fetches company profile data from the Trademo API and saves each response
as a JSON file to S3 (goglo-bronze-layer). Supports single-company mode
(--companyID) or batch mode (--fromBSL) which reads all company IDs from
raw.trademo_buyer_supplier_list via psycopg2.

Chains to: nb_trademo_company_profile_raw.py
"""

import os
import sys
import json
import argparse
import subprocess
import logging
from datetime import datetime

import boto3
import psycopg2
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
PROFILE_URL  = "https://trademo.com/api/v2.0/global_buyer_supplier_company_profile"
S3_BUCKET    = "goglo-bronze-layer"
S3_PREFIX    = "trademo/company-profile"

PGS_DSN = (
    "host=localhost port=5432 dbname=goglo_etl "
    "user=etl_user password=EtlCozmo@2026!"
)

NEXT_SCRIPT = os.path.join(
    ETL_BASE,
    "WS-DEV-RAW-DATA",
    "nb_trademo_company_profile_raw.py",
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


def fetch_profile(
    token: str,
    company_id: str,
    from_date: str,
    to_date: str,
    trading_partner_limit: int,
    trading_partner_trade_volume_unit: str,
    include_subsidiary_companies: bool,
) -> dict:
    """Call the Trademo company profile endpoint for one company ID."""
    payload = {
        "companyID": company_id,
        "tradingPartnerLimit": trading_partner_limit,
        "tradeTimePeriod": {
            "fromDate": from_date,
            "toDate": to_date,
        },
        "includeSubsidiaryCompanies": include_subsidiary_companies,
        "tradingPartnerTradeVolumeUnit": trading_partner_trade_volume_unit,
    }
    resp = requests.post(
        PROFILE_URL,
        json=payload,
        headers={
            "Content-Type": "application/json",
            "sessionid": token,
            "Auth-Custom-Header": AUTH_HDR,
        },
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()


def upload_to_s3(data: dict, company_id: str) -> None:
    """Upload a company profile JSON to S3."""
    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    key = f"{S3_PREFIX}/{company_id}.json"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(data),
        ContentType="application/json",
    )
    log.info("Uploaded s3://%s/%s", S3_BUCKET, key)


def get_company_ids_from_bsl() -> list:
    """Read distinct company IDs from raw.trademo_buyer_supplier_list."""
    conn = psycopg2.connect(PGS_DSN)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT company_id "
            "FROM raw.trademo_buyer_supplier_list "
            "WHERE company_id IS NOT NULL"
        )
        rows = cur.fetchall()
        ids = [str(r[0]) for r in rows]
        log.info("Fetched %d company IDs from BSL.", len(ids))
        return ids
    finally:
        conn.close()


def process_company(
    token: str,
    company_id: str,
    args: argparse.Namespace,
) -> bool:
    """Fetch and upload one company profile. Returns True on success."""
    try:
        data = fetch_profile(
            token=token,
            company_id=company_id,
            from_date=args.fromDate,
            to_date=args.toDate,
            trading_partner_limit=args.tradingPartnerLimit,
            trading_partner_trade_volume_unit=args.tradingPartnerTradeVolumeUnit,
            include_subsidiary_companies=args.includeSubsidiaryCompanies,
        )
        has_data = bool(data.get("companyID"))
        if not has_data:
            log.warning("No data returned for companyID=%s", company_id)
            return False
        upload_to_s3(data, company_id)
        return True
    except Exception as exc:
        log.error("Failed for companyID=%s: %s", company_id, exc)
        return False


def chain_next() -> None:
    """Invoke the raw-layer notebook script as a subprocess."""
    if not os.path.exists(NEXT_SCRIPT):
        log.warning("Next script not found at %s — skipping chain.", NEXT_SCRIPT)
        return
    log.info("Chaining to %s", NEXT_SCRIPT)
    result = subprocess.run(
        [sys.executable, NEXT_SCRIPT],
        check=False,
    )
    if result.returncode != 0:
        log.error("Next script exited with code %d", result.returncode)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Trademo Company Profile — API Ingest"
    )
    parser.add_argument("--companyID", type=str, default=None,
                        help="Single company ID to fetch.")
    parser.add_argument("--fromDate", required=True,
                        help="Start of trade time period (YYYY-MM-DD).")
    parser.add_argument("--toDate", required=True,
                        help="End of trade time period (YYYY-MM-DD).")
    parser.add_argument("--tradingPartnerLimit", type=int, default=10,
                        help="Max trading partners to return (default: 10).")
    parser.add_argument(
        "--tradingPartnerTradeVolumeUnit",
        default="weight",
        choices=["weight", "value"],
        help="Volume unit for trading partner trade (default: weight).",
    )
    parser.add_argument("--includeSubsidiaryCompanies", action="store_true",
                        help="Include subsidiary companies in the profile.")
    parser.add_argument(
        "--fromBSL",
        action="store_true",
        help=(
            "Batch mode: read all company_id values from "
            "raw.trademo_buyer_supplier_list and call API for each."
        ),
    )
    args = parser.parse_args()

    if not args.fromBSL and not args.companyID:
        parser.error("Provide --companyID or --fromBSL.")

    return args


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    args = parse_args()

    token = get_token()

    if args.fromBSL:
        company_ids = get_company_ids_from_bsl()
        success = failure = 0
        for cid in company_ids:
            ok = process_company(token, cid, args)
            if ok:
                success += 1
            else:
                failure += 1
        log.info("Batch complete. success=%d  failure=%d", success, failure)
    else:
        ok = process_company(token, args.companyID, args)
        if not ok:
            log.error("Ingest failed for companyID=%s", args.companyID)
            sys.exit(1)

    chain_next()
    log.info("nb_trademo_company_profile.py — done.")


if __name__ == "__main__":
    main()
