#!/usr/bin/env python3
"""
ZoomInfo Company Search — API Ingest
Authenticates, paginates, and saves envelope JSON to S3 bronze layer.
Chains to nb_zoominfo_company_search_raw.py
"""

import os
import json
import argparse
import requests
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# ENV / CREDENTIALS
# ---------------------------------------------------------------------------
AWS_ACCESS_KEY_ID     = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "")

ZI_USERNAME = os.environ.get("ZI_USERNAME", "sriram@powercozmo.com")
ZI_PASSWORD = os.environ.get("ZI_PASSWORD", "wewfox-3vanve-fecwuZ")

ETL_BASE = "/opt/.debug/kg_v25_enterprise/etl"

S3_BUCKET   = "goglo-bronze-layer"
S3_PREFIX   = "zoominfo/company_search"
ZI_AUTH_URL = "https://api.zoominfo.com/authenticate"
ZI_SEARCH_URL = "https://api.zoominfo.com/search/company"

# ---------------------------------------------------------------------------
# CLI ARGS
# ---------------------------------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(description="ZoomInfo Company Search API Ingest")
    parser.add_argument("--companyId")
    parser.add_argument("--companyName")
    parser.add_argument("--companyWebsite")
    parser.add_argument("--country")
    parser.add_argument("--state")
    parser.add_argument("--industryKeywords")
    parser.add_argument("--revenueMin", type=int)
    parser.add_argument("--revenueMax", type=int)
    parser.add_argument("--employeeRangeMin")
    parser.add_argument("--employeeRangeMax")
    parser.add_argument("--fundingAmountMin", type=int)
    parser.add_argument("--fundingAmountMax", type=int)
    parser.add_argument("--fundingStartDate")
    parser.add_argument("--fundingEndDate")
    parser.add_argument("--companyType")
    parser.add_argument("--oneYearEmployeeGrowthRateMin")
    parser.add_argument("--oneYearEmployeeGrowthRateMax")
    parser.add_argument("--businessModel")
    parser.add_argument("--excludeDefunctCompanies", action="store_true")
    parser.add_argument("--rpp", type=int, default=25)
    parser.add_argument("--sortBy")
    parser.add_argument("--sortOrder", default="desc")
    return parser.parse_args()

# ---------------------------------------------------------------------------
# ZI AUTH
# ---------------------------------------------------------------------------
def authenticate():
    resp = requests.post(
        ZI_AUTH_URL,
        json={"username": ZI_USERNAME, "password": ZI_PASSWORD},
        timeout=30,
    )
    resp.raise_for_status()
    jwt = resp.json().get("jwt")
    if not jwt:
        raise RuntimeError(f"No jwt in auth response: {resp.text}")
    return jwt

# ---------------------------------------------------------------------------
# BUILD FILTER PAYLOAD
# ---------------------------------------------------------------------------
def build_filters(args):
    filters = {}
    if args.companyId:
        filters["companyId"] = args.companyId
    if args.companyName:
        filters["companyName"] = args.companyName
    if args.companyWebsite:
        filters["companyWebsite"] = args.companyWebsite
    if args.country:
        filters["country"] = args.country
    if args.state:
        filters["state"] = args.state
    if args.industryKeywords:
        filters["industryKeywords"] = args.industryKeywords
    if args.revenueMin is not None:
        filters["revenueMin"] = args.revenueMin
    if args.revenueMax is not None:
        filters["revenueMax"] = args.revenueMax
    if args.employeeRangeMin:
        filters["employeeRangeMin"] = args.employeeRangeMin
    if args.employeeRangeMax:
        filters["employeeRangeMax"] = args.employeeRangeMax
    if args.fundingAmountMin is not None:
        filters["fundingAmountMin"] = args.fundingAmountMin
    if args.fundingAmountMax is not None:
        filters["fundingAmountMax"] = args.fundingAmountMax
    if args.fundingStartDate:
        filters["fundingStartDate"] = args.fundingStartDate
    if args.fundingEndDate:
        filters["fundingEndDate"] = args.fundingEndDate
    if args.companyType:
        filters["companyType"] = args.companyType
    if args.oneYearEmployeeGrowthRateMin:
        filters["oneYearEmployeeGrowthRateMin"] = args.oneYearEmployeeGrowthRateMin
    if args.oneYearEmployeeGrowthRateMax:
        filters["oneYearEmployeeGrowthRateMax"] = args.oneYearEmployeeGrowthRateMax
    if args.businessModel:
        filters["businessModel"] = args.businessModel
    if args.excludeDefunctCompanies:
        filters["excludeDefunctCompanies"] = True
    return filters

# ---------------------------------------------------------------------------
# PAGINATE
# ---------------------------------------------------------------------------
def fetch_all(jwt, filters, rpp, sort_by, sort_order):
    headers = {"Authorization": f"Bearer {jwt}", "Content-Type": "application/json"}
    all_data = []
    page = 1
    max_results = None

    while True:
        payload = {
            "outputFields": [
                "id", "name", "website", "ticker", "phone", "revenue",
                "employeeCount", "companyType", "city", "state", "country",
                "continent", "street", "zipCode", "naicsCode", "sicCode",
                "industryKeywords", "hashtags", "founded", "isDefunct",
            ],
            "rpp": rpp,
            "pageNum": page,
        }
        if sort_by:
            payload["sortBy"] = sort_by
        if sort_order:
            payload["sortOrder"] = sort_order
        payload.update(filters)

        resp = requests.post(ZI_SEARCH_URL, json=payload, headers=headers, timeout=60)
        resp.raise_for_status()
        body = resp.json()

        if max_results is None:
            max_results = body.get("maxResults", 0)
            print(f"[ingest] total_available={max_results}")

        records = body.get("data", [])
        all_data.extend(records)
        print(f"[ingest] page={page} fetched={len(records)} cumulative={len(all_data)}")

        if not records or len(all_data) >= max_results:
            break
        page += 1

    return max_results, all_data

# ---------------------------------------------------------------------------
# SAVE TO S3
# ---------------------------------------------------------------------------
def save_to_s3(envelope, tag, timestamp_str):
    import boto3
    key = f"{S3_PREFIX}/{tag}_{timestamp_str}.json"
    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(envelope, default=str),
        ContentType="application/json",
    )
    print(f"[ingest] saved s3://{S3_BUCKET}/{key}")
    return f"s3://{S3_BUCKET}/{key}"

# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    args = parse_args()
    ingested_at = datetime.now(timezone.utc).isoformat()
    timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    filters = build_filters(args)
    tag = (
        args.companyName.replace(" ", "_")
        if args.companyName
        else args.country or args.industryKeywords or "batch"
    )

    print("[ingest] authenticating to ZoomInfo …")
    jwt = authenticate()

    print("[ingest] fetching company search results …")
    total_available, data = fetch_all(jwt, filters, args.rpp, args.sortBy, args.sortOrder)

    envelope = {
        "ingested_at": ingested_at,
        "filters": filters,
        "total_available": total_available,
        "total_fetched": len(data),
        "data": data,
    }

    s3_path = save_to_s3(envelope, tag, timestamp_str)
    print(f"[ingest] done. total_fetched={len(data)} path={s3_path}")


if __name__ == "__main__":
    main()
