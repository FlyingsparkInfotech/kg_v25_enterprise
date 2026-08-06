import os
#!/usr/bin/env python3
# --------------------------------------------------------------------------------------
# Trademo Relationship Health — API INGEST
#
# Usage (single pair):
#   python3 nb_trademo_relationship_health.py \
#     --supplierID 6077ad0a507f71071df7930e \
#     --buyerID    62a4648e4f9b4b6478264394 \
#     --fromDate   2020-01-01 \
#     --toDate     2024-07-31
#
#   Batch mode (reads all BSL pairs from Postgres and processes them):
#     python3 nb_trademo_relationship_health.py --fromBSL \
#       --fromDate 2020-01-01 --toDate 2024-07-31
#
# Saves JSON to s3a://goglo-bronze-layer/trademo/relationship-health/
# Chains to WS-DEV-RAW-DATA/nb_trademo_relationship_health_raw.py
# --------------------------------------------------------------------------------------

import requests
import json
import argparse
import boto3
import subprocess
import logging
import psycopg2
from datetime import datetime

logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s] %(levelname)s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

ETL_BASE = "/opt/.debug/kg_v25_enterprise/etl"

AWS_KEY    = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
BUCKET     = "goglo-bronze-layer"

TOKEN_URL = ("https://trademo.com/trademo/api/generateAPIToken"
             "?auth_key=U3JpcmFtQHBvd2VyY296bW8uY29tOjY0OWJkYmRiMjhiZWVlNmY5MDJhMWZjZDg3OTZkMjlj")
AUTH_HDR  = "cThadVkzbko2VkZLbzFhRGRmZ2hqRFNGRFNGREdHamhnaGpzZA=="
API_URL   = "https://trademo.com/api/v1/relationshipHealthAPI"

PG_CONFIG = dict(host="localhost", port=5432, dbname="goglo_etl",
                 user="etl_user", password="EtlCozmo@2026!")


def get_session_token():
    r = requests.get(TOKEN_URL, headers={"Auth-Custom-Header": AUTH_HDR})
    r.raise_for_status()
    token = r.json()["token"]
    logger.info(f"Token: {token[:12]}...")
    return token


def call_api(supplier_id, buyer_id, from_date, to_date,
             supplier_name="", supplier_country="",
             buyer_name="", buyer_country="", session_id=""):
    payload = {
        "supplierId"         : supplier_id,
        "supplierName"       : supplier_name,
        "supplierCountryName": supplier_country,
        "buyerId"            : buyer_id,
        "buyerName"          : buyer_name,
        "buyerCountryName"   : buyer_country,
        "tradeFromDate"      : from_date,
        "tradeToDate"        : to_date
    }
    headers = {
        "Content-Type"      : "application/json",
        "sessionID"         : session_id,
        "Auth-Custom-Header": AUTH_HDR
    }
    r = requests.post(API_URL, headers=headers, data=json.dumps(payload))
    r.raise_for_status()
    return r.json()


def save_to_s3(response_json, supplier_id, buyer_id, from_date, to_date, s3_client):
    s3_key = f"trademo/relationship-health/{supplier_id}__{buyer_id}.json"
    envelope = {
        "ingested_at" : datetime.utcnow().isoformat() + "Z",
        "request"     : {
            "supplier_id": supplier_id,
            "buyer_id"   : buyer_id,
            "from_date"  : from_date,
            "to_date"    : to_date
        },
        "response": response_json
    }
    s3_client.put_object(
        Bucket=BUCKET, Key=s3_key,
        Body=json.dumps(envelope, indent=2),
        ContentType="application/json"
    )
    logger.info(f"Saved → s3://{BUCKET}/{s3_key}")


def get_bsl_pairs():
    """Fetch all unique (supplier_id, buyer_id) pairs from raw.trademo_buyer_supplier_list."""
    conn = psycopg2.connect(**PG_CONFIG)
    cur  = conn.cursor()
    cur.execute("""
        SELECT DISTINCT supplier_id, buyer_id
        FROM raw.trademo_buyer_supplier_list
        WHERE supplier_id IS NOT NULL AND buyer_id IS NOT NULL
    """)
    pairs = cur.fetchall()
    cur.close(); conn.close()
    logger.info(f"Found {len(pairs)} BSL pairs")
    return pairs


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--supplierID")
    parser.add_argument("--buyerID")
    parser.add_argument("--fromDate", required=True)
    parser.add_argument("--toDate",   required=True)
    parser.add_argument("--fromBSL",  action="store_true",
                        help="Process all pairs from raw.trademo_buyer_supplier_list")
    return parser.parse_args()


def main():
    args       = parse_args()
    session_id = get_session_token()
    s3_client  = boto3.client("s3",
                               aws_access_key_id=AWS_KEY,
                               aws_secret_access_key=AWS_SECRET,
                               region_name="ap-south-1")

    if args.fromBSL:
        pairs = get_bsl_pairs()
        ok = 0; skip = 0
        for (supplier_id, buyer_id) in pairs:
            try:
                resp = call_api(supplier_id, buyer_id,
                                args.fromDate, args.toDate,
                                session_id=session_id)
                if resp:
                    save_to_s3(resp, supplier_id, buyer_id,
                               args.fromDate, args.toDate, s3_client)
                    ok += 1
                else:
                    skip += 1
            except Exception as e:
                logger.warning(f"Pair {supplier_id}/{buyer_id} failed: {e}")
                skip += 1
        logger.info(f"BSL batch done — {ok} saved, {skip} skipped")
    else:
        if not args.supplierID or not args.buyerID:
            raise ValueError("Provide --supplierID and --buyerID, or --fromBSL")
        resp = call_api(args.supplierID, args.buyerID,
                        args.fromDate, args.toDate,
                        session_id=session_id)
        if resp:
            save_to_s3(resp, args.supplierID, args.buyerID,
                       args.fromDate, args.toDate, s3_client)
        else:
            logger.warning("No data returned")

    logger.info("API ingest complete — starting RAW processing...")
    subprocess.run(
        ["python3", f"{ETL_BASE}/WS-DEV-RAW-DATA/nb_trademo_relationship_health_raw.py"],
        check=True
    )
    logger.info("RAW processing completed.")


if __name__ == "__main__":
    main()
