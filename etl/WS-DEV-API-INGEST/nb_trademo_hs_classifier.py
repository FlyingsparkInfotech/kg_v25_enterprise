import os
#!/usr/bin/env python3
# --------------------------------------------------------------------------------------
# Trademo HS Classifier — API INGEST
#
# Usage:
#   python3 nb_trademo_hs_classifier.py \
#     --productTitle "Pipe Fittings" \
#     --productDescription "Fittings of Iron or Steel" \
#     --countryOfClassification "IN" \
#     --tradeDirection "Import"
#
#   Batch mode (one product per line: title|description|country|direction):
#     python3 nb_trademo_hs_classifier.py --batchFile products.txt
#
# Saves JSON to s3a://goglo-bronze-layer/trademo/hs-classifier/
# Chains to WS-DEV-RAW-DATA/nb_trademo_hs_classifier_raw.py
# --------------------------------------------------------------------------------------

import requests
import json
import argparse
import boto3
import subprocess
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s] %(levelname)s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

ETL_BASE = "/opt/.debug/kg_v25_enterprise/etl"

AWS_KEY    = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
BUCKET     = "goglo-bronze-layer"

TOKEN_URL  = ("https://trademo.com/trademo/api/generateAPIToken"
              "?auth_key=U3JpcmFtQHBvd2VyY296bW8uY29tOjY0OWJkYmRiMjhiZWVlNmY5MDJhMWZjZDg3OTZkMjlj")
AUTH_HDR   = "cThadVkzbko2VkZLbzFhRGRmZ2hqRFNGRFNGREdHamhnaGpzZA=="
API_URL    = "http://classifierapi.trademo.com/api/v1/suggestionBasedHsClassifier"


def get_session_token():
    r = requests.get(TOKEN_URL, headers={"Auth-Custom-Header": AUTH_HDR})
    r.raise_for_status()
    token = r.json()["token"]
    logger.info(f"Token generated: {token[:12]}...")
    return token


def call_api(payload, session_id):
    headers = {
        "Content-Type"      : "application/json",
        "sessionid"         : session_id,
        "Auth-Custom-Header": AUTH_HDR
    }
    r = requests.post(API_URL, headers=headers, data=json.dumps(payload))
    r.raise_for_status()
    return r.json()


def save_to_s3(envelope, title, country, direction, s3_client):
    safe_title = title.strip().lower().replace(" ", "_")
    s3_key = (f"trademo/hs-classifier/"
              f"{safe_title}__{country.lower()}__{direction.lower()}.json")
    s3_client.put_object(
        Bucket=BUCKET, Key=s3_key,
        Body=json.dumps(envelope, indent=2),
        ContentType="application/json"
    )
    logger.info(f"Saved → s3://{BUCKET}/{s3_key}")


def process_single(title, description, country, direction, sku_id, session_id, s3_client):
    payload = {
        "sku_id"                  : sku_id,
        "productTitle"            : title,
        "productDescription"      : description,
        "countryOfClassification" : country,
        "tradeDirection"          : direction
    }
    resp = call_api(payload, session_id)
    if resp.get("status") != "success" or not resp.get("mostSuitableHs"):
        logger.warning(f"No classification for '{title}' — skipping")
        return
    envelope = {
        "ingested_at" : datetime.utcnow().isoformat() + "Z",
        "request"     : {
            "product_title"             : title,
            "product_description"       : description,
            "country_of_classification" : country,
            "trade_direction"           : direction,
            "sku_id"                    : sku_id
        },
        "response": resp
    }
    save_to_s3(envelope, title, country, direction, s3_client)
    logger.info(f"  Most suitable HS: {resp['mostSuitableHs'].get('hsCode')}")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--productTitle")
    parser.add_argument("--productDescription")
    parser.add_argument("--countryOfClassification")
    parser.add_argument("--tradeDirection", choices=["Import", "Export"])
    parser.add_argument("--skuId", default="")
    parser.add_argument("--batchFile", help="File with lines: title|description|country|direction")
    return parser.parse_args()


def main():
    args       = parse_args()
    session_id = get_session_token()
    s3_client  = boto3.client(
        "s3",
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
        region_name="ap-south-1"
    )

    if args.batchFile:
        with open(args.batchFile) as f:
            for line in f:
                parts = line.strip().split("|")
                if len(parts) < 4:
                    continue
                title, desc, country, direction = parts[0], parts[1], parts[2], parts[3]
                sku_id = parts[4] if len(parts) > 4 else ""
                logger.info(f"Processing: {title}")
                process_single(title, desc, country, direction, sku_id, session_id, s3_client)
    else:
        if not all([args.productTitle, args.productDescription,
                    args.countryOfClassification, args.tradeDirection]):
            raise ValueError("Provide --productTitle, --productDescription, "
                             "--countryOfClassification, --tradeDirection  OR  --batchFile")
        process_single(args.productTitle, args.productDescription,
                       args.countryOfClassification, args.tradeDirection,
                       args.skuId, session_id, s3_client)

    logger.info("API ingest complete — starting RAW processing...")
    subprocess.run(
        ["python3", f"{ETL_BASE}/WS-DEV-RAW-DATA/nb_trademo_hs_classifier_raw.py"],
        check=True
    )
    logger.info("RAW processing completed.")


if __name__ == "__main__":
    main()
