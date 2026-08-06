import os
#!/usr/bin/env python3
# --------------------------------------------------------------------------------------
# Trademo Company Matcher — API INGEST
#
# Usage (single company):
#   python3 nb_trademo_company_matcher.py \
#     --companyName "Ikea" \
#     --companyCountry "sweden" \
#     --probableMatchesLimit 10
#
#   Batch mode (file with one company per line: name|country):
#     python3 nb_trademo_company_matcher.py --batchFile companies.txt
#
# Saves to s3a://goglo-bronze-layer/trademo/company_matcher/
# Chains to WS-DEV-RAW-DATA/nb_trademo_company_matcher_raw.py
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

TOKEN_URL = ("https://trademo.com/trademo/api/generateAPIToken"
             "?auth_key=U3JpcmFtQHBvd2VyY296bW8uY29tOjY0OWJkYmRiMjhiZWVlNmY5MDJhMWZjZDg3OTZkMjlj")
AUTH_HDR  = "cThadVkzbko2VkZLbzFhRGRmZ2hqRFNGRFNGREdHamhnaGpzZA=="
API_URL   = "https://trademo.com/api/v1/company_matcher_api"
BUCKET    = "goglo-bronze-layer"
AWS_KEY   = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET= os.environ.get("AWS_SECRET_ACCESS_KEY", "")


def get_session_token():
    r = requests.get(TOKEN_URL, headers={"Auth-Custom-Header": AUTH_HDR})
    r.raise_for_status()
    token = r.json()["token"]
    logger.info(f"Token: {token[:12]}...")
    return token


def call_api(company_name, company_country, limit, session_id):
    r = requests.post(API_URL,
                      headers={"Content-Type": "application/json",
                                "sessionid": session_id,
                                "Auth-Custom-Header": AUTH_HDR},
                      data=json.dumps({
                          "companyName"         : company_name,
                          "probableMatchesLimit": limit,
                          "companyCountry"      : company_country
                      }))
    r.raise_for_status()
    return r.json()


def save_to_s3(data, company_name, s3_client):
    safe = company_name.strip().lower().replace(" ", "_")
    s3_key = f"trademo/company_matcher/{safe}.json"
    s3_client.put_object(Bucket=BUCKET, Key=s3_key,
                          Body=json.dumps(data, indent=2),
                          ContentType="application/json")
    logger.info(f"Saved → s3://{BUCKET}/{s3_key}")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--companyName")
    parser.add_argument("--companyCountry", default="")
    parser.add_argument("--probableMatchesLimit", type=int, default=10)
    parser.add_argument("--batchFile", help="File: name|country per line")
    return parser.parse_args()


def main():
    args       = parse_args()
    session_id = get_session_token()
    s3_client  = boto3.client("s3", aws_access_key_id=AWS_KEY,
                               aws_secret_access_key=AWS_SECRET, region_name="ap-south-1")

    if args.batchFile:
        with open(args.batchFile) as f:
            for line in f:
                parts = line.strip().split("|")
                if not parts[0]:
                    continue
                name    = parts[0]
                country = parts[1] if len(parts) > 1 else ""
                try:
                    data = call_api(name, country, args.probableMatchesLimit, session_id)
                    save_to_s3(data, name, s3_client)
                except Exception as e:
                    logger.warning(f"Failed for '{name}': {e}")
    else:
        if not args.companyName:
            raise ValueError("Provide --companyName or --batchFile")
        data = call_api(args.companyName, args.companyCountry,
                        args.probableMatchesLimit, session_id)
        save_to_s3(data, args.companyName, s3_client)

    logger.info("API ingest complete — starting RAW processing...")
    subprocess.run(
        ["python3", f"{ETL_BASE}/WS-DEV-RAW-DATA/nb_trademo_company_matcher_raw.py"],
        check=True
    )
    logger.info("RAW processing completed.")


if __name__ == "__main__":
    main()
