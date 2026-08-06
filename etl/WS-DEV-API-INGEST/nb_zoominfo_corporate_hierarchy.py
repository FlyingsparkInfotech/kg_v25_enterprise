import os
#!/usr/bin/env python3
# --------------------------------------------------------------------------------------
# ZoomInfo Corporate Hierarchy — API INGEST
#
# Usage (single company by name):
#   python3 nb_zoominfo_corporate_hierarchy.py \
#     --companyName "LG Electronics" \
#     --outputFields parentage familyTree companyId
#
#   By ZoomInfo companyId:
#     python3 nb_zoominfo_corporate_hierarchy.py \
#       --companyId 344589814
#
#   Batch mode (reads company IDs from zoominfo.contact_search):
#     python3 nb_zoominfo_corporate_hierarchy.py --fromContacts
#
# Saves to s3a://goglo-bronze-layer/zoominfo/corporate_hierarchy/
# Chains to WS-DEV-RAW-DATA/nb_zoominfo_corporate_hierarchy_raw.py
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

ETL_BASE   = "/opt/.debug/kg_v25_enterprise/etl"
BUCKET     = "goglo-bronze-layer"
AWS_KEY    = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
ZI_USER    = "sriram@powercozmo.com"
ZI_PASS    = "wewfox-3vanve-fecwuZ"

PG_CONFIG = dict(host="localhost", port=5432, dbname="goglo_etl",
                 user="etl_user", password="EtlCozmo@2026!")


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


def call_api(match_input, output_fields, token):
    r = requests.post(
        "https://api.zoominfo.com/enrich/corporatehierarchy",
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
        json={"matchCompanyInput": [match_input], "outputFields": output_fields}
    )
    r.raise_for_status()
    return r.json()


def has_data(resp):
    if not resp.get("success"):
        return False
    results = resp.get("data", {}).get("result", [])
    return any(r.get("data") for r in results)


def save_to_s3(response_json, company_name, company_id, output_fields, s3):
    ts     = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    id_tag = (company_name.lower().replace(" ", "_") if company_name
              else str(company_id))
    s3_key = f"zoominfo/corporate_hierarchy/{id_tag}_{ts}.json"

    envelope = {
        "ingested_at"  : datetime.utcnow().isoformat() + "Z",
        "match_input"  : {"companyName": company_name, "companyId": company_id},
        "output_fields": output_fields,
        "response"     : response_json
    }
    s3.put_object(Bucket=BUCKET, Key=s3_key,
                  Body=json.dumps(envelope, indent=2),
                  ContentType="application/json")
    logger.info(f"Saved → s3://{BUCKET}/{s3_key}")


def get_company_ids_from_contacts():
    """Get distinct ZoomInfo company IDs from zoominfo.contact_search."""
    conn = psycopg2.connect(**PG_CONFIG)
    cur  = conn.cursor()
    cur.execute("""
        SELECT DISTINCT company_id
        FROM zoominfo.contact_search
        WHERE company_id IS NOT NULL
        LIMIT 500
    """)
    ids = [row[0] for row in cur.fetchall()]
    cur.close(); conn.close()
    logger.info(f"Found {len(ids)} company IDs from contact_search")
    return ids


def parse_args():
    parser = argparse.ArgumentParser()
    group  = parser.add_mutually_exclusive_group()
    group.add_argument("--companyName", default=None)
    group.add_argument("--companyId",   type=int, default=None)
    group.add_argument("--fromContacts", action="store_true", default=False,
                       help="Process all company IDs from zoominfo.contact_search")
    parser.add_argument("--outputFields", nargs="+",
                        default=["parentage", "familyTree", "companyId"])
    return parser.parse_args()


def main():
    args  = parse_args()
    token = get_access_token()
    s3    = boto3.client("s3", aws_access_key_id=AWS_KEY,
                          aws_secret_access_key=AWS_SECRET, region_name="ap-south-1")

    if args.fromContacts:
        ids = get_company_ids_from_contacts()
        ok = 0; skip = 0
        for cid in ids:
            try:
                resp = call_api({"companyId": cid}, args.outputFields, token)
                if has_data(resp):
                    save_to_s3(resp, None, cid, args.outputFields, s3)
                    ok += 1
                else:
                    skip += 1
            except Exception as e:
                logger.warning(f"Company {cid} failed: {e}")
                skip += 1
        logger.info(f"Batch done — {ok} saved, {skip} skipped")
    elif args.companyName:
        resp = call_api({"companyName": args.companyName}, args.outputFields, token)
        if has_data(resp):
            save_to_s3(resp, args.companyName, None, args.outputFields, s3)
        else:
            logger.warning("No data returned")
    elif args.companyId:
        resp = call_api({"companyId": args.companyId}, args.outputFields, token)
        if has_data(resp):
            save_to_s3(resp, None, args.companyId, args.outputFields, s3)
        else:
            logger.warning("No data returned")
    else:
        raise ValueError("Provide --companyName, --companyId, or --fromContacts")

    logger.info("API ingest complete — starting RAW processing...")
    subprocess.run(
        ["python3", f"{ETL_BASE}/WS-DEV-RAW-DATA/nb_zoominfo_corporate_hierarchy_raw.py"],
        check=True
    )
    logger.info("RAW processing completed.")


if __name__ == "__main__":
    main()
