import os
#!/usr/bin/env python3
# --------------------------------------------------------------------------------------
# ZoomInfo Contact Enrich — API INGEST
#
# Usage (single person):
#   python3 nb_zoominfo_contact_enrich.py --personId 12345678
#
# Usage (batch from Postgres contact_search table):
#   python3 nb_zoominfo_contact_enrich.py --fromContacts
#
# Custom output fields:
#   python3 nb_zoominfo_contact_enrich.py --personId 12345678 \
#     --outputFields mobilePhone phone email mobilePhoneDoNotCall directPhoneDoNotCall
#
# Saves to s3://goglo-bronze-layer/zoominfo/contact_enrich/person_{personId}.json
# Chains to WS-DEV-RAW-DATA/nb_zoominfo_contact_enrich_raw.py
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
USERNAME   = os.environ.get("ZI_USERNAME", "sriram@powercozmo.com")
PASSWORD   = os.environ.get("ZI_PASSWORD", "wewfox-3vanve-fecwuZ")

PG_DSN = {
    "host"    : "localhost",
    "port"    : 5432,
    "dbname"  : "goglo_etl",
    "user"    : "etl_user",
    "password": "EtlCozmo@2026!"
}


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


def enrich_person(person_id, output_fields, token):
    payload = {
        "outputFields"    : output_fields,
        "matchPersonInput": [{"personId": int(person_id)}]
    }
    r = requests.post(
        "https://api.zoominfo.com/enrich/contact",
        headers={"Content-Type": "application/json",
                 "Authorization": f"Bearer {token}"},
        json=payload
    )
    r.raise_for_status()
    return r.json()


def save_to_s3(s3_client, person_id, data):
    s3_key = f"zoominfo/contact_enrich/person_{person_id}.json"
    s3_client.put_object(
        Bucket=BUCKET,
        Key=s3_key,
        Body=json.dumps(data, indent=2),
        ContentType="application/json"
    )
    logger.info(f"Saved → s3://{BUCKET}/{s3_key}")


def get_all_contact_ids():
    conn = psycopg2.connect(**PG_DSN)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT contact_id FROM zoominfo.contact_search")
            rows = [row[0] for row in cur.fetchall()]
        logger.info(f"Fetched {len(rows)} contact_ids from zoominfo.contact_search")
        return rows
    finally:
        conn.close()


def parse_args():
    parser = argparse.ArgumentParser(description="ZoomInfo Contact Enrich — API Ingest")
    parser.add_argument("--personId",
                        default=None,
                        help="Single ZoomInfo personId to enrich")
    parser.add_argument("--outputFields",
                        nargs="+",
                        default=["mobilePhone", "phone", "email"],
                        help="Fields to request from ZoomInfo enrich API")
    parser.add_argument("--fromContacts",
                        action="store_true",
                        default=False,
                        help="Read all contact_ids from zoominfo.contact_search and enrich each")
    return parser.parse_args()


def main():
    args  = parse_args()
    token = get_access_token()
    s3    = boto3.client("s3",
                         aws_access_key_id=AWS_KEY,
                         aws_secret_access_key=AWS_SECRET,
                         region_name="ap-south-1")

    if args.fromContacts:
        contact_ids = get_all_contact_ids()
        if not contact_ids:
            logger.warning("No contact_ids found in zoominfo.contact_search — nothing to enrich")
            return

        success_count = 0
        fail_count    = 0
        for cid in contact_ids:
            try:
                result = enrich_person(cid, args.outputFields, token)
                save_to_s3(s3, cid, result)
                success_count += 1
            except Exception as exc:
                logger.warning(f"Skipping personId={cid}: {exc}")
                fail_count += 1

        logger.info(
            f"Batch enrich complete: {success_count} succeeded, {fail_count} failed"
        )

    elif args.personId:
        result = enrich_person(args.personId, args.outputFields, token)
        save_to_s3(s3, args.personId, result)

    else:
        raise ValueError("Provide --personId or --fromContacts")

    logger.info("API ingest complete — starting RAW processing...")
    subprocess.run(
        ["python3", f"{ETL_BASE}/WS-DEV-RAW-DATA/nb_zoominfo_contact_enrich_raw.py"],
        check=True
    )
    logger.info("RAW processing completed.")


if __name__ == "__main__":
    main()
