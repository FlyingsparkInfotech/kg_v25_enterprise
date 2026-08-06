"""
ZoomInfo Contact Enrichment ETL
Reads contact_ids from zoominfo.contacts table → calls enrich API → stores results

Usage:
  python3 zoominfo/enrich_etl.py --batchSize 50 --minAccuracyScore 75
  python3 zoominfo/enrich_etl.py --personIds 1582097820 999123
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import requests
import argparse
import time
import logging
import json
import psycopg2.extras
from db import get_conn, upsert_watermark
from config import ZOOMINFO

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')
log = logging.getLogger(__name__)

FEED = "zoominfo_enrich"


def get_token():
    resp = requests.post(
        ZOOMINFO["auth_url"],
        json={"username": ZOOMINFO["username"], "password": ZOOMINFO["password"]},
        headers={"Content-Type": "application/json"},
        timeout=30,
    )
    resp.raise_for_status()
    token = resp.json().get("jwt")
    if not token:
        raise Exception(f"No JWT: {resp.text[:200]}")
    log.info("ZoomInfo token acquired")
    return token


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--personIds",       nargs="*", default=[], help="Person IDs to enrich")
    p.add_argument("--batchSize",       type=int, default=25)
    p.add_argument("--minAccuracyScore",type=int, default=0)
    p.add_argument("--outputFields",    nargs="*",
                   default=["email", "mobilePhone", "phone"])
    return p.parse_args()


def get_unenriched_contacts(conn, min_score, limit=5000):
    sql = """
        SELECT c.contact_id FROM zoominfo.contacts c
        LEFT JOIN zoominfo.contact_enrichment e ON c.contact_id = e.contact_id
        WHERE e.contact_id IS NULL
          AND (c.contact_accuracy_score IS NULL OR c.contact_accuracy_score >= %s)
        LIMIT %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (min_score, limit))
        return [row[0] for row in cur.fetchall()]


def enrich_batch(token, person_ids, output_fields):
    payload = {
        "outputFields": output_fields,
        "matchPersonInput": [{"personId": int(pid)} for pid in person_ids],
    }
    resp = requests.post(
        ZOOMINFO["enrich_url"],
        json=payload,
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()


def upsert_enriched(conn, results):
    # Writes to zoominfo.contact_enrich — the table the KG pipeline reads from
    # contact_key is a serial PK, id = ZoomInfo person ID, phone = direct phone
    sql = """
        INSERT INTO zoominfo.contact_enrich (
            id, person_id, email, mobile_phone, phone,
            mobile_dnc, direct_dnc,
            created_on, created_by, modified_on, modified_by
        ) VALUES %s
        ON CONFLICT DO NOTHING
    """
    from datetime import datetime
    now = datetime.utcnow()
    rows = []
    for r in results:
        data = r.get("data") or {}
        pid = r.get("id") or r.get("personId")
        rows.append((
            pid, pid,
            data.get("email"),
            data.get("mobilePhone"),
            data.get("phone"),
            bool(data.get("mobilePhoneDoNotCall")),
            bool(data.get("directPhoneDoNotCall")),
            now, "etl", now, "etl",
        ))
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows)
    conn.commit()
    return len(rows)


def main():
    args = parse_args()
    conn = get_conn()
    upsert_watermark(conn, FEED, "running")

    try:
        token = get_token()

        # Resolve person IDs
        if args.personIds:
            person_ids = [int(x) for x in args.personIds]
        else:
            person_ids = get_unenriched_contacts(conn, args.minAccuracyScore)
            log.info(f"Found {len(person_ids)} unenriched contacts")

        total = 0
        for i in range(0, len(person_ids), args.batchSize):
            batch = person_ids[i:i + args.batchSize]
            log.info(f"Enriching batch {i//args.batchSize + 1} ({len(batch)} contacts)...")
            try:
                resp = enrich_batch(token, batch, args.outputFields)
                results = (resp.get("data") or {}).get("result") or []
                n = upsert_enriched(conn, results)
                total += n
                log.info(f"  Enriched {n} contacts (total: {total})")
            except Exception as e:
                log.warning(f"  Batch failed: {e}")
            time.sleep(0.5)

        upsert_watermark(conn, FEED, "success", total)
        log.info(f"Completed. Total enriched: {total}")

    except Exception as e:
        log.exception("ZoomInfo enrich ETL failed")
        upsert_watermark(conn, FEED, "failed", 0, str(e))
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
