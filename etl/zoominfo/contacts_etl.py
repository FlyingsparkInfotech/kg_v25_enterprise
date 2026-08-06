"""
ZoomInfo Contact Search ETL
API → zoominfo.contacts (PostgreSQL)

Usage:
  python3 zoominfo/contacts_etl.py --country India --managementLevel "C-Level,VP,Director"
  python3 zoominfo/contacts_etl.py --companyId 1160006778
  python3 zoominfo/contacts_etl.py --jobTitle "Chief Procurement Officer" --country "United Arab Emirates"

Run from Mac with SSH tunnel:
  ssh -N -L 5433:localhost:5432 powercozmo@100.93.186.72
  PG_PORT=5433 python3 zoominfo/contacts_etl.py ...
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import requests
import argparse
import time
import logging
import math
import psycopg2.extras
from datetime import datetime
from db import get_conn, upsert_watermark
from config import ZOOMINFO

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')
log = logging.getLogger(__name__)

FEED = "zoominfo_contacts"


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
        raise Exception(f"No JWT in ZI response: {resp.text[:200]}")
    log.info("ZoomInfo token acquired")
    return token


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--companyId",       default=None)
    p.add_argument("--companyName",     default=None)
    p.add_argument("--jobTitle",        default=None)
    p.add_argument("--jobFunction",     default=None)
    p.add_argument("--department",      default=None)
    p.add_argument("--managementLevel", default=None, help="e.g. 'C-Level,VP,Director'")
    p.add_argument("--country",         default=None)
    p.add_argument("--contactAccuracyScoreMin", default=None)
    p.add_argument("--requiredFields",  default=None, help="e.g. 'email,phone'")
    p.add_argument("--rpp",     type=int, default=25)
    return p.parse_args()


def build_payload(args, page):
    payload = {"rpp": args.rpp, "page": page}
    def add(k, v):
        if v is not None and v != "":
            payload[k] = v
    add("companyId",              args.companyId)
    add("companyName",            args.companyName)
    add("jobTitle",               args.jobTitle)
    add("jobFunction",            args.jobFunction)
    add("department",             args.department)
    add("managementLevel",        args.managementLevel)
    add("country",                args.country)
    add("contactAccuracyScoreMin",args.contactAccuracyScoreMin)
    add("requiredFields",         args.requiredFields)
    return payload


def fetch_page(token, payload):
    resp = requests.post(
        ZOOMINFO["contact_url"],
        json=payload,
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()


def safe_dt(val):
    if not val:
        return None
    try:
        return datetime.strptime(val[:19], "%Y-%m-%dT%H:%M:%S")
    except Exception:
        return None


def upsert_contacts(conn, records):
    # Writes to zoominfo.contact_search — the table the KG pipeline reads from
    sql = """
        INSERT INTO zoominfo.contact_search (
            contact_id, first_name, middle_name, last_name,
            job_title, contact_accuracy_score, valid_date, last_updated_date,
            has_email, has_supplemental_email, has_direct_phone, has_mobile_phone,
            has_company_industry, has_company_phone, has_company_street,
            has_company_state, has_company_zip_code, has_company_country,
            has_company_revenue, has_company_employee_count,
            direct_phone_do_not_call, mobile_phone_do_not_call,
            company_id, company_name,
            ingested_at, created_on, created_by, modified_on, modified_by
        ) VALUES %s
        ON CONFLICT (contact_id) DO UPDATE SET
            job_title              = EXCLUDED.job_title,
            contact_accuracy_score = EXCLUDED.contact_accuracy_score,
            has_email              = EXCLUDED.has_email,
            has_direct_phone       = EXCLUDED.has_direct_phone,
            company_id             = EXCLUDED.company_id,
            company_name           = EXCLUDED.company_name,
            last_updated_date      = EXCLUDED.last_updated_date,
            modified_on            = NOW()
    """
    now = datetime.utcnow()
    rows = []
    for r in records:
        rows.append((
            r.get("id"),
            r.get("firstName"),
            r.get("middleName"),
            r.get("lastName"),
            r.get("jobTitle"),
            r.get("contactAccuracyScore"),
            safe_dt(r.get("validDate")),
            safe_dt(r.get("lastUpdatedDate")),
            bool(r.get("hasEmail")),
            False,  # has_supplemental_email
            bool(r.get("hasDirectPhone")),
            bool(r.get("hasMobilePhone")),
            bool(r.get("companyIndustry")),
            False,  # has_company_phone
            False,  # has_company_street
            bool(r.get("state")),
            False,  # has_company_zip_code
            bool(r.get("country")),
            bool(r.get("revenue")),
            bool(r.get("employeeCount")),
            bool(r.get("directPhoneDoNotCall")),
            bool(r.get("mobilePhoneDoNotCall")),
            r.get("companyId"),
            r.get("companyName"),
            now, now, "etl", now, "etl",
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
        page  = 1
        total = 0

        # Fetch first page to get total
        log.info("Fetching page 1...")
        first = fetch_page(token, build_payload(args, 1))
        max_results = first.get("maxResults", 0)
        total_pages = math.ceil(max_results / args.rpp) if max_results else 1
        log.info(f"Total available: {max_results} | Pages: {total_pages}")

        records = first.get("data", [])
        if records:
            n = upsert_contacts(conn, records)
            total += n
            log.info(f"  Page 1: {n} contacts (total: {total})")

        for page in range(2, total_pages + 1):
            log.info(f"Fetching page {page}/{total_pages}...")
            time.sleep(0.5)
            resp = fetch_page(token, build_payload(args, page))
            records = resp.get("data", [])
            if not records:
                log.info(f"  Page {page}: empty — stopping")
                break
            n = upsert_contacts(conn, records)
            total += n
            log.info(f"  Page {page}: {n} contacts (total: {total})")

        upsert_watermark(conn, FEED, "success", total)
        log.info(f"Completed. Total contacts: {total}")

    except Exception as e:
        log.exception("ZoomInfo contacts ETL failed")
        upsert_watermark(conn, FEED, "failed", 0, str(e))
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
