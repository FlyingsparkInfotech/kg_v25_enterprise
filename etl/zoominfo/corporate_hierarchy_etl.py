"""
ZoomInfo Corporate Hierarchy ETL
API → zoominfo.corporate_hierarchy (PostgreSQL)

Fetches parent/subsidiary relationships for companies.
Used for:
  - Scenario X  (HQ Identified, Subsidiary Buying)
  - Scenario LL (Parent Company Shield on RFQ)
  - Scenario OO (Consultant / Sourcing Agent vs End Buyer)
  - Identity resolution: which entity is actually placing the order?

Usage:
  python3 zoominfo/corporate_hierarchy_etl.py \
      --companyName "Infosys Limited"

  python3 zoominfo/corporate_hierarchy_etl.py --companyId 1160006778

  # Batch from ZoomInfo contact_search companies
  python3 zoominfo/corporate_hierarchy_etl.py --fromContacts --limit 100
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import requests
import argparse
import time
import logging
import json
import psycopg2.extras
from datetime import datetime
from db import get_conn, upsert_watermark
from config import ZOOMINFO

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')
log = logging.getLogger(__name__)

FEED      = "zoominfo_corporate_hierarchy"
HIER_URL  = "https://api.zoominfo.com/enrich/corporatehierarchy"


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
        raise Exception(f"No JWT token: {resp.text[:200]}")
    log.info("ZoomInfo token acquired")
    return token


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--companyName",  default=None)
    p.add_argument("--companyId",    type=int, default=None)
    p.add_argument("--fromContacts", action="store_true",
                   help="Fetch hierarchy for all companies in zoominfo.contact_search")
    p.add_argument("--limit",        type=int, default=0)
    return p.parse_args()


def fetch_hierarchy(token, company_name=None, company_id=None):
    match_input = {}
    if company_id:
        match_input["companyId"] = company_id
    elif company_name:
        match_input["companyName"] = company_name
    else:
        raise ValueError("Need companyName or companyId")

    payload = {
        "matchCompanyInput": [match_input],
        "outputFields":      ["parentage", "familyTree", "companyId"],
    }
    resp = requests.post(
        HIER_URL,
        json=payload,
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
        timeout=60,
    )
    resp.raise_for_status()
    result = resp.json()
    if not result.get("success"):
        log.debug(f"Non-success for {company_name or company_id}: {result}")
        return None
    items = (result.get("data") or {}).get("result", [])
    return items[0].get("data") if items else None


def _flatten_tree(root_id, node, parent_id, level, rows):
    """Recursively flatten familyTree into flat rows."""
    node_id   = int(node.get("companyId", 0) or 0)
    node_name = str(node.get("companyName", "") or "")
    city      = str(node.get("city", "") or "")
    state     = str(node.get("state", "") or "")
    sub_type  = node.get("subUnitType") or {}
    sub_id    = int(sub_type.get("id", 0) or 0) if isinstance(sub_type, dict) else 0
    sub_desc  = str(sub_type.get("description", "") or "") if isinstance(sub_type, dict) else ""

    node_type = "root" if level == 0 else ("parent" if parent_id == 0 else "subsidiary")
    rows.append({
        "root_company_id": root_id,
        "node_company_id": node_id,
        "parent_company_id": parent_id,
        "level":           level,
        "node_type":       node_type,
        "node_name":       node_name,
        "city":            city,
        "state":           state,
        "sub_unit_type_id":   sub_id,
        "sub_unit_type_desc": sub_desc,
    })
    for child in (node.get("familyNodes") or []):
        _flatten_tree(root_id, child, node_id, level + 1, rows)


def upsert_hierarchy(conn, data, query_name):
    now   = datetime.utcnow().isoformat()
    rows  = []

    # Extract root company ID
    root_id = int(data.get("companyId", 0) or 0)

    # Parentage
    parentage = data.get("parentage") or {}
    if parentage:
        rows.append({
            "root_company_id":   root_id,
            "node_company_id":   root_id,
            "parent_company_id": int(parentage.get("parentCompanyId", 0) or 0),
            "level":             -1,
            "node_type":         "parentage",
            "node_name":         str(parentage.get("parentCompanyName", "") or ""),
            "city": "", "state": "", "sub_unit_type_id": 0, "sub_unit_type_desc": "",
        })

    # Family tree
    tree = data.get("familyTree") or {}
    for node in (tree.get("familyNodes") or []):
        _flatten_tree(root_id, node, 0, 0, rows)

    inserted = 0
    with conn.cursor() as cur:
        for r in rows:
            cur.execute("""
                INSERT INTO zoominfo.corporate_hierarchy (
                    root_company_id, node_company_id, parent_company_id,
                    level, node_type, node_name, city, state,
                    sub_unit_type_id, sub_unit_type_desc,
                    match_status, ingested_at,
                    created_on, created_by, modified_on, modified_by
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
            """, (
                r["root_company_id"], r["node_company_id"], r["parent_company_id"],
                r["level"], r["node_type"], r["node_name"], r["city"], r["state"],
                r["sub_unit_type_id"], r["sub_unit_type_desc"],
                "matched", now,
                datetime.utcnow(), f"hier:{query_name[:30]}",
                datetime.utcnow(), f"hier:{query_name[:30]}",
            ))
            inserted += 1
    conn.commit()
    return inserted


def _load_contact_companies(conn, limit=0):
    sql = "SELECT DISTINCT company_id, company_name FROM zoominfo.contact_search WHERE company_id IS NOT NULL"
    if limit:
        sql += f" LIMIT {int(limit)}"
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql)
        return cur.fetchall()


def main():
    args  = parse_args()
    token = get_token()
    conn  = get_conn()
    total = 0
    errors = 0

    companies = []
    if args.fromContacts:
        log.info("Loading companies from zoominfo.contact_search...")
        rows = _load_contact_companies(conn, args.limit)
        companies = [(r["company_id"], r["company_name"]) for r in rows]
        log.info(f"  {len(companies)} companies to process")
    elif args.companyId:
        companies = [(args.companyId, "")]
    elif args.companyName:
        companies = [(None, args.companyName)]
    else:
        log.error("Provide --companyName, --companyId, or --fromContacts")
        sys.exit(1)

    log.info(f"Fetching corporate hierarchy for {len(companies)} company(ies)")

    try:
        for cid, cname in companies:
            try:
                data = fetch_hierarchy(token, company_name=cname or None, company_id=cid or None)
                if data:
                    n = upsert_hierarchy(conn, data, cname or str(cid))
                    log.info(f"  {cname or cid} → {n} hierarchy nodes stored")
                    total += n
                time.sleep(0.5)
            except Exception as e:
                log.error(f"  Error for {cname or cid}: {e}")
                errors += 1

        upsert_watermark(conn, FEED, "success", total)
        log.info(f"Done — {total} hierarchy nodes stored, {errors} errors")
    except Exception as e:
        upsert_watermark(conn, FEED, "error", total, str(e))
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
