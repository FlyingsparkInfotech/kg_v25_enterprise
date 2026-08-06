"""
Trademo Relationship Health ETL
API → trademo.relationship_health (PostgreSQL)

Fetches Trademo's own health score for buyer-supplier relationships.
This is the most valuable Trademo signal for the KG switch lead engine —
it provides vendor-computed health/risk scores that augment (or replace)
our IsolationForest ML model in Stage 3 of the pipeline.

KG Usage:
  The KG switch_lead_engine.py reads from trademo.relationship_health
  in detect_stress() to get vendor health_score + health_status as
  the primary stress signal. ML (IsolationForest) is used as fallback.

Usage:
  # Single relationship
  python3 trademo/relationship_health_etl.py \
      --buyerId abc123 --supplierId def456 \
      --fromDate 2024-01-01 --toDate 2026-08-01

  # From existing TradeRelationships in Neo4j / BSL table
  python3 trademo/relationship_health_etl.py \
      --fromBSL --fromDate 2024-01-01 --toDate 2026-08-01

  --fromBSL reads buyer_id + supplier_id pairs from
  raw.trademo_buyer_supplier_list and fetches health for each.
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
from config import TRADEMO

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')
log = logging.getLogger(__name__)

FEED = "trademo_relationship_health"


def get_session_token():
    resp = requests.get(
        TRADEMO["token_url"],
        headers={"Auth-Custom-Header": TRADEMO["auth_header"]},
        timeout=30,
    )
    resp.raise_for_status()
    token = resp.json().get("token")
    if not token:
        raise Exception("No token returned from Trademo auth")
    log.info(f"Trademo token acquired: {token[:20]}...")
    return token


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--buyerId",     default=None)
    p.add_argument("--supplierId",  default=None)
    p.add_argument("--buyerName",   default=None)
    p.add_argument("--supplierName",default=None)
    p.add_argument("--fromDate",    default="2024-01-01")
    p.add_argument("--toDate",      default=datetime.utcnow().strftime("%Y-%m-%d"))
    p.add_argument("--fromBSL",     action="store_true",
                   help="Read buyer/supplier pairs from raw.trademo_buyer_supplier_list")
    p.add_argument("--limit",       type=int, default=0,
                   help="Max pairs to process in --fromBSL mode (0=all)")
    return p.parse_args()


def fetch_health(session_id, buyer_id=None, supplier_id=None,
                 buyer_name=None, supplier_name=None,
                 from_date="2024-01-01", to_date=None):
    """
    Trademo relationship health API.
    Accepts either IDs or names (IDs preferred for accuracy).
    """
    payload = {
        "tradeTimePeriod": {
            "fromDate": from_date,
            "toDate":   to_date or datetime.utcnow().strftime("%Y-%m-%d"),
        }
    }
    if buyer_id:
        payload["buyerId"]    = buyer_id
    elif buyer_name:
        payload["buyerName"]  = buyer_name
    if supplier_id:
        payload["supplierId"]   = supplier_id
    elif supplier_name:
        payload["supplierName"] = supplier_name

    resp = requests.post(
        TRADEMO["rel_health_url"],
        headers={
            "content-type":       "application/json",
            "sessionid":          session_id,
            "Auth-Custom-Header": TRADEMO["auth_header"],
        },
        data=json.dumps(payload),
        timeout=60,
    )
    if resp.status_code == 400:
        log.debug(f"400 for buyer={buyer_id or buyer_name} / supplier={supplier_id or supplier_name}")
        return None
    resp.raise_for_status()
    return resp.json()


def _parse_health(data: dict) -> dict:
    """Normalise API response fields — handles both flat and nested formats."""
    if not data:
        return {}
    # Try nested structure first, fall back to flat
    rel = data.get("relationshipHealth") or data.get("relationship") or data
    return {
        "health_score":       float(rel.get("healthScore", 0) or rel.get("health_score", 0) or 0),
        "shipment_count":     int(rel.get("shipmentCount", 0)  or rel.get("shipment_count", 0)  or 0),
        "last_shipment_date": rel.get("lastShipmentDate")      or rel.get("last_shipment_date"),
        "health_status":      str(rel.get("healthStatus", "")  or rel.get("status", "unknown")),
        "risk_flags":         json.dumps(rel.get("riskFlags", []) or rel.get("risk_flags", [])),
        "buyer_id":           str(rel.get("buyerId", "")        or rel.get("buyer_id", "")),
        "supplier_id":        str(rel.get("supplierId", "")     or rel.get("supplier_id", "")),
        "buyer_name":         str(rel.get("buyerName", "")      or rel.get("buyer_name", "")),
        "supplier_name":      str(rel.get("supplierName", "")   or rel.get("supplier_name", "")),
    }


def upsert_health(conn, buyer_id, supplier_id, buyer_name, supplier_name, parsed, raw):
    now = datetime.utcnow()
    last_date = None
    if parsed.get("last_shipment_date"):
        try:
            last_date = datetime.strptime(str(parsed["last_shipment_date"])[:10], "%Y-%m-%d").date()
        except Exception:
            pass

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO trademo.relationship_health (
                buyer_id, supplier_id, buyer_name, supplier_name,
                health_score, shipment_count, last_shipment_date,
                health_status, risk_flags, raw_json, ingested_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (buyer_id, supplier_id) DO UPDATE SET
                buyer_name        = EXCLUDED.buyer_name,
                supplier_name     = EXCLUDED.supplier_name,
                health_score      = EXCLUDED.health_score,
                shipment_count    = EXCLUDED.shipment_count,
                last_shipment_date= EXCLUDED.last_shipment_date,
                health_status     = EXCLUDED.health_status,
                risk_flags        = EXCLUDED.risk_flags,
                raw_json          = EXCLUDED.raw_json,
                ingested_at       = EXCLUDED.ingested_at
        """, (
            buyer_id    or parsed.get("buyer_id", ""),
            supplier_id or parsed.get("supplier_id", ""),
            buyer_name  or parsed.get("buyer_name", ""),
            supplier_name or parsed.get("supplier_name", ""),
            parsed.get("health_score", 0),
            parsed.get("shipment_count", 0),
            last_date,
            parsed.get("health_status", "unknown"),
            parsed.get("risk_flags", "[]"),
            json.dumps(raw),
            now,
        ))
    conn.commit()


def _load_bsl_pairs(conn, limit=0):
    """Pull distinct buyer_id / supplier_id pairs from raw.trademo_buyer_supplier_list."""
    sql = """
        SELECT DISTINCT
            b.company_id AS buyer_id,    b.company_name AS buyer_name,
            s.company_id AS supplier_id, s.company_name AS supplier_name
        FROM raw.trademo_buyer_supplier_list b
        JOIN raw.trademo_buyer_supplier_list s
          ON b.company_role = 'Buyer' AND s.company_role = 'Supplier'
        WHERE b.company_id IS NOT NULL AND s.company_id IS NOT NULL
    """
    if limit:
        sql += f" LIMIT {int(limit)}"
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql)
        return cur.fetchall()


def main():
    args  = parse_args()
    token = get_session_token()
    conn  = get_conn()
    total = 0
    errors = 0

    # Build list of (buyer_id, supplier_id, buyer_name, supplier_name)
    pairs = []
    if args.fromBSL:
        log.info("Loading buyer-supplier pairs from raw.trademo_buyer_supplier_list...")
        rows = _load_bsl_pairs(conn, args.limit)
        pairs = [(r["buyer_id"], r["supplier_id"], r["buyer_name"], r["supplier_name"]) for r in rows]
        log.info(f"  {len(pairs)} pairs loaded")
    elif args.buyerId or args.buyerName:
        pairs = [(args.buyerId, args.supplierId, args.buyerName, args.supplierName)]
    else:
        log.error("Provide --buyerId/--supplierId OR --fromBSL")
        sys.exit(1)

    log.info(f"Fetching health for {len(pairs)} relationship(s) — {args.fromDate} to {args.toDate}")

    try:
        for buyer_id, supplier_id, buyer_name, supplier_name in pairs:
            try:
                raw = fetch_health(
                    token,
                    buyer_id=buyer_id,     supplier_id=supplier_id,
                    buyer_name=buyer_name, supplier_name=supplier_name,
                    from_date=args.fromDate, to_date=args.toDate,
                )
                if raw:
                    parsed = _parse_health(raw)
                    upsert_health(conn, buyer_id, supplier_id, buyer_name, supplier_name, parsed, raw)
                    log.info(f"  {buyer_name or buyer_id} ↔ {supplier_name or supplier_id}"
                             f" → score={parsed.get('health_score')} status={parsed.get('health_status')}")
                    total += 1
                time.sleep(0.3)
            except Exception as e:
                log.error(f"  Error for {buyer_id}/{supplier_id}: {e}")
                errors += 1

        upsert_watermark(conn, FEED, "success", total)
        log.info(f"Done — {total} relationships stored, {errors} errors")
    except Exception as e:
        upsert_watermark(conn, FEED, "error", total, str(e))
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
