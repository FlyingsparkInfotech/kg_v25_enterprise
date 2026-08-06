"""
Trademo HS Classifier ETL
API → raw.trademo_hs_classifier (PostgreSQL)

Classifies product descriptions into HS codes using Trademo's AI classifier.
Used to bridge buyer keyword searches on GoGlo → HS codes → matching suppliers.

Usage:
  python3 trademo/hs_classifier_etl.py \
      --productTitle "Pipe Fittings" \
      --productDescription "Fittings of Iron or Steel for pipelines" \
      --countryOfClassification IN \
      --tradeDirection Import

  # Batch mode from file (one product per line: title|description):
  python3 trademo/hs_classifier_etl.py --batchFile products.txt \
      --countryOfClassification IN --tradeDirection Import
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import requests
import argparse
import time
import logging
import hashlib
import json
import psycopg2.extras
from datetime import datetime
from db import get_conn, upsert_watermark
from config import TRADEMO

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')
log = logging.getLogger(__name__)

FEED        = "trademo_hs_classifier"
CLASSIFIER_URL = "http://classifierapi.trademo.com/api/v1/suggestionBasedHsClassifier"


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
    p.add_argument("--productTitle",            default=None)
    p.add_argument("--productDescription",      default=None)
    p.add_argument("--countryOfClassification", default="IN")
    p.add_argument("--tradeDirection",          default="Import",
                   choices=["Import", "Export"])
    p.add_argument("--skuId",                   default="")
    p.add_argument("--batchFile",               default=None,
                   help="File with lines: title|description")
    return p.parse_args()


def classify_product(session_id, title, description, country, direction, sku_id=""):
    payload = {
        "sku_id":                   sku_id,
        "productTitle":             title,
        "productDescription":       description,
        "countryOfClassification":  country,
        "tradeDirection":           direction,
    }
    resp = requests.post(
        CLASSIFIER_URL,
        headers={
            "content-type":       "application/json",
            "sessionid":          session_id,
            "Auth-Custom-Header": TRADEMO["auth_header"],
        },
        data=json.dumps(payload),
        timeout=60,
    )
    if resp.status_code == 400:
        log.warning(f"400 for '{title}' — skipping")
        return None
    resp.raise_for_status()
    data = resp.json()
    if data.get("status") != "success":
        log.warning(f"Non-success status for '{title}': {data.get('status')}")
        return None
    if not data.get("mostSuitableHs") or not data.get("dutiableHsCode"):
        log.warning(f"Empty classification for '{title}'")
        return None
    return data


def _make_hash(title, description, country, direction):
    raw = f"{title}|{description}|{country}|{direction}".lower()
    return hashlib.sha256(raw.encode()).hexdigest()


def upsert_classification(conn, title, description, country, direction, sku_id, data):
    now  = datetime.utcnow()
    best = data.get("mostSuitableHs", {})
    alts = data.get("dutiableHsCode", [])
    alt1 = alts[0] if len(alts) > 0 else {}
    hash_key = _make_hash(title, description, country, direction)
    txn_id   = data.get("transactionId", "")

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO raw.trademo_hs_classifier (
                hash_key, trademo_transaction_id, classification_timestamp,
                sku_id,
                most_suitable_hs_code, most_suitable_hs_description,
                most_suitable_hs_justification,
                dutiable_hs_code, dutiable_hs_description,
                dutiable_hs_justification, dutiable_hs_code_description,
                justification_note,
                created_on, created_by, modified_on, modified_by
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s,
                %s,
                %s, %s, %s, %s
            )
            ON CONFLICT (hash_key) DO UPDATE SET
                trademo_transaction_id        = EXCLUDED.trademo_transaction_id,
                classification_timestamp      = EXCLUDED.classification_timestamp,
                most_suitable_hs_code         = EXCLUDED.most_suitable_hs_code,
                most_suitable_hs_description  = EXCLUDED.most_suitable_hs_description,
                most_suitable_hs_justification= EXCLUDED.most_suitable_hs_justification,
                dutiable_hs_code              = EXCLUDED.dutiable_hs_code,
                dutiable_hs_description       = EXCLUDED.dutiable_hs_description,
                dutiable_hs_justification     = EXCLUDED.dutiable_hs_justification,
                modified_on                   = EXCLUDED.modified_on,
                modified_by                   = EXCLUDED.modified_by
        """, (
            hash_key, txn_id, now,
            sku_id or "",
            best.get("hsCode", ""),    best.get("description", ""),
            best.get("justification", ""),
            alt1.get("hsCode", ""),    alt1.get("description", ""),
            alt1.get("justification", ""), alt1.get("codeDescription", ""),
            data.get("justificationNote", ""),
            now, "etl_hs_classifier", now, "etl_hs_classifier",
        ))
    conn.commit()
    return best.get("hsCode", "")


def main():
    args   = parse_args()
    token  = get_session_token()
    conn   = get_conn()
    total  = 0
    errors = 0

    # Build product list
    products = []
    if args.batchFile:
        with open(args.batchFile) as f:
            for line in f:
                line = line.strip()
                if "|" in line:
                    title, desc = line.split("|", 1)
                    products.append((title.strip(), desc.strip()))
    elif args.productTitle and args.productDescription:
        products = [(args.productTitle, args.productDescription)]
    else:
        log.error("Provide --productTitle + --productDescription OR --batchFile")
        sys.exit(1)

    log.info(f"Classifying {len(products)} product(s) — country={args.countryOfClassification} direction={args.tradeDirection}")

    try:
        for title, description in products:
            try:
                data = classify_product(
                    token, title, description,
                    args.countryOfClassification, args.tradeDirection,
                    args.skuId or "",
                )
                if data:
                    hs = upsert_classification(
                        conn, title, description,
                        args.countryOfClassification, args.tradeDirection,
                        args.skuId or "", data,
                    )
                    log.info(f"  '{title}' → HS {hs}")
                    total += 1
                time.sleep(0.3)
            except Exception as e:
                log.error(f"  Error classifying '{title}': {e}")
                errors += 1

        upsert_watermark(conn, FEED, "success", total)
        log.info(f"Done — {total} classified, {errors} errors")
    except Exception as e:
        upsert_watermark(conn, FEED, "error", total, str(e))
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
