"""
ETL Orchestrator — runs all feeds in dependency order

Usage (run on server or from Mac with SSH tunnel):
  python3 run_all.py

Individual feeds:
  python3 run_all.py --feeds trademo_bsl zoominfo_contacts

Skip migration:
  python3 run_all.py --no-migrate
"""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import argparse
import subprocess
import logging
from db import get_conn

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')
log = logging.getLogger(__name__)

PYTHON = sys.executable

FEEDS = {
    "migrate"            : ["python3", "migrate_from_remote.py"],

    # Trademo
    "trademo_bsl"        : [PYTHON, "trademo/bsl_etl.py",
                            "--fromDate", "2022-01-01", "--toDate", "2026-08-01",
                            "--companyRole", "Supplier",
                            "--hsCodes", "7202", "7201", "7204", "72", "73"],
    "trademo_shipments"  : [PYTHON, "trademo/shipments_etl.py",
                            "--fromDate", "2022-01-01", "--toDate", "2026-08-01"],
    "trademo_company"    : [PYTHON, "trademo/company_etl.py",
                            "--fromDate", "2022-01-01", "--toDate", "2026-08-01"],
    "trademo_rel_health" : [PYTHON, "trademo/relationship_health_etl.py",
                            "--fromBSL",
                            "--fromDate", "2024-01-01", "--toDate", "2026-08-01"],
    # hs_classifier and company_matcher need input files -- run manually:
    # python3 trademo/hs_classifier_etl.py --batchFile products.txt --countryOfClassification IN --tradeDirection Import
    # python3 trademo/company_matcher_etl.py --batchFile companies.txt

    # ZoomInfo
    "zoominfo_contacts"  : [PYTHON, "zoominfo/contacts_etl.py",
                            "--managementLevel", "C-Level,VP,Director",
                            "--requiredFields", "email"],
    "zoominfo_enrich"    : [PYTHON, "zoominfo/enrich_etl.py",
                            "--minAccuracyScore", "70"],
    "zoominfo_intent"    : [PYTHON, "zoominfo/intent_etl.py",
                            "--topics", "Steel", "Metal", "Import Export",
                            "Supply Chain", "Procurement"],
    "zoominfo_news"      : [PYTHON, "zoominfo/news_etl.py",
                            "--pageDateMin", "2026-07-01", "--pageDateMax", "2026-08-01",
                            "--categories",
                            "MERGER_OR_ACQUISITION", "LEADERSHIP_CHANGE",
                            "NEW_OFFICE_OR_EXPANSION", "PARTNERSHIP_OR_ALLIANCE",
                            "FUNDING_OR_INVESTMENT"],
    "zoominfo_scoop"     : [PYTHON, "zoominfo/scoop_etl.py",
                            "--publishedStartDate", "2026-07-01",
                            "--publishedEndDate",   "2026-08-01",
                            "--scoopType",  "leadership,expansion,partnerships,awards"],
    "zoominfo_hierarchy" : [PYTHON, "zoominfo/corporate_hierarchy_etl.py",
                            "--fromContacts", "--limit", "200"],
}

ORDER = [
    "migrate",
    "trademo_bsl", "trademo_shipments", "trademo_company", "trademo_rel_health",
    "zoominfo_contacts", "zoominfo_enrich", "zoominfo_intent",
    "zoominfo_news", "zoominfo_scoop", "zoominfo_hierarchy",
]


def run_feed(name, cmd):
    log.info(f"\n{'='*50}")
    log.info(f"Running: {name}")
    log.info(f"{'='*50}")
    result = subprocess.run(cmd, cwd=os.path.dirname(__file__))
    if result.returncode != 0:
        log.error(f"Feed '{name}' FAILED (exit {result.returncode})")
        return False
    log.info(f"Feed '{name}' SUCCEEDED")
    return True


def watermark_summary(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT feed_name, status, records_loaded, last_success_at
            FROM public.etl_watermarks ORDER BY feed_name
        """)
        rows = cur.fetchall()
    log.info("\n=== ETL Watermarks ===")
    for row in rows:
        log.info(f"  {row[0]:30s} {row[1]:10s} rows={row[2] or 0:6d}  last={row[3]}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--feeds",      nargs="*", default=None, help="Feed names to run")
    p.add_argument("--no-migrate", action="store_true",  help="Skip data migration step")
    args = p.parse_args()

    feeds_to_run = args.feeds if args.feeds else ORDER
    if args.no_migrate and "migrate" in feeds_to_run:
        feeds_to_run = [f for f in feeds_to_run if f != "migrate"]

    results = {}
    for feed in feeds_to_run:
        if feed not in FEEDS:
            log.warning(f"Unknown feed: {feed}")
            continue
        results[feed] = run_feed(feed, FEEDS[feed])

    conn = get_conn()
    watermark_summary(conn)
    conn.close()

    failed = [f for f, ok in results.items() if not ok]
    if failed:
        log.error(f"\nFailed feeds: {failed}")
        sys.exit(1)
    else:
        log.info("\nAll feeds completed successfully.")


if __name__ == "__main__":
    main()
