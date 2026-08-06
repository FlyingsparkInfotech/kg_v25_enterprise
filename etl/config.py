# ETL Configuration — Trademo + ZoomInfo → goglo_etl PostgreSQL
# All credentials extracted from existing ETL code in etl-spark/

# ──────────────────────────────────────────────
# PostgreSQL (local on spark-dgx-dev)
# Run from Mac: open SSH tunnel first:
#   ssh -N -L 5433:localhost:5432 powercozmo@100.93.186.72
# Then set PG_HOST=localhost PG_PORT=5433
# ──────────────────────────────────────────────
import os

PG = {
    "host"    : os.getenv("PG_HOST", "localhost"),
    "port"    : int(os.getenv("PG_PORT", "5432")),
    "dbname"  : "goglo_etl",
    "user"    : "etl_user",
    "password": "EtlCozmo@2026!",
}

# ──────────────────────────────────────────────
# Trademo API
# ──────────────────────────────────────────────
TRADEMO = {
    "token_url"  : "https://trademo.com/trademo/api/generateAPIToken?auth_key=U3JpcmFtQHBvd2VyY296bW8uY29tOjY0OWJkYmRiMjhiZWVlNmY5MDJhMWZjZDg3OTZkMjlj",
    "auth_header": "cThadVkzbko2VkZLbzFhRGRmZ2hqRFNGRFNGREdHamhnaGpzZA==",
    "bsl_url"    : "https://trademo.com/api/v1/global_buyer_supplier_list",
    "shipment_url": "https://trademo.com/api/v2.0/shipment_search_api",
    "company_url" : "https://trademo.com/api/v2.0/global_buyer_supplier_company_profile",
    "hs_url"      : "https://trademo.com/api/v1/hs_classifier",
    "rel_health_url": "https://trademo.com/api/v1/global_buyer_supplier_relationship_health",
}

# ──────────────────────────────────────────────
# ZoomInfo API
# ──────────────────────────────────────────────
ZOOMINFO = {
    "auth_url"    : "https://api.zoominfo.com/authenticate",
    "contact_url" : "https://api.zoominfo.com/search/contact",
    "enrich_url"  : "https://api.zoominfo.com/enrich/contact",
    "intent_url"  : "https://api.zoominfo.com/search/intent",
    "news_url"    : "https://api.zoominfo.com/search/news",
    "scoop_url"   : "https://api.zoominfo.com/search/scoop",
    "username"    : "sriram@powercozmo.com",
    "password"    : "wewfox-3vanve-fecwuZ",
}
