#!/usr/bin/env python3
"""
Trademo Company Profile — Silver Layer
WS-DEV-SILVER-DATA/nb_trademo_company_profile_silver.py

Reads the raw Delta table, deduplicates by keeping the latest ingested_at
per company_id, then merges into the silver Delta table.

Merge key : company_id
Chains to : nb_trademo_company_profile_gold.py
"""

import os
import sys
import subprocess
import logging

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
ETL_BASE = "/opt/.debug/kg_v25_enterprise/etl"

AWS_ACCESS_KEY_ID     = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "")

S3_RAW    = "s3a://goglo-bronze-layer/trademo/trademo-etl-company-profile-raw"
S3_SILVER = "s3a://goglo-silver-layer/trademo/trademo-etl-company-profile-silver"

NEXT_SCRIPT = os.path.join(
    ETL_BASE,
    "WS-DEV-GOLD-DATA",
    "nb_trademo_company_profile_gold.py",
)

BUSINESS_FIELDS = [
    "company_name",
    "country",
    "state",
    "city",
    "zip_code",
    "address",
    "phone",
    "website",
    "company_type",
    "total_shipment_count",
    "total_import_count",
    "total_export_count",
    "trading_partner_count",
    "trade_health_score",
    "supplier_risk_score",
    "import_volume",
    "export_volume",
    "stock_tickers_json",
    "top_trading_partners_json",
    "ingested_at",
]

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------
def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("nb_trademo_company_profile_silver")
        .config(
            "spark.jars.packages",
            ",".join([
                "io.delta:delta-spark_2.12:3.3.0",
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.amazonaws:aws-java-sdk-bundle:1.12.262",
            ]),
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.connection.timeout", "600000")
        .config("spark.hadoop.fs.s3a.socket.timeout", "600000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "600000")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "10")
        .config("spark.hadoop.fs.s3a.retry.limit", "10")
        .config("spark.hadoop.fs.s3a.multipart.size", "104857600")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# ETL
# ---------------------------------------------------------------------------
def dedup(df):
    """Keep the row with the latest ingested_at per company_id."""
    w = Window.partitionBy("company_id").orderBy(F.col("ingested_at").desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )


def run(spark: SparkSession) -> None:
    log.info("Reading raw Delta from %s", S3_RAW)
    raw_df = spark.read.format("delta").load(S3_RAW)
    deduped = dedup(raw_df)

    # Build merge set dict for all business fields
    merge_set = {field: f"source.{field}" for field in BUSINESS_FIELDS}

    if DeltaTable.isDeltaTable(spark, S3_SILVER):
        log.info("Silver table exists — running MERGE on company_id.")
        silver_tbl = DeltaTable.forPath(spark, S3_SILVER)
        (
            silver_tbl.alias("target")
            .merge(deduped.alias("source"), "target.company_id = source.company_id")
            .whenMatchedUpdate(set=merge_set)
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        log.info("Silver table does not exist — initial write.")
        deduped.write.format("delta").mode("overwrite").save(S3_SILVER)

    log.info("Silver layer complete -> %s", S3_SILVER)


# ---------------------------------------------------------------------------
# Chain
# ---------------------------------------------------------------------------
def chain_next() -> None:
    if not os.path.exists(NEXT_SCRIPT):
        log.warning("Next script not found at %s — skipping chain.", NEXT_SCRIPT)
        return
    log.info("Chaining to %s", NEXT_SCRIPT)
    result = subprocess.run([sys.executable, NEXT_SCRIPT], check=False)
    if result.returncode != 0:
        log.error("Next script exited with code %d", result.returncode)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    spark = build_spark()
    try:
        run(spark)
    finally:
        spark.stop()

    chain_next()
    log.info("nb_trademo_company_profile_silver.py — done.")


if __name__ == "__main__":
    main()
