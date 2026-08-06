#!/usr/bin/env python3
"""
Trademo Company Profile — Raw Layer
WS-DEV-RAW-DATA/nb_trademo_company_profile_raw.py

Reads company profile JSON files from S3 bronze layer, selects and renames
fields to snake_case, and writes a Delta table to the bronze layer raw path.

Chains to: nb_trademo_company_profile_silver.py
"""

import os
import sys
import subprocess
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
ETL_BASE = "/opt/.debug/kg_v25_enterprise/etl"

AWS_ACCESS_KEY_ID     = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "")

S3_INPUT  = "s3a://goglo-bronze-layer/trademo/company-profile/*.json"
S3_OUTPUT = "s3a://goglo-bronze-layer/trademo/trademo-etl-company-profile-raw"

NEXT_SCRIPT = os.path.join(
    ETL_BASE,
    "WS-DEV-SILVER-DATA",
    "nb_trademo_company_profile_silver.py",
)

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
        .appName("nb_trademo_company_profile_raw")
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
        # S3A credentials
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        # S3A timeout / retry configs
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
def run(spark: SparkSession) -> None:
    log.info("Reading company profile JSON from %s", S3_INPUT)

    raw = spark.read.option("multiLine", "true").json(S3_INPUT)

    selected = raw.select(
        F.col("companyID").alias("company_id"),
        F.col("companyName").alias("company_name"),
        F.col("country"),
        F.col("state"),
        F.col("city"),
        F.col("zipCode").alias("zip_code"),
        F.col("addressList").alias("address"),
        F.col("phone"),
        F.col("website"),
        F.col("companyType").alias("company_type"),
        F.col("totalShipmentCount").alias("total_shipment_count"),
        F.col("totalImportCount").alias("total_import_count"),
        F.col("totalExportCount").alias("total_export_count"),
        F.col("tradingPartnerCount").alias("trading_partner_count"),
        F.col("tradeHealthScore").cast("double").alias("trade_health_score"),
        F.col("supplierRiskScore").cast("double").alias("supplier_risk_score"),
        F.col("importVolume").alias("import_volume"),
        F.col("exportVolume").alias("export_volume"),
        F.to_json(F.col("stockTickers")).alias("stock_tickers_json"),
        F.to_json(F.col("topTradingPartners")).alias("top_trading_partners_json"),
        F.current_timestamp().alias("ingested_at"),
    )

    record_count = selected.count()
    log.info("Records to write: %d", record_count)

    (
        selected.write
        .format("delta")
        .mode("overwrite")
        .save(S3_OUTPUT)
    )
    log.info("Delta write complete -> %s", S3_OUTPUT)


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
    log.info("nb_trademo_company_profile_raw.py — done.")


if __name__ == "__main__":
    main()
