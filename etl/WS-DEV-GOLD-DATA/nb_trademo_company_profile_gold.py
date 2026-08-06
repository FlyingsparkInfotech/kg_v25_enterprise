#!/usr/bin/env python3
"""
Trademo Company Profile — Gold Layer
WS-DEV-GOLD-DATA/nb_trademo_company_profile_gold.py

Assigns a gap-free sequential surrogate key (cp_key) to company profiles
that do not yet exist in the gold table (anti-join on company_id), then
appends them to the gold Delta table.

Surrogate key : cp_key  (Window.orderBy(company_id), sequential, gap-free)
Chains to     : nb_trademo_company_profile_pgs.py
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

S3_SILVER = "s3a://goglo-silver-layer/trademo/trademo-etl-company-profile-silver"
S3_GOLD   = "s3a://goglo-gold-layer/trademo/trademo-etl-company-profile-gold"

NEXT_SCRIPT = os.path.join(
    ETL_BASE,
    "WS-DEV-PGS-DATA",
    "nb_trademo_company_profile_pgs.py",
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
        .appName("nb_trademo_company_profile_gold")
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
def run(spark: SparkSession) -> None:
    log.info("Reading silver Delta from %s", S3_SILVER)
    silver_df = spark.read.format("delta").load(S3_SILVER)

    # Determine the current max cp_key in gold (0 if table does not exist yet)
    if DeltaTable.isDeltaTable(spark, S3_GOLD):
        gold_df = spark.read.format("delta").load(S3_GOLD)
        max_key_row = gold_df.agg(F.max("cp_key")).collect()[0]
        max_key = int(max_key_row[0]) if max_key_row[0] is not None else 0

        # Anti-join: keep only company_ids not already in gold
        new_df = silver_df.join(
            gold_df.select("company_id"),
            on="company_id",
            how="left_anti",
        )
    else:
        max_key = 0
        new_df = silver_df

    record_count = new_df.count()
    log.info("New records to add to gold: %d  (current max cp_key=%d)", record_count, max_key)

    if record_count == 0:
        log.info("No new records — gold layer is up to date.")
        return

    # Assign gap-free sequential surrogate key ordered by company_id
    w = Window.orderBy("company_id")
    new_with_key = new_df.withColumn(
        "cp_key",
        F.row_number().over(w) + max_key,
    )

    new_with_key.write.format("delta").mode("append").save(S3_GOLD)
    log.info("Gold layer append complete -> %s", S3_GOLD)


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
    log.info("nb_trademo_company_profile_gold.py — done.")


if __name__ == "__main__":
    main()
