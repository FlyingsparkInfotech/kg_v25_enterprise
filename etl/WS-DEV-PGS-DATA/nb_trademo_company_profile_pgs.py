#!/usr/bin/env python3
"""
Trademo Company Profile — Postgres Load
WS-DEV-PGS-DATA/nb_trademo_company_profile_pgs.py

Anti-joins the gold Delta table against raw.trademo_company_profile in
Postgres on cp_key, then JDBC-appends only new rows to that table.

Target table : raw.trademo_company_profile
JDBC driver  : org.postgresql:postgresql:42.6.0
"""

import os
import sys
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
ETL_BASE = "/opt/.debug/kg_v25_enterprise/etl"

AWS_ACCESS_KEY_ID     = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "")

S3_GOLD = "s3a://goglo-gold-layer/trademo/trademo-etl-company-profile-gold"

PGS_URL  = "jdbc:postgresql://localhost:5432/goglo_etl"
PGS_USER = "etl_user"
PGS_PASS = "EtlCozmo@2026!"
PGS_TABLE = "raw.trademo_company_profile"

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
        .appName("nb_trademo_company_profile_pgs")
        .config(
            "spark.jars.packages",
            ",".join([
                "io.delta:delta-spark_2.12:3.3.0",
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.amazonaws:aws-java-sdk-bundle:1.12.262",
                "org.postgresql:postgresql:42.6.0",
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
# JDBC helpers
# ---------------------------------------------------------------------------
def jdbc_props() -> dict:
    return {
        "user": PGS_USER,
        "password": PGS_PASS,
        "driver": "org.postgresql.Driver",
    }


def read_pgs_keys(spark: SparkSession):
    """Return a DataFrame of cp_key values already in Postgres."""
    return spark.read.jdbc(
        url=PGS_URL,
        table=f"(SELECT cp_key FROM {PGS_TABLE}) AS pgs_keys",
        properties=jdbc_props(),
    )


# ---------------------------------------------------------------------------
# ETL
# ---------------------------------------------------------------------------
def run(spark: SparkSession) -> None:
    log.info("Reading gold Delta from %s", S3_GOLD)
    gold_df = spark.read.format("delta").load(S3_GOLD)

    log.info("Reading existing cp_key values from %s", PGS_TABLE)
    try:
        existing_keys = read_pgs_keys(spark)
        new_df = gold_df.join(
            existing_keys,
            on="cp_key",
            how="left_anti",
        )
    except Exception as exc:
        # Table may not exist yet on first run
        log.warning("Could not read from Postgres (first run?): %s", exc)
        new_df = gold_df

    record_count = new_df.count()
    log.info("New records to append to Postgres: %d", record_count)

    if record_count == 0:
        log.info("No new records — Postgres table is up to date.")
        return

    (
        new_df.write
        .jdbc(
            url=PGS_URL,
            table=PGS_TABLE,
            mode="append",
            properties=jdbc_props(),
        )
    )
    log.info("JDBC append complete -> %s", PGS_TABLE)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    spark = build_spark()
    try:
        run(spark)
    finally:
        spark.stop()

    log.info("nb_trademo_company_profile_pgs.py — done.")


if __name__ == "__main__":
    main()
