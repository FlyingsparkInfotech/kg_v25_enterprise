#!/usr/bin/env python3
"""
Trademo Shipment Search — Raw Layer
WS-DEV-RAW-DATA/nb_trademo_shipment_search_raw.py

Reads all paged shipment JSON files from the timestamped S3 directories,
explodes the shipments[] array, selects and renames fields to snake_case,
and writes a Delta table (overwrite) to the bronze raw path.

Input glob : s3a://goglo-bronze-layer/trademo/shipments/**/*.json
Output     : s3a://goglo-bronze-layer/trademo/trademo-etl-shipment-search-raw
Chains to  : nb_trademo_shipment_search_silver.py
"""

import os
import sys
import subprocess
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, ArrayType
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
ETL_BASE = "/opt/.debug/kg_v25_enterprise/etl"

AWS_ACCESS_KEY_ID     = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "")

S3_INPUT  = "s3a://goglo-bronze-layer/trademo/shipments/**/*.json"
S3_OUTPUT = "s3a://goglo-bronze-layer/trademo/trademo-etl-shipment-search-raw"

NEXT_SCRIPT = os.path.join(
    ETL_BASE,
    "WS-DEV-SILVER-DATA",
    "nb_trademo_shipment_search_silver.py",
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
        .appName("nb_trademo_shipment_search_raw")
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
        # Recursive glob support for nested timestamp directories
        .config("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", "true")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# ETL
# ---------------------------------------------------------------------------
def run(spark: SparkSession) -> None:
    log.info("Reading shipment JSON pages from %s", S3_INPUT)

    # multiLine because each page file is a single JSON object
    raw = spark.read.option("multiLine", "true").json(S3_INPUT)

    # Explode the shipments array — each element becomes a row
    exploded = raw.withColumn("shipment", F.explode(F.col("shipments")))
    s = F.col("shipment")

    selected = exploded.select(
        s["shipmentId"].alias("shipment_id"),
        s["shipmentDate"].alias("shipment_date"),
        s["shipperName"].alias("shipper_name"),
        s["shipperId"].alias("shipper_id"),
        s["shipperCountryName"].alias("shipper_country_name"),
        s["shipperAddress"].alias("shipper_address"),
        s["consigneeName"].alias("consignee_name"),
        s["consigneeId"].alias("consignee_id"),
        s["consigneeCountryName"].alias("consignee_country_name"),
        s["consigneeAddress"].alias("consignee_address"),
        s["portOfLading"].alias("port_of_lading"),
        s["portOfUnlading"].alias("port_of_unlading"),
        F.to_json(s["hsCodes"]).alias("hs_codes_json"),
        s["productDescription"].alias("product_description"),
        s["quantity"].alias("quantity"),
        s["quantityUnit"].alias("quantity_unit"),
        s["weight"].alias("weight"),
        s["weightUnit"].alias("weight_unit"),
        s["value"].alias("value"),
        s["valueCurrency"].alias("value_currency"),
        s["billOfLadingNumber"].alias("bill_of_lading_number"),
        F.current_timestamp().alias("ingested_at"),
    )

    record_count = selected.count()
    log.info("Shipment records to write: %d", record_count)

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
    log.info("nb_trademo_shipment_search_raw.py — done.")


if __name__ == "__main__":
    main()
