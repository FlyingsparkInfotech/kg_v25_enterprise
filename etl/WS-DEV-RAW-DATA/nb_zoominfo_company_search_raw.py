#!/usr/bin/env python3
"""
ZoomInfo Company Search — Raw (Bronze → Bronze Delta)
Reads JSON envelopes from S3, explodes data[], writes Delta table.
Chains to nb_zoominfo_company_search_silver.py
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, current_timestamp, to_json,
)

# ---------------------------------------------------------------------------
# ENV / CREDENTIALS
# ---------------------------------------------------------------------------
AWS_ACCESS_KEY_ID     = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "")

ZI_USERNAME = os.environ.get("ZI_USERNAME", "sriram@powercozmo.com")
ZI_PASSWORD = os.environ.get("ZI_PASSWORD", "wewfox-3vanve-fecwuZ")

ETL_BASE = "/opt/.debug/kg_v25_enterprise/etl"

S3_INPUT  = "s3a://goglo-bronze-layer/zoominfo/company_search/*.json"
S3_OUTPUT = "s3a://goglo-bronze-layer/zoominfo/zoominfo-etl-company-search-raw"

# ---------------------------------------------------------------------------
# SPARK SESSION
# ---------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("zoominfo_company_search_raw")
    .config("spark.jars.packages",
            "io.delta:delta-spark_2.12:3.3.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # S3A credentials
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    # S3A timeouts
    .config("spark.hadoop.fs.s3a.connection.timeout", "600000")
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "600000")
    .config("spark.hadoop.fs.s3a.socket.send.buffer", "65536")
    .config("spark.hadoop.fs.s3a.socket.recv.buffer", "65536")
    .config("spark.hadoop.fs.s3a.attempts.maximum", "10")
    .config("spark.hadoop.fs.s3a.retry.limit", "10")
    .config("spark.hadoop.fs.s3a.multipart.size", "104857600")
    .config("spark.hadoop.fs.s3a.fast.upload", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# READ
# ---------------------------------------------------------------------------
print(f"[raw] reading {S3_INPUT}")
raw_df = spark.read.option("multiLine", True).json(S3_INPUT)

# ---------------------------------------------------------------------------
# EXPLODE data[] → company struct
# ---------------------------------------------------------------------------
exploded_df = raw_df.select(
    col("ingested_at"),
    explode(col("data")).alias("company"),
)

# ---------------------------------------------------------------------------
# SELECT / FLATTEN
# ---------------------------------------------------------------------------
flat_df = exploded_df.select(
    col("company.id").alias("company_id"),
    col("company.name").alias("company_name"),
    col("company.website"),
    col("company.ticker"),
    col("company.phone"),
    col("company.revenue"),
    col("company.employeeCount").alias("employee_count"),
    col("company.companyType").alias("company_type"),
    col("company.city"),
    col("company.state"),
    col("company.country"),
    col("company.continent"),
    col("company.street"),
    col("company.zipCode").alias("zip_code"),
    col("company.naicsCode").alias("naics_code"),
    col("company.sicCode").alias("sic_code"),
    to_json(col("company.industryKeywords")).alias("industry_keywords_json"),
    to_json(col("company.hashtags")).alias("hashtags_json"),
    col("company.founded"),
    col("company.isDefunct").alias("is_defunct"),
    col("ingested_at"),
    current_timestamp().alias("etl_loaded_at"),
)

print(f"[raw] record count: {flat_df.count()}")
flat_df.printSchema()

# ---------------------------------------------------------------------------
# WRITE DELTA (overwrite)
# ---------------------------------------------------------------------------
print(f"[raw] writing Delta to {S3_OUTPUT}")
(
    flat_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(S3_OUTPUT)
)

print("[raw] done.")
spark.stop()
