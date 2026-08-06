#!/usr/bin/env python3
"""
ZoomInfo Intent Search — Postgres Load
Reads gold Delta, anti-joins against zoominfo.intent_search in Postgres
on intent_key, appends net-new rows via JDBC.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# ---------------------------------------------------------------------------
# ENV / CREDENTIALS
# ---------------------------------------------------------------------------
AWS_ACCESS_KEY_ID     = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "")

ZI_USERNAME = os.environ.get("ZI_USERNAME", "sriram@powercozmo.com")
ZI_PASSWORD = os.environ.get("ZI_PASSWORD", "wewfox-3vanve-fecwuZ")

ETL_BASE = "/opt/.debug/kg_v25_enterprise/etl"

S3_GOLD = "s3a://goglo-gold-layer/zoominfo/zoominfo-etl-intent-search-gold"

PGS_JDBC_URL = "jdbc:postgresql://localhost:5432/goglo_etl"
PGS_USER     = "etl_user"
PGS_PASSWORD = "EtlCozmo@2026!"
PGS_TABLE    = "zoominfo.intent_search"

PGS_PROPERTIES = {
    "user":     PGS_USER,
    "password": PGS_PASSWORD,
    "driver":   "org.postgresql.Driver",
}

# ---------------------------------------------------------------------------
# SPARK SESSION
# ---------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("zoominfo_intent_search_pgs")
    .config("spark.jars.packages",
            "io.delta:delta-spark_2.12:3.3.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "org.postgresql:postgresql:42.6.0")
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
# READ GOLD DELTA
# ---------------------------------------------------------------------------
print(f"[pgs] reading gold Delta from {S3_GOLD}")
gold_df = spark.read.format("delta").load(S3_GOLD)

# ---------------------------------------------------------------------------
# READ EXISTING KEYS FROM POSTGRES
# ---------------------------------------------------------------------------
print(f"[pgs] reading existing keys from {PGS_TABLE}")
try:
    pgs_keys_df = (
        spark.read
        .jdbc(
            url=PGS_JDBC_URL,
            table=f"(SELECT intent_key FROM {PGS_TABLE}) AS pgs_keys",
            properties=PGS_PROPERTIES,
        )
    )
    existing_count = pgs_keys_df.count()
    print(f"[pgs] existing rows in Postgres: {existing_count}")

    # ANTI-JOIN — rows in gold not yet in Postgres
    new_df = gold_df.join(
        pgs_keys_df,
        on="intent_key",
        how="left_anti",
    )
except Exception as exc:
    # Table may not exist yet on first run
    print(f"[pgs] could not read Postgres table (first run?): {exc}")
    new_df = gold_df

new_count = new_df.count()
print(f"[pgs] net-new rows to insert: {new_count}")

# ---------------------------------------------------------------------------
# APPEND TO POSTGRES
# ---------------------------------------------------------------------------
if new_count > 0:
    print(f"[pgs] appending {new_count} rows to {PGS_TABLE}")
    (
        new_df.write
        .jdbc(
            url=PGS_JDBC_URL,
            table=PGS_TABLE,
            mode="append",
            properties=PGS_PROPERTIES,
        )
    )
    print("[pgs] append complete.")
else:
    print("[pgs] no new rows — skipping write.")

print("[pgs] done.")
spark.stop()
