#!/usr/bin/env python3
"""
ZoomInfo Intent Search — Gold
Reads silver Delta, assigns surrogate key (intent_key) via anti-join on intent_hash,
appends new records to gold Delta table.
Chains to nb_zoominfo_intent_search_pgs.py
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, max as spark_max, lit
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# ---------------------------------------------------------------------------
# ENV / CREDENTIALS
# ---------------------------------------------------------------------------
AWS_ACCESS_KEY_ID     = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "")

ZI_USERNAME = os.environ.get("ZI_USERNAME", "sriram@powercozmo.com")
ZI_PASSWORD = os.environ.get("ZI_PASSWORD", "wewfox-3vanve-fecwuZ")

ETL_BASE = "/opt/.debug/kg_v25_enterprise/etl"

S3_SILVER = "s3a://goglo-silver-layer/zoominfo/zoominfo-etl-intent-search-silver"
S3_GOLD   = "s3a://goglo-gold-layer/zoominfo/zoominfo-etl-intent-search-gold"

# ---------------------------------------------------------------------------
# SPARK SESSION
# ---------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("zoominfo_intent_search_gold")
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
# READ SILVER
# ---------------------------------------------------------------------------
print(f"[gold] reading silver Delta from {S3_SILVER}")
silver_df = spark.read.format("delta").load(S3_SILVER)

# ---------------------------------------------------------------------------
# DETERMINE MAX EXISTING SURROGATE KEY
# ---------------------------------------------------------------------------
if DeltaTable.isDeltaTable(spark, S3_GOLD):
    gold_df = spark.read.format("delta").load(S3_GOLD)
    max_key = gold_df.agg(spark_max("intent_key")).collect()[0][0] or 0

    # ANTI-JOIN: new intent_hashes not yet in gold
    new_df = silver_df.join(
        gold_df.select("intent_hash"),
        on="intent_hash",
        how="left_anti",
    )
    print(f"[gold] existing max_key={max_key}, new records={new_df.count()}")
else:
    max_key = 0
    new_df = silver_df
    print(f"[gold] gold table does not exist — treating all {new_df.count()} records as new")

# ---------------------------------------------------------------------------
# ASSIGN SURROGATE KEY
# Ordered by intent_hash for determinism; offset by existing max_key.
# ---------------------------------------------------------------------------
window_spec = Window.orderBy("intent_hash")

keyed_df = new_df.withColumn(
    "intent_key",
    (row_number().over(window_spec) + lit(max_key)).cast("long"),
)

# ---------------------------------------------------------------------------
# APPEND TO GOLD
# ---------------------------------------------------------------------------
print(f"[gold] writing Delta to {S3_GOLD}")
(
    keyed_df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .save(S3_GOLD)
)

print("[gold] done.")
spark.stop()
