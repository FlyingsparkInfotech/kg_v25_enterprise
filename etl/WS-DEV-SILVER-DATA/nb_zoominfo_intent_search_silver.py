#!/usr/bin/env python3
"""
ZoomInfo Intent Search — Silver
Reads bronze Delta, computes intent_hash, deduplicates (latest ingested_at),
merges into silver Delta table.

Merge key: sha2(concat_ws("|", company_id, topic, signal_date), 256) → intent_hash
No natural unique key — same company can appear multiple times across topics and dates.

Chains to nb_zoominfo_intent_search_gold.py
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, concat_ws, row_number
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

S3_RAW    = "s3a://goglo-bronze-layer/zoominfo/zoominfo-etl-intent-search-raw"
S3_SILVER = "s3a://goglo-silver-layer/zoominfo/zoominfo-etl-intent-search-silver"

# ---------------------------------------------------------------------------
# SPARK SESSION
# ---------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("zoominfo_intent_search_silver")
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
# READ RAW
# ---------------------------------------------------------------------------
print(f"[silver] reading raw Delta from {S3_RAW}")
raw_df = spark.read.format("delta").load(S3_RAW)

# ---------------------------------------------------------------------------
# COMPUTE intent_hash
# ---------------------------------------------------------------------------
hashed_df = raw_df.withColumn(
    "intent_hash",
    sha2(concat_ws("|", col("company_id"), col("topic"), col("signal_date")), 256),
)

# ---------------------------------------------------------------------------
# DEDUP — keep latest ingested_at per intent_hash
# ---------------------------------------------------------------------------
window_spec = Window.partitionBy("intent_hash").orderBy(col("ingested_at").desc())

dedup_df = (
    hashed_df
    .withColumn("_rn", row_number().over(window_spec))
    .filter(col("_rn") == 1)
    .drop("_rn")
)

print(f"[silver] deduped record count: {dedup_df.count()}")

# ---------------------------------------------------------------------------
# MERGE INTO SILVER
# ---------------------------------------------------------------------------
MERGE_KEY = "intent_hash"

BUSINESS_COLS = [
    "company_id", "company_name", "topic", "signal_score",
    "audience_strength", "signal_date", "country", "state", "city",
    "employee_count", "revenue", "website",
    "ingested_at", "etl_loaded_at",
]

set_expr = {c: f"source.{c}" for c in BUSINESS_COLS}

if DeltaTable.isDeltaTable(spark, S3_SILVER):
    print("[silver] silver table exists — merging …")
    silver_tbl = DeltaTable.forPath(spark, S3_SILVER)
    (
        silver_tbl.alias("target")
        .merge(
            dedup_df.alias("source"),
            f"target.{MERGE_KEY} = source.{MERGE_KEY}",
        )
        .whenMatchedUpdate(set=set_expr)
        .whenNotMatchedInsert(values={**{MERGE_KEY: f"source.{MERGE_KEY}"}, **set_expr})
        .execute()
    )
else:
    print("[silver] silver table does not exist — creating …")
    dedup_df.write.format("delta").mode("overwrite").save(S3_SILVER)

print("[silver] done.")
spark.stop()
