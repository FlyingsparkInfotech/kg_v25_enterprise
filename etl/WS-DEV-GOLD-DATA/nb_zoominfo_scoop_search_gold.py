import os
#!/usr/bin/env python3
# --------------------------------------------------------------------------------------
# ZoomInfo Scoop Search — GOLD (surrogate key sc_key + incremental merge)
# Merge key: scoop_id
# Chains to WS-DEV-PGS-DATA/nb_zoominfo_scoop_search_pgs.py
# --------------------------------------------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, row_number, current_timestamp
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import subprocess, logging

spark = (
    SparkSession.builder.appName("ETL")
    .config("spark.jars.packages", ",".join([
        "io.delta:delta-spark_2.12:3.3.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    ]))
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID", ""))
    .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY", ""))
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
    .config("spark.hadoop.fs.s3a.socket.timeout", "60000")
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
    .config("spark.hadoop.fs.s3a.vectored.read.min.seek.size", "131072")
    .config("spark.hadoop.fs.s3a.vectored.read.max.merged.size", "2097152")
    .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000")
    .config("spark.hadoop.fs.s3a.retry.interval", "500")
    .config("spark.hadoop.fs.s3a.retry.throttle.interval", "100")
    .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400000")
    .getOrCreate()
)

logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s] %(levelname)s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

ETL_BASE    = "/opt/.debug/kg_v25_enterprise/etl"
silver_path = "s3a://goglo-silver-layer/zoominfo/zoominfo-etl-scoop-search-silver"
gold_path   = "s3a://goglo-gold-layer/zoominfo/zoominfo-etl-scoop-search-gold"

logger.info("Starting Scoop Search Silver → Gold")

try:
    df_silver   = spark.read.format("delta").load(silver_path)
    gold_exists = DeltaTable.isDeltaTable(spark, gold_path)

    if gold_exists:
        df_gold   = spark.read.format("delta").load(gold_path)
        max_key   = df_gold.select(F.max("sc_key")).collect()[0][0]
        start_key = (max_key + 1) if max_key is not None else 1
    else:
        df_gold   = None
        start_key = 1

    logger.info(f"Next sc_key starts at: {start_key}")

    df_new = (df_silver.join(df_gold.select("scoop_id"), on="scoop_id", how="left_anti")
              if gold_exists and df_gold is not None else df_silver)

    new_count = df_new.count()
    logger.info(f"Net-new records: {new_count}")

    if new_count > 0:
        window_spec = Window.orderBy("scoop_id")
        df_new_with_key = df_new.withColumn(
            "sc_key", row_number().over(window_spec) + (start_key - 1)
        )
        ordered = ["sc_key"] + [c for c in df_new_with_key.columns if c != "sc_key"]
        df_gold_final = df_new_with_key.select(ordered)

    if not gold_exists:
        df_gold_final.write.format("delta").mode("overwrite").save(gold_path)
        logger.info(f"Initial Gold created with {new_count} rows")
    elif new_count == 0:
        logger.info("No new records — skipping Gold merge")
    else:
        df_matched = df_silver.join(
            df_gold.select("scoop_id", "sc_key"), on="scoop_id", how="inner"
        )
        matched_count = df_matched.count()
        df_merge_source = (df_matched.unionByName(df_gold_final)
                           if matched_count > 0 else df_gold_final)

        gold_table = DeltaTable.forPath(spark, gold_path)
        (
            gold_table.alias("g")
            .merge(df_merge_source.alias("s"), "g.scoop_id = s.scoop_id")
            .whenMatchedUpdate(set={
                "scoop_title"   : "s.scoop_title",
                "scoop_summary" : "s.scoop_summary",
                "scoop_type"    : "s.scoop_type",
                "scoop_topic"   : "s.scoop_topic",
                "company_name"  : "s.company_name",
                "published_date": "s.published_date",
                "scoop_url"     : "s.scoop_url",
                "modified_on"   : "current_timestamp()",
                "modified_by"   : "'zoominfo'"
            })
            .whenNotMatchedInsert(values={
                "sc_key"        : "s.sc_key",
                "ingested_at"   : "s.ingested_at",
                "scoop_id"      : "s.scoop_id",
                "scoop_title"   : "s.scoop_title",
                "scoop_summary" : "s.scoop_summary",
                "scoop_type"    : "s.scoop_type",
                "scoop_topic"   : "s.scoop_topic",
                "company_id"    : "s.company_id",
                "company_name"  : "s.company_name",
                "published_date": "s.published_date",
                "scoop_url"     : "s.scoop_url",
                "scoop_source"  : "s.scoop_source",
                "created_on"    : "s.created_on",
                "created_by"    : "s.created_by",
                "modified_on"   : "current_timestamp()",
                "modified_by"   : "'zoominfo'"
            })
            .execute()
        )
        logger.info(f"Gold merge done: {new_count} inserted, {matched_count} updated")

except Exception as e:
    logger.error(f"Gold processing failed: {e}", exc_info=True)
    raise

logger.info("Starting PGS loading...")
subprocess.run(
    ["python3", f"{ETL_BASE}/WS-DEV-PGS-DATA/nb_zoominfo_scoop_search_pgs.py"],
    check=True
)
logger.info("PGS loading completed.")
