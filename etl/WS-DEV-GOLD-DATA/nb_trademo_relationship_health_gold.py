import os
#!/usr/bin/env python3
# --------------------------------------------------------------------------------------
# Trademo Relationship Health — GOLD (surrogate key rh_key + incremental merge)
# Merge key: (supplier_id, buyer_id)
# Chains to WS-DEV-PGS-DATA/nb_trademo_relationship_health_pgs.py
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
silver_path = "s3a://goglo-silver-layer/trademo/trademo-etl-relationship-health-silver"
gold_path   = "s3a://goglo-gold-layer/trademo/trademo-etl-relationship-health-gold"

logger.info("Starting Relationship Health Silver → Gold")

try:
    df_silver = spark.read.format("delta").load(silver_path)

    gold_exists = DeltaTable.isDeltaTable(spark, gold_path)

    if gold_exists:
        df_gold   = spark.read.format("delta").load(gold_path)
        max_key   = df_gold.select(F.max("rh_key")).collect()[0][0]
        start_key = (max_key + 1) if max_key is not None else 1
    else:
        df_gold   = None
        start_key = 1

    logger.info(f"Next rh_key starts at: {start_key}")

    if gold_exists and df_gold is not None:
        df_new = df_silver.join(
            df_gold.select("supplier_id", "buyer_id"),
            on=["supplier_id", "buyer_id"], how="left_anti"
        )
    else:
        df_new = df_silver

    new_count = df_new.count()
    logger.info(f"Net-new records: {new_count}")

    if new_count > 0:
        window_spec = Window.orderBy("supplier_id", "buyer_id")
        df_new_with_key = df_new.withColumn(
            "rh_key", row_number().over(window_spec) + (start_key - 1)
        )
        ordered = ["rh_key"] + [c for c in df_new_with_key.columns if c != "rh_key"]
        df_gold_final = df_new_with_key.select(ordered)

    if not gold_exists:
        df_gold_final.write.format("delta").mode("overwrite").save(gold_path)
        logger.info(f"Initial Gold created with {new_count} rows")
    elif new_count == 0:
        logger.info("No new records — skipping Gold merge")
    else:
        df_matched = df_silver.join(
            df_gold.select("supplier_id", "buyer_id", "rh_key"),
            on=["supplier_id", "buyer_id"], how="inner"
        )
        matched_count = df_matched.count()
        df_merge_source = (df_matched.unionByName(df_gold_final)
                           if matched_count > 0 else df_gold_final)

        gold_table = DeltaTable.forPath(spark, gold_path)
        (
            gold_table.alias("g")
            .merge(df_merge_source.alias("s"),
                   "g.supplier_id = s.supplier_id AND g.buyer_id = s.buyer_id")
            .whenMatchedUpdate(set={
                "supplier_name"             : "s.supplier_name",
                "buyer_name"                : "s.buyer_name",
                "supplier_country"          : "s.supplier_country",
                "buyer_country"             : "s.buyer_country",
                "trade_relationship_health" : "s.trade_relationship_health",
                "total_shipment_count"      : "s.total_shipment_count",
                "shipment_trend"            : "s.shipment_trend",
                "last_shipment_date"        : "s.last_shipment_date",
                "modified_on"               : "current_timestamp()",
                "modified_by"               : "'trademo'"
            })
            .whenNotMatchedInsert(values={
                "rh_key"                    : "s.rh_key",
                "ingested_at"               : "s.ingested_at",
                "supplier_id"               : "s.supplier_id",
                "buyer_id"                  : "s.buyer_id",
                "trade_from_date"           : "s.trade_from_date",
                "trade_to_date"             : "s.trade_to_date",
                "supplier_name"             : "s.supplier_name",
                "supplier_country"          : "s.supplier_country",
                "buyer_name"                : "s.buyer_name",
                "buyer_country"             : "s.buyer_country",
                "trade_relationship_health" : "s.trade_relationship_health",
                "total_shipment_count"      : "s.total_shipment_count",
                "shipment_trend"            : "s.shipment_trend",
                "last_shipment_date"        : "s.last_shipment_date",
                "created_on"                : "s.created_on",
                "created_by"                : "s.created_by",
                "modified_on"               : "current_timestamp()",
                "modified_by"               : "'trademo'"
            })
            .execute()
        )
        logger.info(f"Gold merge done: {new_count} inserted, {matched_count} updated")

except Exception as e:
    logger.error(f"Gold processing failed: {e}", exc_info=True)
    raise

logger.info("Starting PGS loading...")
subprocess.run(
    ["python3", f"{ETL_BASE}/WS-DEV-PGS-DATA/nb_trademo_relationship_health_pgs.py"],
    check=True
)
logger.info("PGS loading completed.")
