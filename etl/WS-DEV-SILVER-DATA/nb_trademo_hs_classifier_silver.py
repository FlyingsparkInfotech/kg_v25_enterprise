import os
#!/usr/bin/env python3
# --------------------------------------------------------------------------------------
# Trademo HS Classifier — SILVER (Delta merge)
# Dedup key: sha2(product_title | country_of_classification | trade_direction)
# Merge key: hs_hash
# Chains to WS-DEV-GOLD-DATA/nb_trademo_hs_classifier_gold.py
# --------------------------------------------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sha2, concat_ws, current_timestamp, row_number
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

ETL_BASE   = "/opt/.debug/kg_v25_enterprise/etl"
raw_path    = "s3a://goglo-bronze-layer/trademo/trademo-etl-hs-classifier-raw"
silver_path = "s3a://goglo-silver-layer/trademo/trademo-etl-hs-classifier-silver"

logger.info("Starting HS Classifier RAW → Silver")

try:
    df_raw = spark.read.format("delta").load(raw_path)

    df_transformed = (
        df_raw
        .withColumn("hs_hash", sha2(
            concat_ws("|",
                col("product_title"),
                col("country_of_classification"),
                col("trade_direction")
            ), 256
        ))
        .withColumn("created_on",  current_timestamp())
        .withColumn("created_by",  lit("spark-etl"))
        .withColumn("modified_on", current_timestamp())
        .withColumn("modified_by", lit("trademo"))
    )

    # Dedup: keep the latest per unique (title|country|direction)
    window_spec = Window.partitionBy("hs_hash").orderBy(col("ingested_at").desc())
    df_dedup = (
        df_transformed
        .withColumn("rn", row_number().over(window_spec))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    silver_exists = DeltaTable.isDeltaTable(spark, silver_path)

    if not silver_exists:
        df_dedup.write.format("delta").mode("overwrite").save(silver_path)
        logger.info(f"Initial Silver created: {df_dedup.count()} rows")
    else:
        dt = DeltaTable.forPath(spark, silver_path)
        (
            dt.alias("t")
            .merge(df_dedup.alias("s"), "t.hs_hash = s.hs_hash")
            .whenMatchedUpdate(set={
                "most_suitable_hs_code"     : "s.most_suitable_hs_code",
                "most_suitable_description" : "s.most_suitable_description",
                "dutiable_hs_codes_json"    : "s.dutiable_hs_codes_json",
                "status"                    : "s.status",
                "ingested_at"               : "s.ingested_at",
                "modified_on"               : "current_timestamp()",
                "modified_by"               : "'trademo'"
            })
            .whenNotMatchedInsert(values={
                "hs_hash"                   : "s.hs_hash",
                "product_title"             : "s.product_title",
                "product_description"       : "s.product_description",
                "country_of_classification" : "s.country_of_classification",
                "trade_direction"           : "s.trade_direction",
                "sku_id"                    : "s.sku_id",
                "status"                    : "s.status",
                "most_suitable_hs_code"     : "s.most_suitable_hs_code",
                "most_suitable_description" : "s.most_suitable_description",
                "dutiable_hs_codes_json"    : "s.dutiable_hs_codes_json",
                "ingested_at"               : "s.ingested_at",
                "created_on"                : "current_timestamp()",
                "created_by"                : "'spark-etl'",
                "modified_on"               : "current_timestamp()",
                "modified_by"               : "'trademo'"
            })
            .execute()
        )
        logger.info("Silver merge completed")

except Exception as e:
    logger.error(f"Silver processing failed: {e}", exc_info=True)
    raise

logger.info("Starting GOLD processing...")
subprocess.run(
    ["python3", f"{ETL_BASE}/WS-DEV-GOLD-DATA/nb_trademo_hs_classifier_gold.py"],
    check=True
)
logger.info("GOLD processing completed.")
