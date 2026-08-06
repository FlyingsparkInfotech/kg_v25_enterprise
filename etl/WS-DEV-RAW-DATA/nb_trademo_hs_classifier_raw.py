import os
#!/usr/bin/env python3
# --------------------------------------------------------------------------------------
# Trademo HS Classifier — RAW (Bronze Delta)
# Reads JSON files from s3a://goglo-bronze-layer/trademo/hs-classifier/
# Flattens response + request fields → Bronze Delta table
# Chains to WS-DEV-SILVER-DATA/nb_trademo_hs_classifier_silver.py
# --------------------------------------------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json
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
source_path = "s3a://goglo-bronze-layer/trademo/hs-classifier"
raw_path    = "s3a://goglo-bronze-layer/trademo/trademo-etl-hs-classifier-raw"

logger.info("Starting HS Classifier JSON → Bronze RAW")

try:
    df = spark.read.option("multiLine", "true").json(source_path)

    # Envelope shape: { ingested_at, request:{...}, response:{status, mostSuitableHs:{}, dutiableHsCode:[]} }
    df_flat = df.select(
        col("ingested_at"),
        col("request.product_title").alias("product_title"),
        col("request.product_description").alias("product_description"),
        col("request.country_of_classification").alias("country_of_classification"),
        col("request.trade_direction").alias("trade_direction"),
        col("request.sku_id").alias("sku_id"),
        col("response.status").alias("status"),
        col("response.mostSuitableHs.hsCode").alias("most_suitable_hs_code"),
        col("response.mostSuitableHs.description").alias("most_suitable_description"),
        to_json(col("response.dutiableHsCode")).alias("dutiable_hs_codes_json")
    )

    (
        df_flat.write
        .format("delta")
        .mode("overwrite")
        .save(raw_path)
    )

    logger.info(f"RAW Delta written: {df_flat.count()} rows → {raw_path}")

except Exception as e:
    logger.error(f"RAW processing failed: {e}", exc_info=True)
    raise

logger.info("Starting SILVER processing...")
subprocess.run(
    ["python3", f"{ETL_BASE}/WS-DEV-SILVER-DATA/nb_trademo_hs_classifier_silver.py"],
    check=True
)
logger.info("SILVER processing completed.")
