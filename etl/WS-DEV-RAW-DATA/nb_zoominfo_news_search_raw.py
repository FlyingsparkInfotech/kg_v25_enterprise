import os
#!/usr/bin/env python3
# --------------------------------------------------------------------------------------
# ZoomInfo News Search — RAW (Bronze Delta)
# Explodes data[] array from JSON envelopes → Bronze Delta
# Chains to WS-DEV-SILVER-DATA/nb_zoominfo_news_search_silver.py
# --------------------------------------------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, to_json
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
source_path = "s3a://goglo-bronze-layer/zoominfo/news_search"
raw_path    = "s3a://goglo-bronze-layer/zoominfo/zoominfo-etl-news-search-raw"

logger.info("Starting News Search JSON → Bronze RAW")

try:
    df = spark.read.option("multiLine", "true").json(source_path)

    df_exploded = df.withColumn("news", explode(col("data")))

    df_flat = df_exploded.select(
        col("ingested_at"),
        col("pageDateMin").alias("page_date_min"),
        col("pageDateMax").alias("page_date_max"),
        col("news.id").cast("long").alias("news_id"),
        col("news.title").alias("title"),
        col("news.url").alias("news_url"),
        col("news.category").alias("category"),
        col("news.publishedDate").alias("published_date"),
        col("news.companyId").cast("long").alias("company_id"),
        col("news.companyName").alias("company_name"),
        col("news.domain").alias("domain"),
    )

    df_flat.write.format("delta").mode("overwrite").save(raw_path)
    logger.info(f"RAW Delta written: {df_flat.count()} rows → {raw_path}")

except Exception as e:
    logger.error(f"RAW processing failed: {e}", exc_info=True)
    raise

logger.info("Starting SILVER processing...")
subprocess.run(
    ["python3", f"{ETL_BASE}/WS-DEV-SILVER-DATA/nb_zoominfo_news_search_silver.py"],
    check=True
)
logger.info("SILVER processing completed.")
