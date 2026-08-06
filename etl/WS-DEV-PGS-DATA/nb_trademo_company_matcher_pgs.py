import os
#!/usr/bin/env python3
# --------------------------------------------------------------------------------------
# Trademo Company Matcher — PGS (Gold → PostgreSQL goglo_etl)
# Target: raw.trademo_company_matcher  (localhost:5432/goglo_etl)
# NOTE: etl-spark version pointed to 3.108.111.17:30002 (legacy remote).
#       This version targets the local goglo_etl postgres.
# --------------------------------------------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import logging

spark = (
    SparkSession.builder.appName("ETL")
    .config("spark.jars.packages", ",".join([
        "io.delta:delta-spark_2.12:3.3.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "org.postgresql:postgresql:42.6.0"
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

PG_URL   = "jdbc:postgresql://localhost:5432/goglo_etl"
PG_PROPS = {"user": "etl_user", "password": "EtlCozmo@2026!", "driver": "org.postgresql.Driver"}
gold_path = "s3a://goglo-gold-layer/trademo/trademo-etl-company-matcher-gold"

logger.info("Starting Company Matcher Gold → PostgreSQL load")

try:
    df_gold = spark.read.format("delta").load(gold_path)

    df_out = df_gold.select(
        col("cm_key"), col("company_id"), col("company_name"), col("country"),
        col("name_match_percentage"), col("total_shipment_count"), col("company_address"),
        col("created_on"), col("created_by"), col("modified_on"), col("modified_by")
    )

    existing_keys = (
        spark.read.format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", "(SELECT cm_key FROM raw.trademo_company_matcher) AS t")
        .option("user", PG_PROPS["user"]).option("password", PG_PROPS["password"])
        .option("driver", PG_PROPS["driver"]).load()
    )

    new_rows  = df_out.join(existing_keys, on="cm_key", how="left_anti")
    new_count = new_rows.count()
    logger.info(f"New rows to insert: {new_count}")

    if new_count > 0:
        new_rows.write.format("jdbc") \
            .option("url", PG_URL).option("dbtable", "raw.trademo_company_matcher") \
            .option("user", PG_PROPS["user"]).option("password", PG_PROPS["password"]) \
            .option("driver", PG_PROPS["driver"]).mode("append").save()
        logger.info(f"Inserted {new_count} rows into raw.trademo_company_matcher")
    else:
        logger.info("No new rows — Postgres already up to date")

except Exception as e:
    logger.error(f"PGS loading failed: {e}", exc_info=True)
    raise

logger.info("Company Matcher PGS load complete.")
