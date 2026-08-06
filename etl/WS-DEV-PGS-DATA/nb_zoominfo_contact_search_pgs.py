import os
#!/usr/bin/env python3
# --------------------------------------------------------------------------------------
# ZoomInfo Contact Search — PGS (Gold → PostgreSQL goglo_etl)
# Anti-join on contact_search_key against existing rows in zoominfo.contact_search
# Target: zoominfo.contact_search  (localhost:5432/goglo_etl)
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
gold_path = "s3a://goglo-gold-layer/zoominfo/zoominfo-etl-contact-search-gold"

logger.info("Starting Contact Search Gold → PostgreSQL load")

try:
    df_gold = spark.read.format("delta").load(gold_path)

    df_out = df_gold.select(
        col("contact_search_key"),
        col("contact_id"),
        col("first_name"),
        col("middle_name"),
        col("last_name"),
        col("job_title"),
        col("contact_accuracy_score"),
        col("valid_date"),
        col("last_updated_date"),
        col("has_email"),
        col("has_supplemental_email"),
        col("has_direct_phone"),
        col("has_mobile_phone"),
        col("has_company_industry"),
        col("has_company_phone"),
        col("has_company_street"),
        col("has_company_state"),
        col("has_company_zip_code"),
        col("has_company_country"),
        col("has_company_revenue"),
        col("has_company_employee_count"),
        col("direct_phone_do_not_call"),
        col("mobile_phone_do_not_call"),
        col("company_id"),
        col("company_name"),
        col("ingested_at"),
        col("created_on"),
        col("created_by"),
        col("modified_on"),
        col("modified_by")
    )

    existing_keys = (
        spark.read.format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", "(SELECT contact_search_key FROM zoominfo.contact_search) AS t")
        .option("user", PG_PROPS["user"])
        .option("password", PG_PROPS["password"])
        .option("driver", PG_PROPS["driver"])
        .load()
    )

    new_rows  = df_out.join(existing_keys, on="contact_search_key", how="left_anti")
    new_count = new_rows.count()
    logger.info(f"New rows to insert: {new_count}")

    if new_count > 0:
        new_rows.write.format("jdbc") \
            .option("url", PG_URL) \
            .option("dbtable", "zoominfo.contact_search") \
            .option("user", PG_PROPS["user"]) \
            .option("password", PG_PROPS["password"]) \
            .option("driver", PG_PROPS["driver"]) \
            .mode("append") \
            .save()
        logger.info(f"Inserted {new_count} rows into zoominfo.contact_search")
    else:
        logger.info("No new rows — Postgres already up to date")

except Exception as e:
    logger.error(f"PGS loading failed: {e}", exc_info=True)
    raise

logger.info("Contact Search PGS load complete.")
