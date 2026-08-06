import os
#!/usr/bin/env python3
# --------------------------------------------------------------------------------------
# ZoomInfo Contact Enrich — RAW (Bronze Delta)
#
# Response structure:
#   { success, data: { result: [ { input: {personid}, data: [{id, mobilePhone,
#     phone, email, mobilePhoneDoNotCall, directPhoneDoNotCall, company:{id}}] } ] } }
#
# Explodes data.result → result, then result.data → contact
# Chains to WS-DEV-SILVER-DATA/nb_zoominfo_contact_enrich_silver.py
# --------------------------------------------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, current_timestamp
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
source_path = "s3a://goglo-bronze-layer/zoominfo/contact_enrich"
raw_path    = "s3a://goglo-bronze-layer/zoominfo/zoominfo-etl-contact-enrich-raw"

logger.info("Starting Contact Enrich JSON → Bronze RAW")

try:
    df = spark.read.option("multiLine", "true").json(source_path)

    # Explode data.result array → one row per result entry
    df_result = df.withColumn("result", explode(col("data.result")))

    # Explode result.data array → one row per contact record
    df_contact = df_result.withColumn("contact", explode(col("result.data")))

    df_flat = df_contact.select(
        col("contact.id").cast("long").alias("id"),
        col("result.input.personid").cast("long").alias("person_id"),
        col("contact.mobilePhone").alias("mobile_phone"),
        col("contact.phone").alias("phone"),
        col("contact.email").alias("email"),
        col("contact.mobilePhoneDoNotCall").cast("boolean").alias("mobile_dnc"),
        col("contact.directPhoneDoNotCall").cast("boolean").alias("direct_dnc"),
        col("contact.company.id").cast("long").alias("company_id"),
        current_timestamp().alias("ingested_at")
    )

    df_flat.write.format("delta").mode("overwrite").save(raw_path)
    logger.info(f"RAW Delta written: {df_flat.count()} rows → {raw_path}")

except Exception as e:
    logger.error(f"RAW processing failed: {e}", exc_info=True)
    raise

logger.info("Starting SILVER processing...")
subprocess.run(
    ["python3", f"{ETL_BASE}/WS-DEV-SILVER-DATA/nb_zoominfo_contact_enrich_silver.py"],
    check=True
)
logger.info("SILVER processing completed.")
