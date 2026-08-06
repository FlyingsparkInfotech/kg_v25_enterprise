import os
#!/usr/bin/env python3
# --------------------------------------------------------------------------------------
# ZoomInfo Contact Search — RAW (Bronze Delta)
# Reads JSON envelopes from S3, explodes data[] → one row per contact
# Chains to WS-DEV-SILVER-DATA/nb_zoominfo_contact_search_silver.py
# --------------------------------------------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    explode, col, to_timestamp, current_timestamp
)
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
source_path = "s3a://goglo-bronze-layer/zoominfo/contact_search"
raw_path    = "s3a://goglo-bronze-layer/zoominfo/zoominfo-etl-contact-search-raw"

# ZoomInfo date format: "M/d/yyyy h:mm a" e.g. "7/4/2025 3:30 PM"
ZI_DATE_FMT = "M/d/yyyy h:mm a"

logger.info("Starting Contact Search JSON → Bronze RAW")

try:
    df = spark.read.option("multiLine", "true").json(source_path)

    df_exploded = df.withColumn("contact", explode(col("data")))

    df_flat = df_exploded.select(
        col("contact.id").cast("long").alias("contact_id"),
        col("contact.firstName").alias("first_name"),
        col("contact.middleName").alias("middle_name"),
        col("contact.lastName").alias("last_name"),
        col("contact.jobTitle").alias("job_title"),
        col("contact.contactAccuracyScore").cast("int").alias("contact_accuracy_score"),
        to_timestamp(col("contact.validDate"), ZI_DATE_FMT).alias("valid_date"),
        to_timestamp(col("contact.lastUpdatedDate"), ZI_DATE_FMT).alias("last_updated_date"),
        col("contact.hasEmail").cast("boolean").alias("has_email"),
        col("contact.hasSupplementalEmail").cast("boolean").alias("has_supplemental_email"),
        col("contact.hasDirectPhone").cast("boolean").alias("has_direct_phone"),
        col("contact.hasMobilePhone").cast("boolean").alias("has_mobile_phone"),
        col("contact.hasCompanyIndustry").cast("boolean").alias("has_company_industry"),
        col("contact.hasCompanyPhone").cast("boolean").alias("has_company_phone"),
        col("contact.hasCompanyStreet").cast("boolean").alias("has_company_street"),
        col("contact.hasCompanyState").cast("boolean").alias("has_company_state"),
        col("contact.hasCompanyZipCode").cast("boolean").alias("has_company_zip_code"),
        col("contact.hasCompanyCountry").cast("boolean").alias("has_company_country"),
        col("contact.hasCompanyRevenue").cast("boolean").alias("has_company_revenue"),
        col("contact.hasCompanyEmployeeCount").cast("boolean").alias("has_company_employee_count"),
        col("contact.directPhoneDoNotCall").cast("boolean").alias("direct_phone_do_not_call"),
        col("contact.mobilePhoneDoNotCall").cast("boolean").alias("mobile_phone_do_not_call"),
        col("contact.company.id").cast("long").alias("company_id"),
        col("contact.company.name").alias("company_name"),
        col("ingested_at")
    )

    df_flat.write.format("delta").mode("overwrite").save(raw_path)
    logger.info(f"RAW Delta written: {df_flat.count()} rows → {raw_path}")

except Exception as e:
    logger.error(f"RAW processing failed: {e}", exc_info=True)
    raise

logger.info("Starting SILVER processing...")
subprocess.run(
    ["python3", f"{ETL_BASE}/WS-DEV-SILVER-DATA/nb_zoominfo_contact_search_silver.py"],
    check=True
)
logger.info("SILVER processing completed.")
