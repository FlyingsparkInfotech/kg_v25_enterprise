import os
#!/usr/bin/env python3
# --------------------------------------------------------------------------------------
# ZoomInfo Contact Enrich — SILVER (Delta merge on person_id)
# Dedup: keep latest row per person_id
# Chains to WS-DEV-GOLD-DATA/nb_zoominfo_contact_enrich_gold.py
# --------------------------------------------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, row_number
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
raw_path    = "s3a://goglo-bronze-layer/zoominfo/zoominfo-etl-contact-enrich-raw"
silver_path = "s3a://goglo-silver-layer/zoominfo/zoominfo-etl-contact-enrich-silver"

logger.info("Starting Contact Enrich RAW → Silver")

try:
    df_raw = spark.read.format("delta").load(raw_path)

    df_transformed = (
        df_raw
        .withColumn("created_on",  current_timestamp())
        .withColumn("created_by",  lit("spark-etl"))
        .withColumn("modified_on", current_timestamp())
        .withColumn("modified_by", lit("zoominfo"))
    )

    # Dedup on person_id — keep latest ingested record
    window_spec = Window.partitionBy("person_id").orderBy(
        col("ingested_at").desc()
    )
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
            .merge(df_dedup.alias("s"), "t.person_id = s.person_id")
            .whenMatchedUpdate(set={
                "id"          : "s.id",
                "mobile_phone": "s.mobile_phone",
                "phone"       : "s.phone",
                "email"       : "s.email",
                "mobile_dnc"  : "s.mobile_dnc",
                "direct_dnc"  : "s.direct_dnc",
                "company_id"  : "s.company_id",
                "ingested_at" : "s.ingested_at",
                "modified_on" : "current_timestamp()",
                "modified_by" : "'zoominfo'"
            })
            .whenNotMatchedInsert(values={
                "id"          : "s.id",
                "person_id"   : "s.person_id",
                "mobile_phone": "s.mobile_phone",
                "phone"       : "s.phone",
                "email"       : "s.email",
                "mobile_dnc"  : "s.mobile_dnc",
                "direct_dnc"  : "s.direct_dnc",
                "company_id"  : "s.company_id",
                "ingested_at" : "s.ingested_at",
                "created_on"  : "current_timestamp()",
                "created_by"  : "'spark-etl'",
                "modified_on" : "current_timestamp()",
                "modified_by" : "'zoominfo'"
            })
            .execute()
        )
        logger.info("Silver merge completed")

except Exception as e:
    logger.error(f"Silver processing failed: {e}", exc_info=True)
    raise

logger.info("Starting GOLD processing...")
subprocess.run(
    ["python3", f"{ETL_BASE}/WS-DEV-GOLD-DATA/nb_zoominfo_contact_enrich_gold.py"],
    check=True
)
logger.info("GOLD processing completed.")
