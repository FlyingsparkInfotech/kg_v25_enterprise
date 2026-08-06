import os
#!/usr/bin/env python3
# --------------------------------------------------------------------------------------
# ZoomInfo Contact Enrich — GOLD (surrogate key contact_key + incremental merge)
# Surrogate key: contact_key (sequential, gap-free)
# Merge key: person_id
# Window: person_id (ascending) for deterministic key assignment
# Chains to WS-DEV-PGS-DATA/nb_zoominfo_contact_enrich_pgs.py
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
silver_path = "s3a://goglo-silver-layer/zoominfo/zoominfo-etl-contact-enrich-silver"
gold_path   = "s3a://goglo-gold-layer/zoominfo/zoominfo-etl-contact-enrich-gold"

logger.info("Starting Contact Enrich Silver → Gold")

try:
    df_silver   = spark.read.format("delta").load(silver_path)
    gold_exists = DeltaTable.isDeltaTable(spark, gold_path)

    if gold_exists:
        df_gold   = spark.read.format("delta").load(gold_path)
        max_key   = df_gold.select(F.max("contact_key")).collect()[0][0]
        start_key = (max_key + 1) if max_key is not None else 1
    else:
        df_gold   = None
        start_key = 1

    logger.info(f"Next contact_key starts at: {start_key}")

    # Anti-join to find net-new persons not yet in Gold
    df_new = (
        df_silver.join(df_gold.select("person_id"), on="person_id", how="left_anti")
        if gold_exists and df_gold is not None else df_silver
    )
    new_count = df_new.count()
    logger.info(f"Net-new records: {new_count}")

    if new_count > 0:
        window_spec = Window.orderBy(col("person_id"))
        df_new_with_key = df_new.withColumn(
            "contact_key",
            row_number().over(window_spec) + (start_key - 1)
        )
        ordered = ["contact_key"] + [
            c for c in df_new_with_key.columns if c != "contact_key"
        ]
        df_gold_final = df_new_with_key.select(ordered)

    if not gold_exists:
        df_gold_final.write.format("delta").mode("overwrite").save(gold_path)
        logger.info(f"Initial Gold created with {new_count} rows")
    elif new_count == 0:
        logger.info("No new records — skipping Gold merge")
    else:
        # Pull matched records with their existing surrogate keys for updates
        df_matched = df_silver.join(
            df_gold.select("person_id", "contact_key"),
            on="person_id",
            how="inner"
        )
        matched_count = df_matched.count()
        df_merge_source = (
            df_matched.unionByName(df_gold_final)
            if matched_count > 0 else df_gold_final
        )

        gold_table = DeltaTable.forPath(spark, gold_path)
        (
            gold_table.alias("g")
            .merge(df_merge_source.alias("s"), "g.person_id = s.person_id")
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
                "contact_key" : "s.contact_key",
                "id"          : "s.id",
                "person_id"   : "s.person_id",
                "mobile_phone": "s.mobile_phone",
                "phone"       : "s.phone",
                "email"       : "s.email",
                "mobile_dnc"  : "s.mobile_dnc",
                "direct_dnc"  : "s.direct_dnc",
                "company_id"  : "s.company_id",
                "ingested_at" : "s.ingested_at",
                "created_on"  : "s.created_on",
                "created_by"  : "s.created_by",
                "modified_on" : "current_timestamp()",
                "modified_by" : "'zoominfo'"
            })
            .execute()
        )
        logger.info(f"Gold merge done: {new_count} inserted, {matched_count} updated")

except Exception as e:
    logger.error(f"Gold processing failed: {e}", exc_info=True)
    raise

logger.info("Starting PGS loading...")
subprocess.run(
    ["python3", f"{ETL_BASE}/WS-DEV-PGS-DATA/nb_zoominfo_contact_enrich_pgs.py"],
    check=True
)
logger.info("PGS loading completed.")
