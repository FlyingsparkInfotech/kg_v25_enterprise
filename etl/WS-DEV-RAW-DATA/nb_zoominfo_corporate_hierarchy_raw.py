import os
#!/usr/bin/env python3
# --------------------------------------------------------------------------------------
# ZoomInfo Corporate Hierarchy — RAW (Bronze Delta)
# Flattens deeply nested familyTree/parentage JSON into tabular rows
# Chains to WS-DEV-SILVER-DATA/nb_zoominfo_corporate_hierarchy_silver.py
# --------------------------------------------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_json, get_json_object
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
source_path = "s3a://goglo-bronze-layer/zoominfo/corporate_hierarchy"
raw_path    = "s3a://goglo-bronze-layer/zoominfo/zoominfo-etl-corporate-hierarchy-raw"

logger.info("Starting Corporate Hierarchy JSON → Bronze RAW")

try:
    df = spark.read.option("multiLine", "true").json(source_path)

    # Envelope: { ingested_at, match_input:{companyName,companyId}, response:{success,data:{result:[...]}} }
    # Explode result array — each item is one company lookup
    df_results = df.withColumn("result", explode(col("response.data.result")))

    # Extract flat company data from nested response
    # response.data.result[i].data: { companyId, parentage:{...}, familyTree:{familyNodes:[...]} }
    df_flat = df_results.select(
        col("ingested_at"),
        col("match_input.companyName").alias("query_company_name"),
        col("match_input.companyId").alias("query_company_id"),
        col("result.data.companyId").cast("long").alias("company_id"),
        # parentage fields
        col("result.data.parentage.parentCompany.id").cast("long").alias("parent_company_id"),
        col("result.data.parentage.parentCompany.name").alias("parent_company_name"),
        col("result.data.parentage.parentCompany.country").alias("parent_country"),
        col("result.data.parentage.ultimateParentCompany.id").cast("long").alias("ultimate_parent_id"),
        col("result.data.parentage.ultimateParentCompany.name").alias("ultimate_parent_name"),
        col("result.data.parentage.ultimateParentCompany.country").alias("ultimate_parent_country"),
        # family tree stored as JSON string (deeply nested — flatten in Silver if needed)
        to_json(col("result.data.familyTree")).alias("family_tree_json"),
    )

    df_flat.write.format("delta").mode("overwrite").save(raw_path)
    logger.info(f"RAW Delta written: {df_flat.count()} rows → {raw_path}")

except Exception as e:
    logger.error(f"RAW processing failed: {e}", exc_info=True)
    raise

logger.info("Starting SILVER processing...")
subprocess.run(
    ["python3", f"{ETL_BASE}/WS-DEV-SILVER-DATA/nb_zoominfo_corporate_hierarchy_silver.py"],
    check=True
)
logger.info("SILVER processing completed.")
