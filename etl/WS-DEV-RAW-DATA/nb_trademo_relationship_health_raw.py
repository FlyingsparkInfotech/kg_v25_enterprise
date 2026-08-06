import os
#!/usr/bin/env python3
# --------------------------------------------------------------------------------------
# Trademo Relationship Health — RAW (Bronze Delta)
# Reads JSON envelopes from s3a://goglo-bronze-layer/trademo/relationship-health/
# Flattens to tabular form and writes Bronze Delta
# Chains to WS-DEV-SILVER-DATA/nb_trademo_relationship_health_silver.py
# --------------------------------------------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object
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
source_path = "s3a://goglo-bronze-layer/trademo/relationship-health"
raw_path    = "s3a://goglo-bronze-layer/trademo/trademo-etl-relationship-health-raw"

logger.info("Starting Relationship Health JSON → Bronze RAW")

try:
    df = spark.read.option("multiLine", "true").json(source_path)

    # Envelope: { ingested_at, request:{supplier_id,buyer_id,from_date,to_date}, response:{...} }
    # API response fields: supplierId, buyerId, supplierName, buyerName, supplierCountryName,
    #   buyerCountryName, tradeRelationshipHealth, totalShipmentCount, shipmentTrend,
    #   lastShipmentDate, tradeFromDate, tradeToDate
    df_flat = df.select(
        col("ingested_at"),
        col("request.supplier_id").alias("supplier_id"),
        col("request.buyer_id").alias("buyer_id"),
        col("request.from_date").alias("trade_from_date"),
        col("request.to_date").alias("trade_to_date"),
        col("response.supplierName").alias("supplier_name"),
        col("response.supplierCountryName").alias("supplier_country"),
        col("response.buyerName").alias("buyer_name"),
        col("response.buyerCountryName").alias("buyer_country"),
        col("response.tradeRelationshipHealth").cast("double").alias("trade_relationship_health"),
        col("response.totalShipmentCount").cast("long").alias("total_shipment_count"),
        col("response.shipmentTrend").alias("shipment_trend"),
        col("response.lastShipmentDate").alias("last_shipment_date"),
    )

    df_flat.write.format("delta").mode("overwrite").save(raw_path)
    logger.info(f"RAW Delta written: {df_flat.count()} rows → {raw_path}")

except Exception as e:
    logger.error(f"RAW processing failed: {e}", exc_info=True)
    raise

logger.info("Starting SILVER processing...")
subprocess.run(
    ["python3", f"{ETL_BASE}/WS-DEV-SILVER-DATA/nb_trademo_relationship_health_silver.py"],
    check=True
)
logger.info("SILVER processing completed.")
