import os
#!/usr/bin/env python3
# --------------------------------------------------------------------------------------
# Trademo Buyer Supplier List — RAW (Bronze Delta)
# Reads paginated JSON files from s3a://goglo-bronze-layer/trademo/buyer-supplier-list/
# Explodes companies[] array → Bronze Delta table
# Chains to WS-DEV-SILVER-DATA/nb_trademo_buyer_supplier_list_silver.py
# --------------------------------------------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    explode, col, input_file_name, regexp_extract, current_timestamp,
    concat_ws, to_json
)
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
# Glob pattern covers: role/country/mode/date_range/timestamp/page_N.json
source_path = "s3a://goglo-bronze-layer/trademo/buyer-supplier-list/*/*/*/*/*/*.json"
bronze_path = "s3a://goglo-bronze-layer/trademo/trademo-etl-buyer-supplier-list-raw"

logger.info("Starting Buyer Supplier List JSON → Bronze RAW")

try:
    df = (
        spark.read
        .option("multiLine", "true")
        .json(source_path)
        .withColumn("source_file", input_file_name())
    )

    # Extract company_role, from_date, to_date from file path since the API
    # response does not include these fields.
    # Path: ...buyer-supplier-list/{role}/{country}/{mode}/{fromDate}_to_{toDate}/{ts}/page_N.json
    df = (
        df
        .withColumn("company_role",
            regexp_extract(col("source_file"), r"buyer-supplier-list/([^/]+)/", 1))
        .withColumn("from_date",
            regexp_extract(col("source_file"), r"(\d{4}-\d{2}-\d{2})_to_\d{4}-\d{2}-\d{2}", 1))
        .withColumn("to_date",
            regexp_extract(col("source_file"), r"\d{4}-\d{2}-\d{2}_to_(\d{4}-\d{2}-\d{2})", 1))
    )

    df_flat = (
        df
        .withColumn("company", explode(col("companies")))
        .select(
            col("company.companyId").alias("company_id"),
            col("company.companyName").alias("company_name"),
            col("company.country"),
            col("company.state"),
            col("company.city"),
            col("company.zipCode").alias("zip_code"),
            col("company.addressList").alias("address"),
            col("company.numberOfShipments").alias("number_of_shipments"),
            col("company.shipmentValue").alias("shipment_value"),
            col("company.tradingPartnerCount").alias("trading_partner_count"),
            col("company.matchedHsCodes").alias("matched_hs_codes"),
            col("company.stockTickers").alias("stock_tickers"),
            col("company.matchedProductKeyword").alias("matched_product_keyword"),
            col("company.matchedCountriesTradingWith").alias("matched_countries_trading_with"),
            col("company_role"),
            col("from_date"),
            col("to_date"),
            current_timestamp().alias("ingested_at")
        )
    )

    df_flat.write.format("delta").mode("overwrite").save(bronze_path)
    logger.info(f"RAW Delta written: {df_flat.count()} rows → {bronze_path}")

except Exception as e:
    logger.error(f"RAW processing failed: {e}", exc_info=True)
    raise

logger.info("Starting SILVER processing...")
subprocess.run(
    ["python3", f"{ETL_BASE}/WS-DEV-SILVER-DATA/nb_trademo_buyer_supplier_list_silver.py"],
    check=True
)
logger.info("SILVER processing completed.")
