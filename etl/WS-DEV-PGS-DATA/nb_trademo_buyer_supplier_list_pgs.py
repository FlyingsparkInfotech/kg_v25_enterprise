import os
#!/usr/bin/env python3
# --------------------------------------------------------------------------------------
# Trademo Buyer Supplier List — PGS (Gold → PostgreSQL goglo_etl)
# Target: raw.trademo_buyer_supplier_list  (localhost:5432/goglo_etl)
# Anti-join on bsl_key to prevent duplicates
# Arrays flattened: matched_hs_codes, matched_product_keyword,
#                   matched_countries_trading_with → comma-separated TEXT
#                   stock_tickers → JSON string
# --------------------------------------------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, to_json
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
PG_PROPS = {
    "user"    : "etl_user",
    "password": "EtlCozmo@2026!",
    "driver"  : "org.postgresql.Driver"
}

gold_path = "s3a://goglo-gold-layer/trademo/trademo-etl-buyer-supplier-list-gold"

logger.info("Starting Buyer Supplier List Gold → PostgreSQL load")

try:
    df_gold = spark.read.format("delta").load(gold_path)

    # Flatten arrays to TEXT for Postgres compatibility
    df_out = df_gold.select(
        col("bsl_key"),
        col("hash_key"),
        col("company_id"),
        col("company_name"),
        col("country"),
        col("state"),
        col("city"),
        col("zip_code"),
        col("address"),
        col("number_of_shipments"),
        col("shipment_value"),
        col("trading_partner_count"),
        concat_ws(",", col("matched_hs_codes")).alias("matched_hs_codes"),
        to_json(col("stock_tickers")).alias("stock_tickers"),
        concat_ws(",", col("matched_product_keyword")).alias("matched_product_keyword"),
        concat_ws(",", col("matched_countries_trading_with")).alias("matched_countries_trading_with"),
        col("company_role"),
        col("from_date"),
        col("to_date"),
        col("ingested_at"),
        col("created_on"),
        col("created_by"),
        col("modified_on"),
        col("modified_by")
    )

    existing_keys = (
        spark.read.format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", "(SELECT bsl_key FROM raw.trademo_buyer_supplier_list) AS t")
        .option("user", PG_PROPS["user"])
        .option("password", PG_PROPS["password"])
        .option("driver", PG_PROPS["driver"])
        .load()
    )

    new_rows  = df_out.join(existing_keys, on="bsl_key", how="left_anti")
    new_count = new_rows.count()
    logger.info(f"New rows to insert: {new_count}")

    if new_count > 0:
        new_rows.write.format("jdbc") \
            .option("url", PG_URL) \
            .option("dbtable", "raw.trademo_buyer_supplier_list") \
            .option("user", PG_PROPS["user"]) \
            .option("password", PG_PROPS["password"]) \
            .option("driver", PG_PROPS["driver"]) \
            .mode("append") \
            .save()
        logger.info(f"Inserted {new_count} rows into raw.trademo_buyer_supplier_list")
    else:
        logger.info("No new rows — Postgres already up to date")

except Exception as e:
    logger.error(f"PGS loading failed: {e}", exc_info=True)
    raise

logger.info("Buyer Supplier List PGS load complete.")
