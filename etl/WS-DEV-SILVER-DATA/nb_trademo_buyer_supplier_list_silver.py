import os
#!/usr/bin/env python3
# --------------------------------------------------------------------------------------
# Trademo Buyer Supplier List — SILVER (Delta merge)
# Merge key: hash_key = sha2(company_id|country|zip_code|matched_hs_codes|company_role)
# Dedup: keep record with highest number_of_shipments per hash_key
# Chains to WS-DEV-GOLD-DATA/nb_trademo_buyer_supplier_list_gold.py
# --------------------------------------------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, current_timestamp, row_number, sha2, concat_ws, coalesce
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
bronze_path = "s3a://goglo-bronze-layer/trademo/trademo-etl-buyer-supplier-list-raw"
silver_path = "s3a://goglo-silver-layer/trademo/trademo-etl-buyer-supplier-list-silver"

logger.info("Starting Buyer Supplier List Bronze → Silver")

try:
    df_bronze = spark.read.format("delta").load(bronze_path)

    df_transformed = (
        df_bronze.select(
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
            col("matched_hs_codes"),
            col("stock_tickers"),
            col("matched_product_keyword"),
            col("matched_countries_trading_with"),
            col("company_role"),
            col("from_date"),
            col("to_date"),
            col("ingested_at")
        )
        .withColumn("created_on",  current_timestamp())
        .withColumn("created_by",  lit("spark-etl"))
        .withColumn("modified_on", current_timestamp())
        .withColumn("modified_by", lit("trademo"))
    )

    # Hash key: company_id|country|zip_code|matched_hs_codes|company_role
    # company_role included so same company as Buyer vs Supplier gets distinct rows
    df_transformed = (
        df_transformed
        .withColumn("hash_key", sha2(
            concat_ws("|",
                coalesce(col("company_id").cast("string"), lit("")),
                coalesce(col("country"),                   lit("")),
                coalesce(col("zip_code"),                  lit("")),
                coalesce(col("matched_hs_codes").cast("string"), lit("")),
                coalesce(col("company_role"),              lit(""))
            ), 256
        ))
    )

    ordered = ["hash_key"] + [c for c in df_transformed.columns if c != "hash_key"]
    df_transformed = df_transformed.select(ordered)

    # Dedup: keep record with highest number_of_shipments per hash_key
    window_spec = Window.partitionBy("hash_key").orderBy(col("number_of_shipments").desc())
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
            .merge(df_dedup.alias("s"), "t.hash_key = s.hash_key")
            .whenMatchedUpdate(set={
                "company_id"                     : "s.company_id",
                "company_name"                   : "s.company_name",
                "country"                        : "s.country",
                "state"                          : "s.state",
                "city"                           : "s.city",
                "zip_code"                       : "s.zip_code",
                "address"                        : "s.address",
                "number_of_shipments"            : "s.number_of_shipments",
                "shipment_value"                 : "s.shipment_value",
                "trading_partner_count"          : "s.trading_partner_count",
                "matched_hs_codes"               : "s.matched_hs_codes",
                "stock_tickers"                  : "s.stock_tickers",
                "matched_product_keyword"        : "s.matched_product_keyword",
                "matched_countries_trading_with" : "s.matched_countries_trading_with",
                "company_role"                   : "s.company_role",
                "from_date"                      : "s.from_date",
                "to_date"                        : "s.to_date",
                "modified_on"                    : "current_timestamp()",
                "modified_by"                    : "'trademo'"
            })
            .whenNotMatchedInsert(values={
                "hash_key"                       : "s.hash_key",
                "company_id"                     : "s.company_id",
                "company_name"                   : "s.company_name",
                "country"                        : "s.country",
                "state"                          : "s.state",
                "city"                           : "s.city",
                "zip_code"                       : "s.zip_code",
                "address"                        : "s.address",
                "number_of_shipments"            : "s.number_of_shipments",
                "shipment_value"                 : "s.shipment_value",
                "trading_partner_count"          : "s.trading_partner_count",
                "matched_hs_codes"               : "s.matched_hs_codes",
                "stock_tickers"                  : "s.stock_tickers",
                "matched_product_keyword"        : "s.matched_product_keyword",
                "matched_countries_trading_with" : "s.matched_countries_trading_with",
                "company_role"                   : "s.company_role",
                "from_date"                      : "s.from_date",
                "to_date"                        : "s.to_date",
                "ingested_at"                    : "s.ingested_at",
                "created_on"                     : "current_timestamp()",
                "created_by"                     : "'spark-etl'",
                "modified_on"                    : "current_timestamp()",
                "modified_by"                    : "'trademo'"
            })
            .execute()
        )
        logger.info("Silver merge completed")

except Exception as e:
    logger.error(f"Silver processing failed: {e}", exc_info=True)
    raise

logger.info("Starting GOLD processing...")
subprocess.run(
    ["python3", f"{ETL_BASE}/WS-DEV-GOLD-DATA/nb_trademo_buyer_supplier_list_gold.py"],
    check=True
)
logger.info("GOLD processing completed.")
