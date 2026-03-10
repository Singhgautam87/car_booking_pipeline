from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))
from config_loader import load_config

config      = load_config()
minio_cfg   = config.get_minio_config()
delta_paths = config.get_delta_paths()

spark = SparkSession.builder \
    .appName("MergeAll") \
    .config("spark.hadoop.fs.s3a.endpoint",          minio_cfg['endpoint']) \
    .config("spark.hadoop.fs.s3a.access.key",        minio_cfg['access_key']) \
    .config("spark.hadoop.fs.s3a.secret.key",        minio_cfg['secret_key']) \
    .config("spark.hadoop.fs.s3a.path.style.access", str(minio_cfg['path_style']).lower()) \
    .getOrCreate()


import sys
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

try:
    spark.sparkContext.setLogLevel("WARN")

    customer_df = spark.read.format("delta").load(delta_paths['customer_transformed'])
    booking_df  = spark.read.format("delta").load(delta_paths['booking_transformed'])
    cars_df     = spark.read.format("delta").load(delta_paths['cars_transformed'])
    payments_df = spark.read.format("delta").load(delta_paths['payments_transformed'])

    # ✅ Drop partition/audit columns before merge to avoid duplicates
    booking_df  = booking_df.drop("year", "month", "transformed_at")
    customer_df = customer_df.drop("transformed_at")
    cars_df     = cars_df.drop("transformed_at")
    payments_df = payments_df.drop("transformed_at")

    merged_df = booking_df \
        .join(customer_df, on="customer_id", how="left") \
        .join(cars_df,     on="booking_id",  how="left") \
        .join(payments_df, on="booking_id",  how="left") \
        .withColumn("merged_at", current_timestamp())

    output_path = delta_paths['merged']

    # ✅ IDEMPOTENCY — MERGE on booking_id
    if DeltaTable.isDeltaTable(spark, output_path):
        delta_table = DeltaTable.forPath(spark, output_path)
        delta_table.alias("target").merge(
            merged_df.alias("source"),
            "target.booking_id = source.booking_id"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
        print("✅ Merged UPSERTED (idempotent)")
    else:
        merged_df.write.format("delta").mode("overwrite").save(output_path)
        print("✅ Merged CREATED (first run)")

    total = spark.read.format("delta").load(output_path).count()
    print(f"✅ Merge complete | records: {total:,}")

except Exception as e:
    logger.error(f"❌ Merge Data FAILED: {e}")
    spark.stop()
    sys.exit(1)