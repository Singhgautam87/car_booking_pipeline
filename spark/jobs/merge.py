from pyspark.sql import SparkSession
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))
from config_loader import load_config

config      = load_config()
minio_cfg   = config.get_minio_config()
delta_paths = config.get_delta_paths()

CUSTOMER_PATH = delta_paths['customer_transformed'].replace("s3a://delta", "s3a://iceberg")
BOOKING_PATH  = delta_paths['booking_transformed'].replace("s3a://delta",  "s3a://iceberg")
CARS_PATH     = delta_paths['cars_transformed'].replace("s3a://delta",     "s3a://iceberg")
PAYMENTS_PATH = delta_paths['payments_transformed'].replace("s3a://delta", "s3a://iceberg")
MERGED_PATH   = delta_paths['merged'].replace("s3a://delta",               "s3a://iceberg")

spark = SparkSession.builder \
    .appName("MergeAll") \
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
    .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://iceberg/warehouse") \
    .config("spark.hadoop.fs.s3a.endpoint",          minio_cfg['endpoint']) \
    .config("spark.hadoop.fs.s3a.access.key",        minio_cfg['access_key']) \
    .config("spark.hadoop.fs.s3a.secret.key",        minio_cfg['secret_key']) \
    .config("spark.hadoop.fs.s3a.path.style.access", str(minio_cfg['path_style']).lower()) \
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

customer_df = spark.read.format("iceberg").load(CUSTOMER_PATH)
booking_df  = spark.read.format("iceberg").load(BOOKING_PATH)
cars_df     = spark.read.format("iceberg").load(CARS_PATH)
payments_df = spark.read.format("iceberg").load(PAYMENTS_PATH)

merged_df = booking_df \
    .join(customer_df, on="customer_id", how="left") \
    .join(cars_df,     on="booking_id",  how="left") \
    .join(payments_df, on="booking_id",  how="left")

merged_df.write \
    .format("iceberg") \
    .mode("overwrite") \
    .option("overwrite-mode", "dynamic") \
    .save(MERGED_PATH)

total = merged_df.count()
print(f"✅ All data merged → Iceberg: {MERGED_PATH}")
print(f"   Total records: {total:,}")