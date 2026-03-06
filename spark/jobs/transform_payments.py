from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))
from config_loader import load_config

config      = load_config()
minio_cfg   = config.get_minio_config()
delta_paths = config.get_delta_paths()

RAW_PAY_PATH = delta_paths['raw_payments'].replace("s3a://delta", "s3a://iceberg")
OUTPUT_PATH  = delta_paths['payments_transformed'].replace("s3a://delta", "s3a://iceberg")

spark = SparkSession.builder \
    .appName("TransformPayments") \
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

raw_payments_df = spark.read.format("iceberg").load(RAW_PAY_PATH)

payments_clean = raw_payments_df.select(
    trim(col("booking_id")).alias("booking_id"),
    trim(col("payment_id")).alias("payment_id"),
    lower(trim(col("payment_method"))).alias("payment_method"),
    col("payment_amount").cast("double").alias("payment_amount")
)

payments_clean.write \
    .format("iceberg") \
    .mode("overwrite") \
    .option("overwrite-mode", "dynamic") \
    .save(OUTPUT_PATH)

print(f"✅ Payments transformed → Iceberg: {OUTPUT_PATH}")
print(f"   Total records: {payments_clean.count():,}")