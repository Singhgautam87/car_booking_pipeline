from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))
from config_loader import load_config

config      = load_config()
minio_cfg   = config.get_minio_config()
delta_paths = config.get_delta_paths()

RAW_CARS_PATH = delta_paths['raw_cars'].replace("s3a://delta", "s3a://iceberg")
OUTPUT_PATH   = delta_paths['cars_transformed'].replace("s3a://delta", "s3a://iceberg")

spark = SparkSession.builder \
    .appName("TransformCars") \
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

raw_cars_df = spark.read.format("iceberg").load(RAW_CARS_PATH)

cars_clean = raw_cars_df.select(
    trim(col("booking_id")).alias("booking_id"),
    trim(col("car_id")).alias("car_id"),
    trim(col("model")).alias("model"),
    col("price_per_day").cast("double").alias("price_per_day"),
    trim(col("insurance_provider")).alias("insurance_provider"),
    lower(trim(col("insurance_coverage"))).alias("insurance_coverage")
)

cars_clean.write \
    .format("iceberg") \
    .mode("overwrite") \
    .option("overwrite-mode", "dynamic") \
    .save(OUTPUT_PATH)

print(f"✅ Cars transformed → Iceberg: {OUTPUT_PATH}")
print(f"   Total records: {cars_clean.count():,}")