from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))
from config_loader import load_config

config = load_config()
minio_cfg = config.get_minio_config()
delta_paths = config.get_delta_paths()

spark = SparkSession.builder \
    .appName("TransformCars") \
    .config("spark.hadoop.fs.s3a.endpoint", minio_cfg['endpoint']) \
    .config("spark.hadoop.fs.s3a.access.key", minio_cfg['access_key']) \
    .config("spark.hadoop.fs.s3a.secret.key", minio_cfg['secret_key']) \
    .config("spark.hadoop.fs.s3a.path.style.access", str(minio_cfg['path_style']).lower()) \
    .getOrCreate()

raw_cars_df = spark.read.format("delta").load(delta_paths['raw_cars'])

cars_clean = raw_cars_df.select(
    trim(col("booking_id")).alias("booking_id"),
    trim(col("car_id")).alias("car_id"),
    trim(col("model")).alias("model"),
    col("price_per_day").cast("double").alias("price_per_day"),
    trim(col("insurance_provider")).alias("insurance_provider"),
    lower(trim(col("insurance_coverage"))).alias("insurance_coverage")
)

cars_clean.write.format("delta") \
    .mode("overwrite") \
    .save(delta_paths['cars_transformed'])

print("✅ Cars transformed")