from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))
from config_loader import load_config

config = load_config()
minio_cfg = config.get_minio_config()
delta_paths = config.get_delta_paths()

spark = SparkSession.builder \
    .appName("TransformPayments") \
    .config("spark.hadoop.fs.s3a.endpoint", minio_cfg['endpoint']) \
    .config("spark.hadoop.fs.s3a.access.key", minio_cfg['access_key']) \
    .config("spark.hadoop.fs.s3a.secret.key", minio_cfg['secret_key']) \
    .config("spark.hadoop.fs.s3a.path.style.access", str(minio_cfg['path_style']).lower()) \
    .getOrCreate()

raw_payments_df = spark.read.format("delta").load(delta_paths['raw_payments'])

payments_clean = raw_payments_df.select(
    trim(col("booking_id")).alias("booking_id"),
    trim(col("payment_id")).alias("payment_id"),
    lower(trim(col("payment_method"))).alias("payment_method"),
    col("payment_amount").cast("double").alias("payment_amount")
)

payments_clean.write.format("delta") \
    .mode("overwrite") \
    .save(delta_paths['payments_transformed'])

print("✅ Payments transformed")