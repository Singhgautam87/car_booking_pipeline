from pyspark.sql import SparkSession
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))
from config_loader import load_config

config = load_config()
minio_cfg = config.get_minio_config()
delta_paths = config.get_delta_paths()

spark = SparkSession.builder \
    .appName("MergeAll") \
    .config("spark.hadoop.fs.s3a.endpoint", minio_cfg['endpoint']) \
    .config("spark.hadoop.fs.s3a.access.key", minio_cfg['access_key']) \
    .config("spark.hadoop.fs.s3a.secret.key", minio_cfg['secret_key']) \
    .config("spark.hadoop.fs.s3a.path.style.access", str(minio_cfg['path_style']).lower()) \
    .getOrCreate()

customer_df = spark.read.format("delta").load(delta_paths['customer_transformed'])
booking_df  = spark.read.format("delta").load(delta_paths['booking_transformed'])
cars_df     = spark.read.format("delta").load(delta_paths['cars_transformed'])
payments_df = spark.read.format("delta").load(delta_paths['payments_transformed'])

merged_df = booking_df \
    .join(customer_df, on="customer_id", how="left") \
    .join(cars_df,     on="booking_id",  how="left") \
    .join(payments_df, on="booking_id",  how="left")

merged_df.write.format("delta") \
    .mode("overwrite") \
    .save(delta_paths['merged'])

print("✅ All data merged")