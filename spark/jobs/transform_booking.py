from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, trim
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))
from config_loader import load_config

config = load_config()
minio_cfg = config.get_minio_config()
delta_paths = config.get_delta_paths()

spark = SparkSession.builder \
    .appName("TransformBooking") \
    .config("spark.hadoop.fs.s3a.endpoint", minio_cfg['endpoint']) \
    .config("spark.hadoop.fs.s3a.access.key", minio_cfg['access_key']) \
    .config("spark.hadoop.fs.s3a.secret.key", minio_cfg['secret_key']) \
    .config("spark.hadoop.fs.s3a.path.style.access", str(minio_cfg['path_style']).lower()) \
    .getOrCreate()

raw_df = spark.read.format("delta").load(delta_paths['raw'])

booking_clean = raw_df.select(
    trim(col("booking_id")).alias("booking_id"),
    trim(col("customer.customer_id")).alias("customer_id"),
    to_date(col("booking_date"), "yyyy-MM-dd").alias("booking_date"),
    trim(col("booking_details.pickup.location")).alias("pickup_location"),
    trim(col("booking_details.pickup.time")).alias("pickup_time"),
    trim(col("booking_details.drop.location")).alias("drop_location"),
    trim(col("booking_details.drop.time")).alias("drop_time")
)

booking_clean.write.format("delta") \
    .mode("overwrite") \
    .save(delta_paths['booking_transformed'])

print("✅ Booking transformed")