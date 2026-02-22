from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))
from config_loader import load_config

config = load_config()
minio_cfg = config.get_minio_config()
delta_paths = config.get_delta_paths()

spark = SparkSession.builder \
    .appName("TransformCustomer") \
    .config("spark.hadoop.fs.s3a.endpoint", minio_cfg['endpoint']) \
    .config("spark.hadoop.fs.s3a.access.key", minio_cfg['access_key']) \
    .config("spark.hadoop.fs.s3a.secret.key", minio_cfg['secret_key']) \
    .config("spark.hadoop.fs.s3a.path.style.access", str(minio_cfg['path_style']).lower()) \
    .getOrCreate()

raw_df = spark.read.format("delta").load(delta_paths['raw'])

customer_clean = raw_df.select(
    trim(col("customer.customer_id")).alias("customer_id"),
    trim(col("customer.name")).alias("customer_name"),
    lower(trim(col("customer.email"))).alias("email"),
    lower(trim(col("customer.loyalty.tier"))).alias("loyalty_tier"),
    col("customer.loyalty.points").cast("integer").alias("loyalty_points")
).dropDuplicates(["customer_id"])

customer_clean.write.format("delta") \
    .mode("overwrite") \
    .save(delta_paths['customer_transformed'])

print("✅ Customer transformed")