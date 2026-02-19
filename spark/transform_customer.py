from pyspark.sql.functions import lower, regexp_replace
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("CarBookingTransform") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()


df = spark.read.parquet("s3a://raw/car_booking")

customer_df = df.select(
    "customer_id",
    "customer_name",
    "email",
    regexp_replace("phone", "[^0-9]", "").alias("phone"),
    lower("customer_status").alias("customer_status"),
    "signup_date"
).dropDuplicates(["customer_id"])

customer_df.write.mode("overwrite").parquet("s3a://processed/customer")
