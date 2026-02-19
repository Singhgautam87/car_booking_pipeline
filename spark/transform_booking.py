from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("TransformBooking") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()
df = spark.read.parquet("s3a://raw/car_booking")

booking_df = df.select(
    "booking_id",
    "customer_id",
    "start_time",
    "end_time",
    "booking_status",
    "booking_date",
    "car_type",
    "pickup_city"
)

booking_df.write.mode("overwrite").parquet("s3a://processed/booking")
