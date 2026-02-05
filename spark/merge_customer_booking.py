from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MergeCustomerBooking") \
    .getOrCreate()

booking_df = spark.read.parquet("s3a://processed/booking")
customer_df = spark.read.parquet("s3a://processed/customer")

final_df = booking_df.join(
    customer_df,
    on="customer_id",
    how="left"
)

final_df.write.mode("overwrite").parquet("s3a://curated/customer_booking")
