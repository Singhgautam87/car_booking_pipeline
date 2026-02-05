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
