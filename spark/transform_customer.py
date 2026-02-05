from pyspark.sql.functions import lower, regexp_replace

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
