from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("WritePostgres") \
    .getOrCreate()

df = spark.read.parquet("s3a://curated/customer_booking")

df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/booking") \
    .option("dbtable", "customer_booking_staging") \
    .option("user", "admin") \
    .option("password", "admin") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()
