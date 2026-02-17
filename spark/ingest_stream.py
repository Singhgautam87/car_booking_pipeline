from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("KafkaIngest") \
    .getOrCreate()

schema = StructType([
    StructField("booking_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("start_time", StringType()),
    StructField("end_time", StringType()),
    StructField("booking_status", StringType()),
    StructField("booking_date", StringType()),
    StructField("car_type", StringType()),
    StructField("pickup_city", StringType()),
    StructField("customer_name", StringType()),
    StructField("email", StringType()),
    StructField("phone", StringType()),
    StructField("customer_status", StringType()),
    StructField("signup_date", StringType())
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "raw_car_booking") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .option("kafka.metadata.max.age.ms", "5000") \
    .option("kafka.session.timeout.ms", "30000")  \ 
    .load()

json_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

query = json_df.writeStream \
    .format("parquet") \
    .option("path", "s3a://raw/car_booking") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()

query.awaitTermination()
