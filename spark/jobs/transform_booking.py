from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, trim, year, month, current_timestamp,
    when, lit, to_timestamp, hour, regexp_extract
)
from delta.tables import DeltaTable
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))
from config_loader import load_config

config      = load_config()
minio_cfg   = config.get_minio_config()
delta_paths = config.get_delta_paths()

spark = SparkSession.builder \
    .appName("TransformBooking") \
    .config("spark.hadoop.fs.s3a.endpoint",          minio_cfg['endpoint']) \
    .config("spark.hadoop.fs.s3a.access.key",        minio_cfg['access_key']) \
    .config("spark.hadoop.fs.s3a.secret.key",        minio_cfg['secret_key']) \
    .config("spark.hadoop.fs.s3a.path.style.access", str(minio_cfg['path_style']).lower()) \
    .getOrCreate()


import sys
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

try:
    spark.sparkContext.setLogLevel("WARN")

    raw_df = spark.read.format("delta").load(delta_paths['raw'])

    booking_clean = raw_df.select(
        trim(col("booking_id")).alias("booking_id"),
        trim(col("customer.customer_id")).alias("customer_id"),
        to_date(col("booking_date"), "yyyy-MM-dd").alias("booking_date"),
        trim(col("booking_details.pickup.location")).alias("pickup_location"),
        trim(col("booking_details.pickup.time")).alias("pickup_time"),
        trim(col("booking_details.drop.location")).alias("drop_location"),
        trim(col("booking_details.drop.time")).alias("drop_time"),
    )

    # ✅ Pickup hour extract — "3:00 AM" → 3
    # Business insight: Peak hours kaun se hain?
    booking_clean = booking_clean.withColumn(
        "pickup_hour",
        regexp_extract(col("pickup_time"), r"(\d+):", 1).cast("integer")
    ).withColumn(
        "pickup_hour",
        when(col("pickup_time").contains("PM") & (col("pickup_hour") != 12),
             col("pickup_hour") + 12)
        .when(col("pickup_time").contains("AM") & (col("pickup_hour") == 12),
             lit(0))
        .otherwise(col("pickup_hour"))
    )

    # ✅ Time slot classify — data mein 3:00 AM, 9:00 PM type times hain
    booking_clean = booking_clean.withColumn(
        "pickup_slot",
        when((col("pickup_hour") >= 0)  & (col("pickup_hour") < 6),  lit("late_night"))
        .when((col("pickup_hour") >= 6)  & (col("pickup_hour") < 12), lit("morning"))
        .when((col("pickup_hour") >= 12) & (col("pickup_hour") < 18), lit("afternoon"))
        .otherwise(lit("evening"))
    )

    # ✅ Trip type — same city vs different city
    # Delhi Airport → Noida = intercity
    # Delhi Airport → Delhi Airport = local
    booking_clean = booking_clean.withColumn(
        "trip_type",
        when(col("pickup_location") == col("drop_location"), lit("local"))
        .otherwise(lit("intercity"))
    )

    # ✅ Route — "Delhi Airport → Noida" — analytics ke liye
    booking_clean = booking_clean.withColumn(
        "route",
        col("pickup_location").cast("string")
        .concat(lit(" → "))
        .concat(col("drop_location").cast("string"))
    )

    # ✅ Partition columns
    booking_clean = booking_clean \
        .withColumn("booking_year",  year(col("booking_date"))) \
        .withColumn("booking_month", month(col("booking_date"))) \
        .withColumn("transformed_at", current_timestamp())

    output_path = delta_paths['booking_transformed']

    # ✅ IDEMPOTENCY — MERGE
    if DeltaTable.isDeltaTable(spark, output_path):
        DeltaTable.forPath(spark, output_path).alias("target").merge(
            booking_clean.alias("source"),
            "target.booking_id = source.booking_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        print("✅ Booking MERGED (idempotent)")
    else:
        booking_clean.write \
            .format("delta") \
            .partitionBy("booking_year", "booking_month") \
            .mode("overwrite") \
            .save(output_path)
        print("✅ Booking CREATED (first run)")

    total = spark.read.format("delta").load(output_path).count()
    print(f"✅ Booking transformed | records: {total:,}")

except Exception as e:
    logger.error(f"❌ Transform Booking FAILED: {e}")
    spark.stop()
    sys.exit(1)