from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, trim, current_timestamp,
    when, lit, round as spark_round, coalesce,
    sum as spark_sum
)
from delta.tables import DeltaTable
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))
from config_loader import load_config

config      = load_config()
minio_cfg   = config.get_minio_config()
delta_paths = config.get_delta_paths()

spark = SparkSession.builder \
    .appName("TransformPayments") \
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

    raw_payments_df = spark.read.format("delta").load(delta_paths['raw_payments'])

    payments_clean = raw_payments_df.select(
        trim(col("booking_id")).alias("booking_id"),
        trim(col("payment_id")).alias("payment_id"),
        lower(trim(col("payment_method"))).alias("payment_method"),
        spark_round(col("payment_amount").cast("double"), 2).alias("payment_amount"),
    )

    # ✅ Payment method standardize — actual data mein:
    # "Debit Card", "Credit Card", "UPI" hain
    payments_clean = payments_clean.withColumn(
        "payment_method",
        when(lower(col("payment_method")).isin("credit card","credit_card"), lit("credit_card"))
        .when(lower(col("payment_method")).isin("debit card","debit_card"),  lit("debit_card"))
        .when(lower(col("payment_method")).isin("upi","gpay","phonepe"),     lit("upi"))
        .when(lower(col("payment_method")).isin("cash","cod"),               lit("cash"))
        .otherwise(lower(col("payment_method")))
    )

    # ✅ Payment type classify
    payments_clean = payments_clean.withColumn(
        "is_digital_payment",
        when(col("payment_method").isin("credit_card","debit_card","upi"), True)
        .otherwise(False)
    )

    # ✅ Amount tier — actual data range 3941 to 13407
    payments_clean = payments_clean.withColumn(
        "amount_tier",
        when(col("payment_amount") >= 10000, lit("high"))
        .when(col("payment_amount") >= 5000,  lit("medium"))
        .otherwise(lit("low"))
    )

    # ✅ Split payment detect — ek booking mein multiple payments hain
    # B008296 → P23699 (3941) + P19133 (13356) = split payment
    booking_payment_count = raw_payments_df \
        .groupBy("booking_id") \
        .count() \
        .withColumnRenamed("count", "payment_count")

    payments_clean = payments_clean.join(
        booking_payment_count, on="booking_id", how="left"
    ).withColumn(
        "is_split_payment",
        when(col("payment_count") > 1, True).otherwise(False)
    ).withColumn("transformed_at", current_timestamp())

    output_path = delta_paths['payments_transformed']

    # ✅ IDEMPOTENCY — MERGE on payment_id
    if DeltaTable.isDeltaTable(spark, output_path):
        DeltaTable.forPath(spark, output_path).alias("target").merge(
            payments_clean.alias("source"),
            "target.payment_id = source.payment_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        print("✅ Payments MERGED (idempotent)")
    else:
        payments_clean.write.format("delta").mode("overwrite").save(output_path)
        print("✅ Payments CREATED (first run)")

    total = spark.read.format("delta").load(output_path).count()
    print(f"✅ Payments transformed | records: {total:,}")

except Exception as e:
    logger.error(f"❌ Transform Payments FAILED: {e}")
    spark.stop()
    sys.exit(1)