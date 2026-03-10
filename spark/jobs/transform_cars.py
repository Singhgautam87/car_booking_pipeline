from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, trim, current_timestamp,
    when, lit, round as spark_round,
    array_contains, size
)
from delta.tables import DeltaTable
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))
from config_loader import load_config

config      = load_config()
minio_cfg   = config.get_minio_config()
delta_paths = config.get_delta_paths()

spark = SparkSession.builder \
    .appName("TransformCars") \
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

    # Raw cars already exploded hain ingest_stream se
    raw_cars_df = spark.read.format("delta").load(delta_paths['raw_cars'])

    cars_clean = raw_cars_df.select(
        trim(col("booking_id")).alias("booking_id"),
        trim(col("car_id")).alias("car_id"),
        trim(col("model")).alias("model"),
        spark_round(col("price_per_day").cast("double"), 2).alias("price_per_day"),
        trim(col("insurance_provider")).alias("insurance_provider"),
        lower(trim(col("insurance_coverage"))).alias("insurance_coverage"),
    )

    # ✅ Price tier — actual data mein 1200-3900 range hai
    # economy < 1500, standard 1500-3000, premium > 3000
    cars_clean = cars_clean.withColumn(
        "price_tier",
        when(col("price_per_day") >= 3000, lit("premium"))
        .when(col("price_per_day") >= 1500, lit("standard"))
        .otherwise(lit("economy"))
    )

    # ✅ Insurance standardize — HDFC/ICICI/TATA AIG → provider clean
    # Full/Partial → standardize
    cars_clean = cars_clean.withColumn(
        "insurance_coverage",
        when(lower(col("insurance_coverage")) == "full",    lit("full"))
        .when(lower(col("insurance_coverage")) == "partial", lit("partial"))
        .otherwise(lit("none"))
    ).withColumn(
        "has_full_insurance",
        when(col("insurance_coverage") == "full", True).otherwise(False)
    )

    # ✅ Insurance provider category — Indian providers
    # HDFC/ICICI = private bank, TATA AIG = general insurance
    cars_clean = cars_clean.withColumn(
        "insurer_type",
        when(col("insurance_provider").isin("HDFC", "ICICI"), lit("private_bank"))
        .when(col("insurance_provider") == "TATA AIG",         lit("general_insurance"))
        .otherwise(lit("other"))
    )

    # ✅ Car segment — actual models from data
    # Thar = adventure, Creta/Nexon = SUV, i20/Baleno = hatchback, Venue = compact_suv
    cars_clean = cars_clean.withColumn(
        "car_segment",
        when(col("model") == "Thar",   lit("adventure"))
        .when(col("model").isin("Creta", "Nexon"), lit("suv"))
        .when(col("model").isin("i20", "Baleno"),  lit("hatchback"))
        .when(col("model") == "Venue",             lit("compact_suv"))
        .otherwise(lit("other"))
    ).withColumn("transformed_at", current_timestamp())

    output_path = delta_paths['cars_transformed']

    # ✅ IDEMPOTENCY — MERGE on booking_id + car_id
    if DeltaTable.isDeltaTable(spark, output_path):
        DeltaTable.forPath(spark, output_path).alias("target").merge(
            cars_clean.alias("source"),
            "target.booking_id = source.booking_id AND target.car_id = source.car_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        print("✅ Cars MERGED (idempotent)")
    else:
        cars_clean.write.format("delta").mode("overwrite").save(output_path)
        print("✅ Cars CREATED (first run)")

    total = spark.read.format("delta").load(output_path).count()
    print(f"✅ Cars transformed | records: {total:,}")

except Exception as e:
    logger.error(f"❌ Transform Cars FAILED: {e}")
    spark.stop()
    sys.exit(1)