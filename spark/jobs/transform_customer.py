from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, trim, current_timestamp,
    when, split, length, coalesce, lit
)
from delta.tables import DeltaTable
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))
from config_loader import load_config

config      = load_config()
minio_cfg   = config.get_minio_config()
delta_paths = config.get_delta_paths()

spark = SparkSession.builder \
    .appName("TransformCustomer") \
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

    customer_clean = raw_df.select(
        trim(col("customer.customer_id")).alias("customer_id"),
        trim(col("customer.name")).alias("customer_name"),
        lower(trim(col("customer.email"))).alias("email"),
        lower(trim(col("customer.loyalty.tier"))).alias("loyalty_tier"),
        col("customer.loyalty.points").cast("integer").alias("loyalty_points"),
    ).dropDuplicates(["customer_id"])

    # ✅ Email domain extract — user33186@example.com → example.com
    customer_clean = customer_clean.withColumn(
        "email_domain",
        when(col("email").contains("@"),
             split(col("email"), "@").getItem(1)
        ).otherwise(lit("unknown"))
    )

    # ✅ Loyalty tier standardize — Silver/SILVER/silver → silver
    customer_clean = customer_clean.withColumn(
        "loyalty_tier",
        when(col("loyalty_tier").isin("platinum","gold","silver","bronze"),
             col("loyalty_tier")
        ).otherwise(lit("bronze"))
    )

    # ✅ Loyalty numeric rank — interview mein poochte hain
    # "Kaise sort kiya tiers ko?"
    customer_clean = customer_clean.withColumn(
        "loyalty_rank",
        when(col("loyalty_tier") == "platinum", 4)
        .when(col("loyalty_tier") == "gold",    3)
        .when(col("loyalty_tier") == "silver",  2)
        .otherwise(1)
    )

    # ✅ High value customer flag — Gold + Platinum
    # Business logic: Premium customers ko alag treat karo
    customer_clean = customer_clean.withColumn(
        "is_high_value_customer",
        when(col("loyalty_rank") >= 3, True).otherwise(False)
    )

    # ✅ Points tier — Bronze(0-999) Silver(1000-4999) Gold(5000-9999) Platinum(10000+)
    customer_clean = customer_clean.withColumn(
        "loyalty_points", coalesce(col("loyalty_points"), lit(0))
    ).withColumn(
        "points_tier",
        when(col("loyalty_points") >= 10000, lit("platinum_range"))
        .when(col("loyalty_points") >= 5000,  lit("gold_range"))
        .when(col("loyalty_points") >= 1000,  lit("silver_range"))
        .otherwise(lit("bronze_range"))
    ).withColumn("transformed_at", current_timestamp())

    output_path = delta_paths['customer_transformed']

    # ✅ IDEMPOTENCY — MERGE
    if DeltaTable.isDeltaTable(spark, output_path):
        DeltaTable.forPath(spark, output_path).alias("target").merge(
            customer_clean.alias("source"),
            "target.customer_id = source.customer_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        print("✅ Customer MERGED (idempotent)")
    else:
        customer_clean.write.format("delta").mode("overwrite").save(output_path)
        print("✅ Customer CREATED (first run)")

    total = spark.read.format("delta").load(output_path).count()
    print(f"✅ Customer transformed | records: {total:,}")

except Exception as e:
    logger.error(f"❌ Transform Customer FAILED: {e}")
    spark.stop()
    sys.exit(1)