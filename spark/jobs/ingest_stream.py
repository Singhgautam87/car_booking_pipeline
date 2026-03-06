from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import *
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))
from config_loader import load_config

config      = load_config()
minio_cfg   = config.get_minio_config()
kafka_cfg   = config.get_kafka_config()
delta_paths = config.get_delta_paths()

# ── Iceberg paths ─────────────────────────────────────────
RAW_PATH       = delta_paths['raw'].replace("s3a://delta", "s3a://iceberg")
RAW_CARS_PATH  = delta_paths['raw_cars'].replace("s3a://delta", "s3a://iceberg")
RAW_PAY_PATH   = delta_paths['raw_payments'].replace("s3a://delta", "s3a://iceberg")

spark = SparkSession.builder \
    .appName("KafkaIngest") \
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
    .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://iceberg/warehouse") \
    .config("spark.sql.catalog.local",
            "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "s3a://iceberg/warehouse") \
    .config("spark.hadoop.fs.s3a.endpoint",          minio_cfg['endpoint']) \
    .config("spark.hadoop.fs.s3a.access.key",        minio_cfg['access_key']) \
    .config("spark.hadoop.fs.s3a.secret.key",        minio_cfg['secret_key']) \
    .config("spark.hadoop.fs.s3a.path.style.access", str(minio_cfg['path_style']).lower()) \
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("booking_id",   StringType()),
    StructField("booking_date", StringType()),
    StructField("customer", StructType([
        StructField("customer_id", StringType()),
        StructField("name",        StringType()),
        StructField("email",       StringType()),
        StructField("loyalty", StructType([
            StructField("tier",   StringType()),
            StructField("points", IntegerType())
        ]))
    ])),
    StructField("booking_details", StructType([
        StructField("pickup", StructType([
            StructField("location", StringType()),
            StructField("time",     StringType())
        ])),
        StructField("drop", StructType([
            StructField("location", StringType()),
            StructField("time",     StringType())
        ]))
    ])),
    StructField("cars", ArrayType(StructType([
        StructField("car_id",        StringType()),
        StructField("model",         StringType()),
        StructField("price_per_day", DoubleType()),
        StructField("features",      ArrayType(StringType())),
        StructField("insurance", StructType([
            StructField("provider",      StringType()),
            StructField("coverage_type", StringType())
        ]))
    ]))),
    StructField("payments", ArrayType(StructType([
        StructField("payment_id", StringType()),
        StructField("method",     StringType()),
        StructField("amount",     DoubleType())
    ])))
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_cfg['bootstrap_servers']) \
    .option("subscribe",               kafka_cfg['topic']) \
    .option("startingOffsets",         kafka_cfg['starting_offsets']) \
    .option("failOnDataLoss",          "false") \
    .option("kafka.metadata.max.age.ms",    kafka_cfg['max_age_ms']) \
    .option("kafka.session.timeout.ms",     kafka_cfg['session_timeout_ms']) \
    .load()

parsed_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# ── Raw booking + customer ────────────────────────────────
raw_df = parsed_df.select(
    col("booking_id"),
    col("booking_date"),
    col("customer"),
    col("booking_details")
)

# ── Cars explode + flatten ────────────────────────────────
raw_cars_df = parsed_df.select(
    col("booking_id"),
    explode(col("cars")).alias("car")
).select(
    col("booking_id"),
    col("car.car_id").alias("car_id"),
    col("car.model").alias("model"),
    col("car.price_per_day").alias("price_per_day"),
    col("car.insurance.provider").alias("insurance_provider"),
    col("car.insurance.coverage_type").alias("insurance_coverage")
)

# ── Payments explode + flatten ────────────────────────────
raw_payments_df = parsed_df.select(
    col("booking_id"),
    explode(col("payments")).alias("payment")
).select(
    col("booking_id"),
    col("payment.payment_id").alias("payment_id"),
    col("payment.method").alias("payment_method"),
    col("payment.amount").alias("payment_amount")
)

# ── Write streams → Iceberg format ───────────────────────
raw_query = raw_df.writeStream \
    .format("iceberg") \
    .option("path",               RAW_PATH) \
    .option("checkpointLocation", "/tmp/checkpoint_raw") \
    .outputMode("append") \
    .start()

cars_query = raw_cars_df.writeStream \
    .format("iceberg") \
    .option("path",               RAW_CARS_PATH) \
    .option("checkpointLocation", "/tmp/checkpoint_raw_cars") \
    .outputMode("append") \
    .start()

payments_query = raw_payments_df.writeStream \
    .format("iceberg") \
    .option("path",               RAW_PAY_PATH) \
    .option("checkpointLocation", "/tmp/checkpoint_raw_payments") \
    .outputMode("append") \
    .start()

raw_query.awaitTermination(timeout=120)
cars_query.awaitTermination(timeout=120)
payments_query.awaitTermination(timeout=120)

print("✅ All streams written to Iceberg!")