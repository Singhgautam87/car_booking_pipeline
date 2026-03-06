from pyspark.sql import SparkSession
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))
from config_loader import load_config

config       = load_config()
minio_cfg    = config.get_minio_config()
postgres_cfg = config.get_postgres_config()
delta_paths  = config.get_delta_paths()
tables       = config.get_tables()

MERGED_PATH = delta_paths['merged'].replace("s3a://delta", "s3a://iceberg")

spark = SparkSession.builder \
    .appName("WritePostgres") \
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
    .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://iceberg/warehouse") \
    .config("spark.hadoop.fs.s3a.endpoint",          minio_cfg['endpoint']) \
    .config("spark.hadoop.fs.s3a.access.key",        minio_cfg['access_key']) \
    .config("spark.hadoop.fs.s3a.secret.key",        minio_cfg['secret_key']) \
    .config("spark.hadoop.fs.s3a.path.style.access", str(minio_cfg['path_style']).lower()) \
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ── Read from Iceberg ─────────────────────────────────────
merged_df = spark.read.format("iceberg").load(MERGED_PATH)

total = merged_df.count()
print(f"📦 Iceberg se {total:,} records read kiye")

# ── Write to PostgreSQL staging ───────────────────────────
merged_df.write \
    .format("jdbc") \
    .option("url",      config.get_postgres_jdbc_url()) \
    .option("dbtable",  tables['staging']) \
    .option("user",     postgres_cfg['user']) \
    .option("password", postgres_cfg['password']) \
    .option("driver",   postgres_cfg['driver']) \
    .mode("overwrite") \
    .save()

print(f"✅ Data written to PostgreSQL staging: {tables['staging']}")
print(f"   Total records: {total:,}")