from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, to_timestamp
import sys, os, logging

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))
from config_loader import load_config

config       = load_config()
minio_cfg    = config.get_minio_config()
postgres_cfg = config.get_postgres_config()
spark_cfg    = config.get_spark_config()
delta_paths  = config.get_delta_paths()
tables       = config.get_tables()

spark = SparkSession.builder \
    .appName("WritePostgres") \
    .config("spark.hadoop.fs.s3a.endpoint",          minio_cfg['endpoint']) \
    .config("spark.hadoop.fs.s3a.access.key",        minio_cfg['access_key']) \
    .config("spark.hadoop.fs.s3a.secret.key",        minio_cfg['secret_key']) \
    .config("spark.hadoop.fs.s3a.path.style.access", str(minio_cfg['path_style']).lower()) \
    .config("spark.jars",                            spark_cfg['postgresql_jar']) \
    .getOrCreate()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

try:
    spark.sparkContext.setLogLevel("WARN")

    merged_df = spark.read.format("delta").load(delta_paths['merged'])

    # ✅ pickup_hour derive karo + audit column drop karo
    final_df = merged_df.drop("merged_at") \
        .withColumn("pickup_hour", hour(to_timestamp("pickup_time", "HH:mm:ss")))

    total = final_df.count()
    print(f"📦 Delta Lake se {total:,} records read kiye")

    # ✅ IDEMPOTENCY — overwrite staging table (truncate + insert)
    final_df.write \
        .format("jdbc") \
        .option("url",      config.get_postgres_jdbc_url()) \
        .option("dbtable",  tables['staging']) \
        .option("user",     postgres_cfg['user']) \
        .option("password", postgres_cfg['password']) \
        .option("driver",   postgres_cfg['driver']) \
        .option("truncate", "true") \
        .mode("overwrite") \
        .save()

    print(f"✅ PostgreSQL staging refreshed | table: {tables['staging']} | records: {total:,}")

except Exception as e:
    logger.error(f"❌ Write Postgres FAILED: {e}")
    spark.stop()
    sys.exit(1)