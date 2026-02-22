from pyspark.sql import SparkSession
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))
from config_loader import load_config

config = load_config()
minio_cfg = config.get_minio_config()
postgres_cfg = config.get_postgres_config()
delta_paths = config.get_delta_paths()
tables = config.get_tables()

spark = SparkSession.builder \
    .appName("WritePostgres") \
    .config("spark.hadoop.fs.s3a.endpoint", minio_cfg['endpoint']) \
    .config("spark.hadoop.fs.s3a.access.key", minio_cfg['access_key']) \
    .config("spark.hadoop.fs.s3a.secret.key", minio_cfg['secret_key']) \
    .config("spark.hadoop.fs.s3a.path.style.access", str(minio_cfg['path_style']).lower()) \
    .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar") \
    .getOrCreate()

merged_df = spark.read.format("delta").load(delta_paths['merged'])

merged_df.write \
    .format("jdbc") \
    .option("url", config.get_postgres_jdbc_url()) \
    .option("dbtable", tables['staging']) \
    .option("user", postgres_cfg['user']) \
    .option("password", postgres_cfg['password']) \
    .option("driver", postgres_cfg['driver']) \
    .mode("overwrite") \
    .save()

print("✅ Data written to PostgreSQL staging")