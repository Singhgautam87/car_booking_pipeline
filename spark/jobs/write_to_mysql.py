#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys, os
import json
import time
import traceback
from pathlib import Path
import mysql.connector

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))
from config_loader import load_config

config = load_config()
postgres_cfg = config.get_postgres_config()
mysql_cfg = config.get_mysql_config()
tables = config.get_tables()

spark = SparkSession.builder \
    .appName("WriteMySQL") \
    .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar:/opt/spark/jars/mysql-connector-java-8.0.33.jar") \
    .getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("url", config.get_postgres_jdbc_url()) \
    .option("dbtable", tables['staging']) \
    .option("user", postgres_cfg['user']) \
    .option("password", postgres_cfg['password']) \
    .option("driver", postgres_cfg['driver']) \
    .load()

total_count = df.count()

def find_latest_failed_ids():
    paths = [
        Path('/ge/great_expectations/uncommitted/validations'),
        Path('great_expectations/uncommitted/validations')
    ]
    for p in paths:
        if p.exists():
            files = sorted(p.glob('failed_ids_*.json'), reverse=True)
            if files:
                return files[0]
    return None

failed_ids = []
failed_file = find_latest_failed_ids()
if failed_file:
    try:
        with open(failed_file, 'r') as f:
            payload = json.load(f)
            failed_ids = [str(x) for x in payload.get('failed_booking_ids', []) or []]
        print(f"Found failed_ids: {failed_file} ({len(failed_ids)} ids)")
    except Exception as e:
        print(f"Warning: could not read failed_ids file: {e}")

df_to_write = df.filter(~col('booking_id').isin(failed_ids)) if failed_ids else df
write_count = df_to_write.count()
print(f"Staging: {total_count:,} | To write: {write_count:,} | Excluded: {total_count - write_count:,}")

df_to_write.write \
    .format("jdbc") \
    .option("url", config.get_mysql_jdbc_url()) \
    .option("dbtable", tables['final_mysql']) \
    .option("user", mysql_cfg['user']) \
    .option("password", mysql_cfg['password']) \
    .option("driver", mysql_cfg['driver']) \
    .mode("append") \
    .save()

print("✅ Data written to MySQL")

def find_latest_validation_json():
    paths = [
        Path('/ge/great_expectations/uncommitted/validations'),
        Path('great_expectations/uncommitted/validations')
    ]
    for p in paths:
        if p.exists():
            files = sorted(p.glob('validation_result_*.json'), reverse=True)
            if files:
                return files[0]
    return None

def upsert_validation_summary(data, max_retries=3):
    results = data.get('results', [])
    passed = sum(1 for r in results if r.get('success', False))
    failed = len(results) - passed
    success_rate = (passed / len(results) * 100) if results else 0
    overall_status = "PASSED" if data.get('success', False) else "VALIDATION_ISSUES"

    try:
        run_identifier = data.get('meta', {}).get('run_id', {}).get('run_name')
    except Exception:
        run_identifier = None
    if not run_identifier:
        run_identifier = f"run_{int(time.time())}"

    checkpoint_name = data.get('meta', {}).get('checkpoint_name', 'customer_booking_checkpoint')

    insert_sql = """
    INSERT INTO validation_results
    (run_identifier, checkpoint_name, total_expectations, passed_expectations,
     failed_expectations, success_rate, validation_status, validation_details)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      checkpoint_name=VALUES(checkpoint_name),
      total_expectations=VALUES(total_expectations),
      passed_expectations=VALUES(passed_expectations),
      failed_expectations=VALUES(failed_expectations),
      success_rate=VALUES(success_rate),
      validation_status=VALUES(validation_status),
      validation_details=VALUES(validation_details),
      created_at=CURRENT_TIMESTAMP
    """

    for attempt in range(1, max_retries + 1):
        try:
            conn = mysql.connector.connect(
                host=mysql_cfg['host'],
                port=mysql_cfg['port'],
                database=mysql_cfg['database'],
                user=mysql_cfg['user'],
                password=mysql_cfg['password']
            )
            cursor = conn.cursor()
            cursor.execute(insert_sql, (
                run_identifier,
                checkpoint_name,
                len(results),
                passed,
                failed,
                round(success_rate, 2),
                overall_status,
                json.dumps(data)
            ))
            conn.commit()
            cursor.close()
            conn.close()
            print(f"✅ Validation summary stored: {passed}/{len(results)} passed (run_id={run_identifier})")
            return True
        except Exception as e:
            print(f"Attempt {attempt} failed: {e}")
            traceback.print_exc()
            time.sleep(2 ** attempt)

    print("❌ Failed to store validation summary after retries")
    return False

json_file = find_latest_validation_json()
if json_file:
    print(f"Found GE validation JSON: {json_file}")
    try:
        with open(json_file, 'r') as f:
            data = json.load(f)
        upsert_validation_summary(data)
    except Exception as e:
        print(f"Could not process validation JSON: {e}")
else:
    print("No GE validation JSON found")