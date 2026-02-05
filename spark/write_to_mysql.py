from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("WriteMySQL") \
    .getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/booking") \
    .option("dbtable", "customer_booking_staging") \
    .option("user", "admin") \
    .option("password", "admin") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df.write \
    .format("jdbc") \
    .option("url", "jdbc:mysql://mysql:3306/booking") \
    .option("dbtable", "customer_booking_final") \
    .option("user", "admin") \
    .option("password", "admin") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .mode("append") \
    .save()
