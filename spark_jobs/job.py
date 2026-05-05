from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("S3 Test") \
    .getOrCreate()

spark._jsc.hadoopConfiguration().set(
    "fs.s3a.impl",
    "org.apache.hadoop.fs.s3a.S3AFileSystem"
)

df = spark.range(0, 10)
df.write.format("parquet").save("s3a://dbt-snow-bucket/airflow_tut/movies1")
spark.stop()