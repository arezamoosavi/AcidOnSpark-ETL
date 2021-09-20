import os
import sys
import logging

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def get_spark_session(appname, minio_url,
                      minio_access_key, minio_secret_key):

    sc = (SparkSession.builder
          .appName(appname)
          .config("spark.network.timeout", "10000s")
          .config("spark.hadoop.fs.s3a.multiobjectdelete.enable", "true")
          .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
          .config("spark.hadoop.fs.s3a.fast.upload", "true")
          .config("spark.hadoop.fs.s3a.endpoint", minio_url)
          .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
          .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
          .config("spark.hadoop.fs.s3a.path.style.access", "true")
          .config("spark.history.fs.logDirectory", "s3a://spark-logs/")
          .config("spark.sql.files.ignoreMissingFiles", "true")
          .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
          .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
          .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
          .getOrCreate())
    return sc


def extract(sc, bucket_name, raw_data_path):
    return (sc.read.format("com.databricks.spark.csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("delimiter", ",")
            .option("mode", "DROPMALFORMED")
            .load('s3a://' + os.path.os.path.join(bucket_name,
                                                  raw_data_path)))


# spark session
spark = get_spark_session("ETL", "http://minio:9000",
                          "spark", "spark12345")

# Set log4j
log4jLogger = spark._jvm.org.apache.log4j
logger = log4jLogger.LogManager.getLogger("ETL_LOGGER")
logger.setLevel(log4jLogger.Level.INFO)

# sdf = sdf.persist(StorageLevel.DISK_ONLY)
# sdf.unpersist()

sdf = extract(spark, "raw-data", "bitcoinity_data.csv")
sdf = sdf.withColumn("Time", sdf["Time"].cast("timestamp").alias("Time"))

sdf.printSchema()
sdf.show(5)

sdf.write.format("delta").mode("overwrite").save('s3a://raw-data/delta')

spark.stop()
