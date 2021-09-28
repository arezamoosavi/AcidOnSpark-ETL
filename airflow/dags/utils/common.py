import os
import sys
import logging

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def get_spark_session(appname, hive_metastore, minio_url,
                      minio_access_key, minio_secret_key):

    spark = (SparkSession.builder
             .appName(appname)
             .config("spark.network.timeout", "10000s")
             .config("hive.metastore.uris", hive_metastore)
             .config("hive.exec.dynamic.partition", "true")
             .config("hive.exec.dynamic.partition.mode", "nonstrict")
             .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
             .config("spark.hadoop.fs.s3a.multiobjectdelete.enable", "true")
             .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
             .config("spark.hadoop.fs.s3a.fast.upload", "true")
             .config("spark.hadoop.fs.s3a.endpoint", minio_url)
             .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
             .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.history.fs.logDirectory", "s3a://spark/")
             .config("spark.sql.files.ignoreMissingFiles", "true")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
             .enableHiveSupport()
             .getOrCreate())
    return spark


def date_partition(ts):
    return str(ts).split(" ")[0]


def data_transformations(sdf):
    "Timestamp,Open,High,Low,Close,Volume_(BTC),Volume_(Currency),Weighted_Price"

    udf_date_partition = F.udf(date_partition, "string")

    sdf = sdf.withColumnRenamed("Volume_(BTC)", "btc_volume")
    sdf = sdf.withColumnRenamed("Volume_(Currency)", "curr_volume")

    sdf = sdf.withColumn("created", sdf["Timestamp"].cast("timestamp"))

    sdf = sdf.withColumn("ds", udf_date_partition(
        F.col("created")).cast("string")
    )

    sdf = sdf.select([F.col(x).alias(x.lower()) for x in sdf.columns])

    # sdf.filter(sdf.created.isNull()).show()
    # sdf.where(sdf.timestamp.contains("T")).show()

    sdf = sdf.select(["created", "open", "high", "low",
                      "close", "btc_volume", "curr_volume",
                      "weighted_price", "timestamp", "ds"])
    return sdf


def write_mariadb(sdf, host, user, password, database, table, mode="append"):
    maria_properties = {
        "driver": "org.mariadb.jdbc.Driver",
        "user": user,
        "password": password,
    }
    maria_url = f"jdbc:mysql://{host}:3306/{database}?user={user}&password={password}"

    sdf.write.jdbc(
        url=maria_url, table=table, mode=mode, properties=maria_properties
    )
