import os
import sys
import logging

from common import get_spark_session

# spark session
spark = get_spark_session("ETL", "thrift://hive:9083", "http://minio:9000",
                          "spark", "spark12345")
# Set log4j
spark.sparkContext.setLogLevel("ERROR")
log4jLogger = spark._jvm.org.apache.log4j
logger = log4jLogger.LogManager.getLogger("LOGGER")
logger.setLevel(log4jLogger.Level.INFO)

spark.sql("""SELECT * FROM delta_bitcoin WHERE timestamp = 1424702340""").show()
# update
spark.sql(
    """UPDATE delta_bitcoin SET low = 237.2 WHERE timestamp = 1424705640;""").show()
spark.sql("""SELECT * FROM delta_bitcoin WHERE timestamp = 1424705640""").show()
# delete
spark.sql("DELETE FROM delta.`s3a://datalake/deltatables/bitcoin/` WHERE timestamp = 1429197120").show()
spark.sql("DELETE FROM delta_bitcoin WHERE timestamp = 1424702340").show()
# spark.sql("""SELECT * FROM delta_bitcoin WHERE timestamp = 1424702340""").show()

spark.stop()
