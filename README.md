# AcidOnSpark-ETL
Watch about it here:
## [youtube](https://www.youtube.com/watch?v=C4UiqIc_gWA)
Read about it here:
## [medium](https://sdamoosavi.medium.com/doing-acid-on-spark-6ef54f3d1a8d)

## Technologies
The main technologies are:
* Python3
* Docker
* Spark
* Airflow
* MinIo
* DeltaTable
* Hive
* Mariadb
* Presto
* Superset

## How is it work?
I this section each part of this ETL pipeline will be illustrated:
### Spark
Spark is used to read and write data in distributed and scalable manner.
```bash
make spark
```
will run spark master and one instance of worker
```bash
make scale-spark
```
will scale spark worker.
### Airflow
One of the best workflow managemnet for spark jobs.
```bash
make airflow
```
### MinIo
An opensource, distributed and performant object storage for datalake files and hive tables.
```bash
make minio
```
### DeltaTable (Deltalake)
An opensource columnar parquet files formats with snappy compression. Delta supports update and delete, which is very nice. All necessary jar files for supporting delta and s3 objects are added to hive and spark docker images.
### Hive and Mariadb
In order to create tables to run Spark SQL on delta tables, spark needs hive metastore and hive needs mariadb as metastoreDb. Mariadb is also used for data warehouse for to run query faster and create dashboards.
```bash
make hive
```
It will create hive and mariadb instances.

### Presto
In order to have acces to delta tables without spark, presto is going to be employed as distributed query engine. It works with superset and hive tables. Presto is opensource, scalable and it can connect to any databases.
```bash
make presto-cluster
```
By this command will create a presto coordinator and worker, the worker can scale horizontally. In order to query delta tables using presto:
```bash
make presto-cli
```
In presto-cli just like spark sql, any query can be run.
### Superset
Superset is opensource, supports any databases with many dashbord styles also famous in tech in order to create dashboards or to get hands on databases.